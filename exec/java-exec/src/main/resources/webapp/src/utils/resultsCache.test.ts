/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  cacheResults,
  getCachedResults,
  invalidateCache,
  clearProjectCache,
  clearAllCache,
  configureCacheConfig,
  getCacheConfig,
  getLocalCacheStats,
  estimateResultSize,
  getCacheId,
  setCacheId,
} from './resultsCache';
import type { QueryResult } from '../types';

function makeResult(rowCount: number, colCount = 3): QueryResult {
  const columns = Array.from({ length: colCount }, (_, i) => `col${i}`);
  const metadata = columns.map(() => 'VARCHAR');
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < rowCount; i++) {
    const row: Record<string, unknown> = {};
    columns.forEach((col, idx) => {
      row[col] = `value-${i}-${idx}`;
    });
    rows.push(row);
  }
  return { columns, metadata, rows, queryId: `q-${rowCount}` };
}

describe('resultsCache', () => {
  beforeEach(() => {
    clearAllCache();
    configureCacheConfig({ maxEntries: 10, maxBytes: 50 * 1024 * 1024, ttlMs: 30 * 60 * 1000 });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('caches and retrieves results', () => {
    const result = makeResult(5);
    cacheResults('tab-1', result, 100);

    const cached = getCachedResults('tab-1');
    expect(cached).not.toBeNull();
    expect(cached!.result.rows.length).toBe(5);
    expect(cached!.executionTime).toBe(100);
  });

  it('returns null for non-existent entry', () => {
    expect(getCachedResults('nonexistent')).toBeNull();
  });

  it('respects TTL expiration', () => {
    const result = makeResult(3);
    cacheResults('tab-1', result, 50);

    // Advance time past TTL
    const now = Date.now();
    vi.spyOn(Date, 'now').mockReturnValue(now + 31 * 60 * 1000); // 31 minutes

    expect(getCachedResults('tab-1')).toBeNull();
  });

  it('respects custom TTL on retrieval', () => {
    const result = makeResult(3);
    cacheResults('tab-1', result, 50);

    const now = Date.now();
    vi.spyOn(Date, 'now').mockReturnValue(now + 5 * 60 * 1000); // 5 minutes

    // Still valid with default 30min TTL
    expect(getCachedResults('tab-1')).not.toBeNull();

    // Expired with custom 1min TTL
    expect(getCachedResults('tab-1', undefined, 60 * 1000)).toBeNull();
  });

  it('invalidates a single entry', () => {
    cacheResults('tab-1', makeResult(3), 100);
    cacheResults('tab-2', makeResult(5), 200);

    invalidateCache('tab-1');

    expect(getCachedResults('tab-1')).toBeNull();
    expect(getCachedResults('tab-2')).not.toBeNull();
  });

  it('clears project cache', () => {
    cacheResults('tab-1', makeResult(3), 100, 'proj1');
    cacheResults('tab-2', makeResult(5), 200, 'proj1');
    cacheResults('tab-3', makeResult(2), 50, 'proj2');

    clearProjectCache('proj1');

    expect(getCachedResults('tab-1', 'proj1')).toBeNull();
    expect(getCachedResults('tab-2', 'proj1')).toBeNull();
    expect(getCachedResults('tab-3', 'proj2')).not.toBeNull();
  });

  it('separates entries by project', () => {
    const result1 = makeResult(3);
    const result2 = makeResult(5);

    cacheResults('tab-1', result1, 100, 'proj1');
    cacheResults('tab-1', result2, 200, 'proj2');

    const cached1 = getCachedResults('tab-1', 'proj1');
    const cached2 = getCachedResults('tab-1', 'proj2');

    expect(cached1!.result.rows.length).toBe(3);
    expect(cached2!.result.rows.length).toBe(5);
  });

  it('stores and retrieves cacheId', () => {
    cacheResults('tab-1', makeResult(3), 100, undefined, 'backend-123');

    const cached = getCachedResults('tab-1');
    expect(cached!.cacheId).toBe('backend-123');
  });

  it('updates cacheId without replacing results', () => {
    cacheResults('tab-1', makeResult(3), 100);
    expect(getCacheId('tab-1')).toBeUndefined();

    setCacheId('tab-1', 'backend-456');
    expect(getCacheId('tab-1')).toBe('backend-456');

    // Results should still be intact
    const cached = getCachedResults('tab-1');
    expect(cached!.result.rows.length).toBe(3);
    expect(cached!.cacheId).toBe('backend-456');
  });

  describe('LRU eviction', () => {
    it('evicts oldest entries when max entries exceeded', () => {
      configureCacheConfig({ maxEntries: 3, maxBytes: 100 * 1024 * 1024, ttlMs: 30 * 60 * 1000 });

      cacheResults('tab-1', makeResult(1), 100);
      cacheResults('tab-2', makeResult(1), 100);
      cacheResults('tab-3', makeResult(1), 100);
      cacheResults('tab-4', makeResult(1), 100); // Should evict tab-1

      expect(getCachedResults('tab-1')).toBeNull();
      expect(getCachedResults('tab-2')).not.toBeNull();
      expect(getCachedResults('tab-3')).not.toBeNull();
      expect(getCachedResults('tab-4')).not.toBeNull();
    });

    it('accessing an entry moves it to most-recently-used', () => {
      configureCacheConfig({ maxEntries: 3, maxBytes: 100 * 1024 * 1024, ttlMs: 30 * 60 * 1000 });

      cacheResults('tab-1', makeResult(1), 100);
      cacheResults('tab-2', makeResult(1), 100);
      cacheResults('tab-3', makeResult(1), 100);

      // Access tab-1, making tab-2 the oldest
      getCachedResults('tab-1');

      cacheResults('tab-4', makeResult(1), 100); // Should evict tab-2

      expect(getCachedResults('tab-1')).not.toBeNull();
      expect(getCachedResults('tab-2')).toBeNull();
      expect(getCachedResults('tab-3')).not.toBeNull();
      expect(getCachedResults('tab-4')).not.toBeNull();
    });

    it('evicts when memory budget exceeded', () => {
      // Use a very small memory budget
      configureCacheConfig({ maxEntries: 100, maxBytes: 500, ttlMs: 30 * 60 * 1000 });

      // Each result with 100 rows should be well over 500 bytes
      cacheResults('tab-1', makeResult(100), 100);
      cacheResults('tab-2', makeResult(100), 100);

      const stats = getLocalCacheStats();
      expect(stats.totalBytes).toBeLessThanOrEqual(500);
    });
  });

  describe('estimateResultSize', () => {
    it('estimates size for empty result', () => {
      const result = makeResult(0);
      expect(estimateResultSize(result)).toBe(200);
    });

    it('estimates size proportional to row count', () => {
      const small = estimateResultSize(makeResult(10));
      const large = estimateResultSize(makeResult(100));
      expect(large).toBeGreaterThan(small);
    });

    it('scales with column count', () => {
      const narrow = estimateResultSize(makeResult(10, 2));
      const wide = estimateResultSize(makeResult(10, 10));
      expect(wide).toBeGreaterThan(narrow);
    });
  });

  describe('getLocalCacheStats', () => {
    it('reports empty cache stats', () => {
      const stats = getLocalCacheStats();
      expect(stats.entryCount).toBe(0);
      expect(stats.totalBytes).toBe(0);
    });

    it('reports entry count and total bytes', () => {
      cacheResults('tab-1', makeResult(5), 100);
      cacheResults('tab-2', makeResult(10), 200);

      const stats = getLocalCacheStats();
      expect(stats.entryCount).toBe(2);
      expect(stats.totalBytes).toBeGreaterThan(0);
      expect(stats.entries.length).toBe(2);
    });

    it('includes cacheId in entries', () => {
      cacheResults('tab-1', makeResult(5), 100, undefined, 'backend-789');

      const stats = getLocalCacheStats();
      expect(stats.entries[0].cacheId).toBe('backend-789');
    });
  });

  describe('getCacheConfig / configureCacheConfig', () => {
    it('returns default config', () => {
      const cfg = getCacheConfig();
      expect(cfg.maxEntries).toBe(10);
    });

    it('allows partial config updates', () => {
      configureCacheConfig({ maxEntries: 20 });
      const cfg = getCacheConfig();
      expect(cfg.maxEntries).toBe(20);
      expect(cfg.maxBytes).toBe(50 * 1024 * 1024); // Unchanged
    });
  });

  describe('clearAllCache', () => {
    it('removes all entries', () => {
      cacheResults('tab-1', makeResult(5), 100, 'proj1');
      cacheResults('tab-2', makeResult(3), 50);

      clearAllCache();

      expect(getCachedResults('tab-1', 'proj1')).toBeNull();
      expect(getCachedResults('tab-2')).toBeNull();
      expect(getLocalCacheStats().entryCount).toBe(0);
    });
  });
});
