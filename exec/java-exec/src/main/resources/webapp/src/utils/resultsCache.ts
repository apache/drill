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

import type { QueryResult } from '../types';

/**
 * LRU in-memory result cache with configurable memory budget.
 *
 * The browser keeps only a bounded number of query results in memory.
 * When the budget is exceeded, the least-recently-used entry is evicted.
 * Each tab can also store a backend cacheId so results can be re-fetched
 * from the server-side persistent cache on demand.
 */

const DEFAULT_TTL_MS = 30 * 60 * 1000; // 30 minutes
const DEFAULT_MAX_ENTRIES = 10;
const DEFAULT_MAX_BYTES = 50 * 1024 * 1024; // 50 MB

export interface CachedResult {
  result: QueryResult;
  executionTime: number;
  cachedAt: number;
  lastAccessedAt: number;
  estimatedBytes: number;
  cacheId?: string; // Backend cache ID for persistent retrieval
  ttlOverride?: number; // Per-tab TTL override (in ms), for pinned tabs
}

export interface CacheConfig {
  maxEntries: number;
  maxBytes: number;
  ttlMs: number;
}

// Use a Map for insertion-order iteration (LRU eviction)
const cache = new Map<string, CachedResult>();

let config: CacheConfig = {
  maxEntries: DEFAULT_MAX_ENTRIES,
  maxBytes: DEFAULT_MAX_BYTES,
  ttlMs: DEFAULT_TTL_MS,
};

/**
 * Update cache configuration. Takes effect immediately.
 */
export function configureCacheConfig(newConfig: Partial<CacheConfig>): void {
  config = { ...config, ...newConfig };
  evictIfOverBudget();
}

/**
 * Get the current cache configuration.
 */
export function getCacheConfig(): Readonly<CacheConfig> {
  return config;
}

function cacheKey(tabId: string, projectId?: string): string {
  return `${projectId || 'global'}:${tabId}`;
}

/**
 * Estimate memory usage of a QueryResult in bytes.
 * Uses a rough heuristic: JSON-stringify a sample and extrapolate.
 */
export function estimateResultSize(result: QueryResult): number {
  if (!result.rows || result.rows.length === 0) {
    return 200; // Minimal overhead for metadata
  }

  // Sample up to 10 rows to estimate average row size
  const sampleSize = Math.min(10, result.rows.length);
  let sampleBytes = 0;
  for (let i = 0; i < sampleSize; i++) {
    sampleBytes += JSON.stringify(result.rows[i]).length * 2; // ×2 for UTF-16
  }
  const avgRowBytes = sampleBytes / sampleSize;

  // Columns + metadata overhead
  const metaBytes = JSON.stringify(result.columns).length * 2
    + (result.metadata ? JSON.stringify(result.metadata).length * 2 : 0)
    + 200; // queryId, queryState, etc.

  return Math.ceil(avgRowBytes * result.rows.length + metaBytes);
}

/**
 * Cache a query result in the browser's LRU memory cache.
 * @param ttlOverride - Optional TTL override in ms (e.g., for pinned tabs with longer retention)
 */
export function cacheResults(
  tabId: string,
  result: QueryResult,
  executionTime: number,
  projectId?: string,
  cacheId?: string,
  ttlOverride?: number,
): void {
  const key = cacheKey(tabId, projectId);
  const estimatedBytes = estimateResultSize(result);

  // Delete first so re-insertion moves it to the end (most recently used)
  cache.delete(key);

  cache.set(key, {
    result,
    executionTime,
    cachedAt: Date.now(),
    lastAccessedAt: Date.now(),
    estimatedBytes,
    cacheId,
    ttlOverride,
  });

  evictIfOverBudget();
}

/**
 * Retrieve a cached result if available and not expired.
 * Updates the last-accessed timestamp for LRU tracking.
 */
export function getCachedResults(
  tabId: string,
  projectId?: string,
  ttlMs?: number,
): { result: QueryResult; executionTime: number; cacheId?: string } | null {
  const key = cacheKey(tabId, projectId);
  const entry = cache.get(key);
  if (!entry) {
    return null;
  }

  // Use ttlOverride if set (for pinned tabs), otherwise use provided ttl or config default
  const effectiveTtl = entry.ttlOverride ?? ttlMs ?? config.ttlMs;
  if (Date.now() - entry.cachedAt > effectiveTtl) {
    cache.delete(key);
    return null;
  }

  // Move to end of Map for LRU (delete + re-set)
  entry.lastAccessedAt = Date.now();
  cache.delete(key);
  cache.set(key, entry);

  return { result: entry.result, executionTime: entry.executionTime, cacheId: entry.cacheId };
}

/**
 * Get only the cacheId for a tab (without loading full results).
 */
export function getCacheId(tabId: string, projectId?: string): string | undefined {
  const key = cacheKey(tabId, projectId);
  return cache.get(key)?.cacheId;
}

/**
 * Update the cacheId for an existing entry without replacing the results.
 */
export function setCacheId(tabId: string, cacheId: string, projectId?: string): void {
  const key = cacheKey(tabId, projectId);
  const entry = cache.get(key);
  if (entry) {
    entry.cacheId = cacheId;
  }
}

/**
 * Invalidate (remove) a single cached result.
 */
export function invalidateCache(tabId: string, projectId?: string): void {
  cache.delete(cacheKey(tabId, projectId));
}

/**
 * Clear all cached results for a project.
 */
export function clearProjectCache(projectId?: string): void {
  const prefix = `${projectId || 'global'}:`;
  for (const key of [...cache.keys()]) {
    if (key.startsWith(prefix)) {
      cache.delete(key);
    }
  }
}

/**
 * Get cache statistics for debugging / UI display.
 */
export function getLocalCacheStats(): {
  entryCount: number;
  totalBytes: number;
  maxEntries: number;
  maxBytes: number;
  entries: { key: string; bytes: number; age: number; cacheId?: string }[];
} {
  const now = Date.now();
  const entries: { key: string; bytes: number; age: number; cacheId?: string }[] = [];
  let totalBytes = 0;
  for (const [key, entry] of cache) {
    entries.push({
      key,
      bytes: entry.estimatedBytes,
      age: now - entry.cachedAt,
      cacheId: entry.cacheId,
    });
    totalBytes += entry.estimatedBytes;
  }
  return {
    entryCount: cache.size,
    totalBytes,
    maxEntries: config.maxEntries,
    maxBytes: config.maxBytes,
    entries,
  };
}

// ==================== Internal ====================

function evictIfOverBudget(): void {
  // Evict expired entries first
  const now = Date.now();
  for (const [key, entry] of cache) {
    if (now - entry.cachedAt > config.ttlMs) {
      cache.delete(key);
    }
  }

  // Evict LRU entries if over max count
  while (cache.size > config.maxEntries) {
    const oldest = cache.keys().next().value;
    if (oldest !== undefined) {
      cache.delete(oldest);
    }
  }

  // Evict LRU entries if over max bytes
  let totalBytes = 0;
  for (const entry of cache.values()) {
    totalBytes += entry.estimatedBytes;
  }
  while (totalBytes > config.maxBytes && cache.size > 0) {
    const oldest = cache.keys().next().value;
    if (oldest !== undefined) {
      const entry = cache.get(oldest);
      if (entry) {
        totalBytes -= entry.estimatedBytes;
      }
      cache.delete(oldest);
    }
  }
}

/**
 * Clear all entries. Primarily for testing.
 */
export function clearAllCache(): void {
  cache.clear();
}
