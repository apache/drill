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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  getCacheMetadata,
  getCacheRows,
  evictCacheEntry,
  getCacheStats,
  storeResultInCache,
  getDownloadUrl,
} from './resultCache';

// Mock the API client
vi.mock('./client', () => {
  const mockGet = vi.fn();
  const mockPost = vi.fn();
  const mockDelete = vi.fn();
  return {
    default: {
      get: mockGet,
      post: mockPost,
      delete: mockDelete,
    },
    __mockGet: mockGet,
    __mockPost: mockPost,
    __mockDelete: mockDelete,
  };
});

// Access mocks
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let mockGet: any, mockPost: any, mockDelete: any;

beforeEach(async () => {
  const mod = await import('./client');
  mockGet = (mod as Record<string, unknown>).__mockGet as ReturnType<typeof vi.fn>;
  mockPost = (mod as Record<string, unknown>).__mockPost as ReturnType<typeof vi.fn>;
  mockDelete = (mod as Record<string, unknown>).__mockDelete as ReturnType<typeof vi.fn>;
  mockGet.mockReset();
  mockPost.mockReset();
  mockDelete.mockReset();
});

describe('resultCache API', () => {
  describe('getCacheMetadata', () => {
    it('returns metadata on success', async () => {
      const meta = { cacheId: 'abc', queryId: 'q1', totalRows: 10 };
      mockGet.mockResolvedValue({ data: meta });

      const result = await getCacheMetadata('abc');
      expect(result).toEqual(meta);
      expect(mockGet).toHaveBeenCalledWith('/api/v1/results/abc');
    });

    it('returns null on error', async () => {
      mockGet.mockRejectedValue(new Error('Not found'));

      const result = await getCacheMetadata('nonexistent');
      expect(result).toBeNull();
    });

    it('encodes special characters in cacheId', async () => {
      mockGet.mockResolvedValue({ data: {} });
      await getCacheMetadata('abc/def');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/results/abc%2Fdef');
    });
  });

  describe('getCacheRows', () => {
    it('returns paginated rows on success', async () => {
      const page = { rows: [{ id: '1' }], offset: 0, limit: 500, totalRows: 1, hasMore: false };
      mockGet.mockResolvedValue({ data: page });

      const result = await getCacheRows('abc', 0, 500);
      expect(result).toEqual(page);
      expect(mockGet).toHaveBeenCalledWith(
        '/api/v1/results/abc/rows',
        { params: { offset: 0, limit: 500 } }
      );
    });

    it('uses default offset and limit', async () => {
      mockGet.mockResolvedValue({ data: { rows: [] } });
      await getCacheRows('abc');
      expect(mockGet).toHaveBeenCalledWith(
        '/api/v1/results/abc/rows',
        { params: { offset: 0, limit: 500 } }
      );
    });

    it('returns null on error', async () => {
      mockGet.mockRejectedValue(new Error('fail'));
      expect(await getCacheRows('abc')).toBeNull();
    });
  });

  describe('evictCacheEntry', () => {
    it('returns true on success', async () => {
      mockDelete.mockResolvedValue({});
      expect(await evictCacheEntry('abc')).toBe(true);
      expect(mockDelete).toHaveBeenCalledWith('/api/v1/results/abc');
    });

    it('returns false on error', async () => {
      mockDelete.mockRejectedValue(new Error('fail'));
      expect(await evictCacheEntry('abc')).toBe(false);
    });
  });

  describe('getCacheStats', () => {
    it('returns stats on success', async () => {
      const stats = { entryCount: 5, totalSizeBytes: 1000 };
      mockGet.mockResolvedValue({ data: stats });

      expect(await getCacheStats()).toEqual(stats);
      expect(mockGet).toHaveBeenCalledWith('/api/v1/results/stats');
    });

    it('returns null on error', async () => {
      mockGet.mockRejectedValue(new Error('fail'));
      expect(await getCacheStats()).toBeNull();
    });
  });

  describe('storeResultInCache', () => {
    it('stores results and returns metadata', async () => {
      const meta = { cacheId: 'new-id', queryId: 'q1', totalRows: 2 };
      mockPost.mockResolvedValue({ data: meta });

      const result = await storeResultInCache(
        'q1', 'SELECT 1', 'dfs', 'COMPLETED',
        ['id', 'name'], ['INTEGER', 'VARCHAR'],
        [{ id: 1, name: 'test' }]
      );

      expect(result).toEqual(meta);
      expect(mockPost).toHaveBeenCalledWith('/api/v1/results/store', expect.objectContaining({
        queryId: 'q1',
        sql: 'SELECT 1',
        columns: ['id', 'name'],
      }));
    });

    it('converts row values to strings', async () => {
      mockPost.mockResolvedValue({ data: { cacheId: 'x' } });

      await storeResultInCache(
        'q1', 'SELECT 1', undefined, undefined,
        ['num', 'nul'], ['INTEGER', 'VARCHAR'],
        [{ num: 42, nul: null }]
      );

      const call = mockPost.mock.calls[0];
      const body = call[1];
      expect(body.rows[0].num).toBe('42');
      expect(body.rows[0].nul).toBe('');
    });

    it('returns null on error', async () => {
      mockPost.mockRejectedValue(new Error('too large'));
      expect(await storeResultInCache('q1', 'SQL', '', '', [], [], [])).toBeNull();
    });
  });

  describe('getDownloadUrl', () => {
    it('returns CSV download URL by default', () => {
      expect(getDownloadUrl('abc')).toBe('/api/v1/results/abc/download?format=csv');
    });

    it('returns JSON download URL', () => {
      expect(getDownloadUrl('abc', 'json')).toBe('/api/v1/results/abc/download?format=json');
    });

    it('encodes special characters', () => {
      expect(getDownloadUrl('a/b')).toBe('/api/v1/results/a%2Fb/download?format=csv');
    });
  });
});
