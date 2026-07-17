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
import apiClient from './client';

export interface CacheMeta {
  cacheId: string;
  queryId: string;
  sqlHash: string;
  sql: string;
  defaultSchema: string;
  userName: string;
  queryState: string;
  columns: string[];
  metadata: string[];
  totalRows: number;
  sizeBytes: number;
  cachedAt: number;
  lastAccessedAt: number;
}

export interface PaginatedRows {
  rows: Record<string, string>[];
  offset: number;
  limit: number;
  totalRows: number;
  hasMore: boolean;
}

export interface CacheStats {
  entryCount: number;
  totalSizeBytes: number;
  maxTotalSizeBytes: number;
  ttlMinutes: number;
  cacheDirectory: string;
}

/**
 * Get cache entry metadata.
 */
export async function getCacheMetadata(cacheId: string): Promise<CacheMeta | null> {
  try {
    const response = await apiClient.get<CacheMeta>(`/api/v1/results/${encodeURIComponent(cacheId)}`);
    return response.data;
  } catch {
    return null;
  }
}

/**
 * Get paginated rows from a cached result.
 */
export async function getCacheRows(
  cacheId: string,
  offset = 0,
  limit = 500,
): Promise<PaginatedRows | null> {
  try {
    const response = await apiClient.get<PaginatedRows>(
      `/api/v1/results/${encodeURIComponent(cacheId)}/rows`,
      { params: { offset, limit } },
    );
    return response.data;
  } catch {
    return null;
  }
}

/**
 * Evict a cache entry.
 */
export async function evictCacheEntry(cacheId: string): Promise<boolean> {
  try {
    await apiClient.delete(`/api/v1/results/${encodeURIComponent(cacheId)}`);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get cache statistics.
 */
export async function getCacheStats(): Promise<CacheStats | null> {
  try {
    const response = await apiClient.get<CacheStats>('/api/v1/results/stats');
    return response.data;
  } catch {
    return null;
  }
}

/**
 * Store query results in the backend cache.
 * Returns the cache metadata including the cacheId, or null if caching failed.
 */
export async function storeResultInCache(
  queryId: string,
  sql: string,
  defaultSchema: string | undefined,
  queryState: string | undefined,
  columns: string[],
  metadata: string[],
  rows: Record<string, unknown>[],
): Promise<CacheMeta | null> {
  try {
    // Convert rows to string values as expected by the backend
    const stringRows = rows.map((row) => {
      const stringRow: Record<string, string> = {};
      for (const [key, value] of Object.entries(row)) {
        stringRow[key] = value != null ? String(value) : '';
      }
      return stringRow;
    });

    const response = await apiClient.post<CacheMeta>('/api/v1/results/store', {
      queryId,
      sql,
      defaultSchema: defaultSchema || '',
      queryState: queryState || 'COMPLETED',
      columns,
      metadata,
      rows: stringRows,
    });
    return response.data;
  } catch {
    return null;
  }
}

/**
 * Build the download URL for a cached result.
 */
export function getDownloadUrl(cacheId: string, format: 'csv' | 'json' = 'csv'): string {
  return `/api/v1/results/${encodeURIComponent(cacheId)}/download?format=${format}`;
}
