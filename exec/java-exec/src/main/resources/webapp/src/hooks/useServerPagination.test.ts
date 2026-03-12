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
import { renderHook, act, waitFor } from '@testing-library/react';
import {
  useServerPagination,
  SERVER_PAGINATION_THRESHOLD,
  DEFAULT_SERVER_PAGE_SIZE,
} from './useServerPagination';
import type { QueryResult } from '../types';

// Mock the API
vi.mock('../api/resultCache', () => ({
  getCacheMetadata: vi.fn(),
  getCacheRows: vi.fn(),
}));

import { getCacheMetadata, getCacheRows } from '../api/resultCache';

const mockGetCacheMetadata = getCacheMetadata as ReturnType<typeof vi.fn>;
const mockGetCacheRows = getCacheRows as ReturnType<typeof vi.fn>;

function makeResult(rowCount: number): QueryResult {
  const columns = ['id', 'name'];
  const metadata = ['INTEGER', 'VARCHAR'];
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < rowCount; i++) {
    rows.push({ id: String(i), name: `row-${i}` });
  }
  return { columns, metadata, rows, queryId: 'q1' };
}

describe('useServerPagination', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('is disabled when no cacheId is provided', () => {
    const { result } = renderHook(() => useServerPagination(undefined, makeResult(5)));
    expect(result.current.state.enabled).toBe(false);
    expect(result.current.effectiveResults?.rows.length).toBe(5);
  });

  it('is disabled when cacheId exists but rows below threshold', async () => {
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows: 100,
      columns: ['id', 'name'],
      metadata: ['INTEGER', 'VARCHAR'],
      queryId: 'q1',
    });

    const localResults = makeResult(100);
    const { result } = renderHook(() => useServerPagination('abc', localResults));

    await waitFor(() => {
      expect(mockGetCacheMetadata).toHaveBeenCalledWith('abc');
    });

    // Below threshold — pagination not enabled
    expect(result.current.state.enabled).toBe(false);
    expect(result.current.effectiveResults).toBe(localResults);
  });

  it('enables pagination when total rows exceed threshold', async () => {
    const largeCount = SERVER_PAGINATION_THRESHOLD + 1000;
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows: largeCount,
      columns: ['id', 'name'],
      metadata: ['INTEGER', 'VARCHAR'],
      queryId: 'q1',
      queryState: 'COMPLETED',
    });

    mockGetCacheRows.mockResolvedValue({
      rows: Array.from({ length: DEFAULT_SERVER_PAGE_SIZE }, (_, i) => ({
        id: String(i),
        name: `row-${i}`,
      })),
      offset: 0,
      limit: DEFAULT_SERVER_PAGE_SIZE,
      totalRows: largeCount,
      hasMore: true,
    });

    // Pass empty results to trigger first page fetch
    const { result } = renderHook(() => useServerPagination('abc', makeResult(0)));

    await waitFor(() => {
      expect(result.current.state.enabled).toBe(true);
    });

    expect(result.current.state.totalRows).toBe(largeCount);
    expect(result.current.state.totalPages).toBe(Math.ceil(largeCount / DEFAULT_SERVER_PAGE_SIZE));
    expect(result.current.state.currentPage).toBe(1);
  });

  it('fetches first page when results are empty', async () => {
    const totalRows = 20000;
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows,
      columns: ['id'],
      metadata: ['INTEGER'],
      queryId: 'q1',
      queryState: 'COMPLETED',
    });

    const pageRows = Array.from({ length: DEFAULT_SERVER_PAGE_SIZE }, (_, i) => ({
      id: String(i),
    }));
    mockGetCacheRows.mockResolvedValue({
      rows: pageRows,
      offset: 0,
      limit: DEFAULT_SERVER_PAGE_SIZE,
      totalRows,
      hasMore: true,
    });

    const { result } = renderHook(() => useServerPagination('abc', makeResult(0)));

    await waitFor(() => {
      expect(result.current.state.pageResults).toBeTruthy();
    });

    expect(result.current.state.pageResults?.rows.length).toBe(DEFAULT_SERVER_PAGE_SIZE);
    expect(result.current.effectiveResults?.rows.length).toBe(DEFAULT_SERVER_PAGE_SIZE);
  });

  it('does not re-fetch metadata when cacheId does not change', async () => {
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows: 100,
      columns: ['id'],
      metadata: ['INTEGER'],
      queryId: 'q1',
    });

    const { rerender } = renderHook(
      ({ cacheId }) => useServerPagination(cacheId, makeResult(100)),
      { initialProps: { cacheId: 'abc' } }
    );

    await waitFor(() => {
      expect(mockGetCacheMetadata).toHaveBeenCalledTimes(1);
    });

    rerender({ cacheId: 'abc' });

    // Should not call again for the same cacheId
    expect(mockGetCacheMetadata).toHaveBeenCalledTimes(1);
  });

  it('returns effective total row count from server when paginated', async () => {
    const totalRows = 50000;
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows,
      columns: ['id'],
      metadata: ['INTEGER'],
      queryId: 'q1',
      queryState: 'COMPLETED',
    });

    mockGetCacheRows.mockResolvedValue({
      rows: [{ id: '0' }],
      offset: 0,
      limit: DEFAULT_SERVER_PAGE_SIZE,
      totalRows,
      hasMore: true,
    });

    const { result } = renderHook(() => useServerPagination('abc', makeResult(0)));

    await waitFor(() => {
      expect(result.current.state.enabled).toBe(true);
    });

    expect(result.current.effectiveTotalRows).toBe(totalRows);
  });

  it('returns local row count when not paginated', () => {
    const localResults = makeResult(500);
    const { result } = renderHook(() => useServerPagination(undefined, localResults));

    expect(result.current.effectiveTotalRows).toBe(500);
  });

  it('handles metadata fetch failure gracefully', async () => {
    mockGetCacheMetadata.mockResolvedValue(null);

    const localResults = makeResult(5);
    const { result } = renderHook(() => useServerPagination('abc', localResults));

    await waitFor(() => {
      expect(mockGetCacheMetadata).toHaveBeenCalled();
    });

    // Should fall back to local results
    expect(result.current.state.enabled).toBe(false);
    expect(result.current.effectiveResults).toBe(localResults);
  });

  it('exposes page navigation functions', async () => {
    const totalRows = 15000;
    mockGetCacheMetadata.mockResolvedValue({
      cacheId: 'abc',
      totalRows,
      columns: ['id'],
      metadata: ['INTEGER'],
      queryId: 'q1',
      queryState: 'COMPLETED',
    });

    mockGetCacheRows.mockResolvedValue({
      rows: [{ id: '0' }],
      offset: 0,
      limit: DEFAULT_SERVER_PAGE_SIZE,
      totalRows,
      hasMore: true,
    });

    const { result } = renderHook(() => useServerPagination('abc', makeResult(0)));

    await waitFor(() => {
      expect(result.current.state.enabled).toBe(true);
    });

    expect(typeof result.current.goToPage).toBe('function');
    expect(typeof result.current.nextPage).toBe('function');
    expect(typeof result.current.prevPage).toBe('function');
    expect(typeof result.current.setPageSize).toBe('function');
  });
});
