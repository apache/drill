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
import { useState, useCallback, useRef, useEffect } from 'react';
import { getCacheRows, getCacheMetadata } from '../api/resultCache';
import type { CacheMeta } from '../api/resultCache';
import type { QueryResult } from '../types';

export const DEFAULT_SERVER_PAGE_SIZE = 5000;
export const SERVER_PAGE_SIZE_OPTIONS = [1000, 2000, 5000, 10000, 25000];

/**
 * Threshold in rows above which server-side pagination is used.
 * Below this, all rows are kept in memory for fully client-side behavior.
 */
export const SERVER_PAGINATION_THRESHOLD = 10000;

export interface ServerPaginationState {
  /** Whether server pagination is active (cacheId present and total rows > threshold) */
  enabled: boolean;
  /** Current page offset (0-based row index) */
  offset: number;
  /** Rows per server page */
  pageSize: number;
  /** Total rows in the cached result */
  totalRows: number;
  /** Current page number (1-based) */
  currentPage: number;
  /** Total number of pages */
  totalPages: number;
  /** Whether a page fetch is in progress */
  isLoading: boolean;
  /** Error message if page fetch failed */
  error?: string;
  /** The currently loaded page of results (fed to AG Grid) */
  pageResults?: QueryResult;
  /** Cache metadata */
  cacheMeta?: CacheMeta;
}

export interface UseServerPaginationReturn {
  state: ServerPaginationState;
  /** Go to a specific page (1-based) */
  goToPage: (page: number) => void;
  /** Go to next page */
  nextPage: () => void;
  /** Go to previous page */
  prevPage: () => void;
  /** Change the server page size */
  setPageSize: (size: number) => void;
  /**
   * Get the effective results to display.
   * Returns paginated page results when server pagination is active,
   * otherwise returns the original results unchanged.
   */
  effectiveResults?: QueryResult;
  /** Effective total row count (from cache meta when paginated, from results otherwise) */
  effectiveTotalRows: number;
}

export function useServerPagination(
  cacheId?: string,
  results?: QueryResult,
): UseServerPaginationReturn {
  const [state, setState] = useState<ServerPaginationState>({
    enabled: false,
    offset: 0,
    pageSize: DEFAULT_SERVER_PAGE_SIZE,
    totalRows: 0,
    currentPage: 1,
    totalPages: 1,
    isLoading: false,
  });

  const fetchingRef = useRef(false);
  const lastCacheIdRef = useRef<string | undefined>();

  // When cacheId changes, fetch metadata to determine if server pagination is needed
  useEffect(() => {
    if (!cacheId) {
      setState((prev) => ({ ...prev, enabled: false, cacheMeta: undefined, pageResults: undefined }));
      lastCacheIdRef.current = undefined;
      return;
    }

    if (cacheId === lastCacheIdRef.current) {
      return;
    }
    lastCacheIdRef.current = cacheId;

    getCacheMetadata(cacheId).then((meta) => {
      if (!meta) {
        return;
      }

      const shouldPaginate = meta.totalRows > SERVER_PAGINATION_THRESHOLD;
      setState((prev) => ({
        ...prev,
        enabled: shouldPaginate,
        totalRows: meta.totalRows,
        cacheMeta: meta,
        totalPages: shouldPaginate
          ? Math.ceil(meta.totalRows / prev.pageSize)
          : 1,
        currentPage: 1,
        offset: 0,
      }));

      // If pagination is needed and we don't already have results in memory, fetch first page
      if (shouldPaginate && (!results || results.rows.length === 0)) {
        fetchPage(cacheId, 0, state.pageSize, meta);
      }
    });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheId]);

  const fetchPage = useCallback(async (
    id: string,
    offset: number,
    pageSize: number,
    meta?: CacheMeta,
  ) => {
    if (fetchingRef.current) {
      return;
    }
    fetchingRef.current = true;
    setState((prev) => ({ ...prev, isLoading: true, error: undefined }));

    try {
      const page = await getCacheRows(id, offset, pageSize);
      if (!page) {
        setState((prev) => ({ ...prev, isLoading: false, error: 'Failed to fetch page' }));
        return;
      }

      const effectiveMeta = meta || state.cacheMeta;
      const pageResults: QueryResult = {
        columns: effectiveMeta?.columns || [],
        metadata: effectiveMeta?.metadata || [],
        rows: page.rows,
        queryId: effectiveMeta?.queryId || '',
        queryState: effectiveMeta?.queryState,
      };

      setState((prev) => ({
        ...prev,
        isLoading: false,
        pageResults,
        offset,
        currentPage: Math.floor(offset / pageSize) + 1,
        totalPages: Math.ceil(page.totalRows / pageSize),
        totalRows: page.totalRows,
      }));
    } catch (e) {
      setState((prev) => ({
        ...prev,
        isLoading: false,
        error: e instanceof Error ? e.message : 'Failed to fetch page',
      }));
    } finally {
      fetchingRef.current = false;
    }
  }, [state.cacheMeta]);

  const goToPage = useCallback((page: number) => {
    if (!cacheId || !state.enabled) {
      return;
    }
    const clamped = Math.max(1, Math.min(page, state.totalPages));
    const offset = (clamped - 1) * state.pageSize;
    fetchPage(cacheId, offset, state.pageSize);
  }, [cacheId, state.enabled, state.totalPages, state.pageSize, fetchPage]);

  const nextPage = useCallback(() => {
    goToPage(state.currentPage + 1);
  }, [state.currentPage, goToPage]);

  const prevPage = useCallback(() => {
    goToPage(state.currentPage - 1);
  }, [state.currentPage, goToPage]);

  const setPageSize = useCallback((size: number) => {
    setState((prev) => {
      const newTotalPages = Math.ceil(prev.totalRows / size);
      return {
        ...prev,
        pageSize: size,
        totalPages: newTotalPages,
        currentPage: 1,
        offset: 0,
      };
    });
    if (cacheId && state.enabled) {
      fetchPage(cacheId, 0, size);
    }
  }, [cacheId, state.enabled, fetchPage]);

  // Determine effective results
  const effectiveResults = state.enabled ? state.pageResults : results;
  const effectiveTotalRows = state.enabled
    ? state.totalRows
    : (results?.rows?.length ?? 0);

  return {
    state,
    goToPage,
    nextPage,
    prevPage,
    setPageSize,
    effectiveResults,
    effectiveTotalRows,
  };
}
