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
import { useCallback, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { useMutation } from '@tanstack/react-query';
import type { RootState, AppDispatch } from '../store';
import { setExecuting, setResults, setError, setSql, setCacheId } from '../store/querySlice';
import { executeQuery as executeQueryApi, cancelQuery as cancelQueryApi } from '../api/queries';
import { storeResultInCache } from '../api/resultCache';
import { recordQueryExecution, lookupCacheForQuery } from '../utils/queryExecutionHistory';
import { cacheResults } from '../utils/resultsCache';
import type { QueryRequest, QueryError, QueryHistoryEntry } from '../types';

export function useQueryExecution(
  tabId: string,
  addHistory?: (entry: QueryHistoryEntry) => void
) {
  const dispatch = useDispatch<AppDispatch>();
  const tab = useSelector((state: RootState) =>
    state.query.tabs.find((t) => t.id === tabId)
  );
  const lastExecutedSql = useRef<string>('');

  const mutation = useMutation({
    mutationFn: async (request: QueryRequest) => {
      const sqlAtExecution = request.query;
      const startTime = Date.now();
      const result = await executeQueryApi(request);
      const executionTime = Date.now() - startTime;
      return { result, executionTime, sqlAtExecution, defaultSchema: request.defaultSchema };
    },
    onMutate: () => {
      dispatch(setExecuting({ tabId, isExecuting: true }));
    },
    onSuccess: ({ result, executionTime, sqlAtExecution, defaultSchema }) => {
      dispatch(setResults({ tabId, results: result, executionTime }));
      addHistory?.({
        id: crypto.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}`,
        sql: sqlAtExecution,
        status: 'success',
        rowCount: result.rows?.length || 0,
        duration: executionTime,
        timestamp: Date.now(),
        queryId: result.queryId,
      });

      // Cache results in browser with extended TTL for pinned tabs (2 hours vs 30 min default)
      const isPinned = tab?.isPinned || false;
      const ttlOverride = isPinned ? 2 * 60 * 60 * 1000 : undefined; // 2 hours for pinned

      if (result.rows && result.rows.length > 0) {
        // Cache locally with extended TTL if pinned
        cacheResults(tabId, result, executionTime, undefined, undefined, ttlOverride);

        // Fire-and-forget: cache results to the backend for persistence
        storeResultInCache(
          result.queryId,
          sqlAtExecution,
          defaultSchema,
          result.queryState,
          result.columns || [],
          result.metadata || [],
          result.rows,
        ).then((meta) => {
          if (meta?.cacheId) {
            dispatch(setCacheId({ tabId, cacheId: meta.cacheId }));
            // Record in execution history for smart cache lookup
            recordQueryExecution(
              sqlAtExecution,
              meta.cacheId,
              result.rows?.length || 0,
              executionTime,
              defaultSchema,
            );
          }
        }).catch(() => {
          // Backend caching is best-effort
        });
      }
    },
    onError: (error: Error) => {
      const queryError: QueryError = {
        message: error.message || 'Query execution failed',
      };
      dispatch(setError({ tabId, error: queryError }));
      addHistory?.({
        id: crypto.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2)}`,
        sql: lastExecutedSql.current,
        status: 'error',
        rowCount: 0,
        duration: 0,
        timestamp: Date.now(),
        errorMessage: error.message,
      });
    },
  });

  const execute = useCallback(
    (options?: { autoLimit?: number; defaultSchema?: string; sqlOverride?: string; skipCache?: boolean }) => {
      const sqlToRun = options?.sqlOverride?.trim() || tab?.sql?.trim();
      if (!sqlToRun) {
        dispatch(
          setError({
            tabId,
            error: { message: 'Please enter a SQL query' },
          })
        );
        return;
      }

      // Smart cache lookup: check if we have a cached result for this query
      if (!options?.skipCache) {
        const cached = lookupCacheForQuery(sqlToRun);
        if (cached) {
          // Try to retrieve from backend cache using the cacheId
          // For now, we'll just note that a cache is available
          // A full implementation would fetch from backend here
          dispatch(setCacheId({ tabId, cacheId: cached.cacheId }));
        }
      }

      const request: QueryRequest = {
        query: sqlToRun,
        queryType: 'SQL',
        autoLimitRowCount: options?.autoLimit,
        defaultSchema: options?.defaultSchema || tab?.defaultSchema,
      };

      lastExecutedSql.current = sqlToRun;
      mutation.mutate(request);
    },
    [tab, tabId, dispatch, mutation]
  );

  const cancel = useCallback(async () => {
    if (tab?.results?.queryId) {
      try {
        await cancelQueryApi(tab.results.queryId);
      } catch (error) {
        console.error('Failed to cancel query:', error);
      }
    }
  }, [tab]);

  const updateSql = useCallback(
    (sql: string) => {
      dispatch(setSql({ tabId, sql }));
    },
    [tabId, dispatch]
  );

  return {
    sql: tab?.sql || '',
    results: tab?.results,
    error: tab?.error,
    isExecuting: tab?.isExecuting || false,
    executionTime: tab?.executionTime,
    cacheId: tab?.cacheId,
    execute,
    cancel,
    updateSql,
  };
}
