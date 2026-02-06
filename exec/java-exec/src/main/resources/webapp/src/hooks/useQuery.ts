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
import { useCallback } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { useMutation } from '@tanstack/react-query';
import type { RootState, AppDispatch } from '../store';
import { setExecuting, setResults, setError, setSql } from '../store/querySlice';
import { executeQuery as executeQueryApi, cancelQuery as cancelQueryApi } from '../api/queries';
import type { QueryRequest, QueryError } from '../types';

export function useQueryExecution(tabId: string) {
  const dispatch = useDispatch<AppDispatch>();
  const tab = useSelector((state: RootState) =>
    state.query.tabs.find((t) => t.id === tabId)
  );

  const mutation = useMutation({
    mutationFn: async (request: QueryRequest) => {
      const startTime = Date.now();
      const result = await executeQueryApi(request);
      const executionTime = Date.now() - startTime;
      return { result, executionTime };
    },
    onMutate: () => {
      dispatch(setExecuting({ tabId, isExecuting: true }));
    },
    onSuccess: ({ result, executionTime }) => {
      dispatch(setResults({ tabId, results: result, executionTime }));
    },
    onError: (error: Error) => {
      const queryError: QueryError = {
        message: error.message || 'Query execution failed',
      };
      dispatch(setError({ tabId, error: queryError }));
    },
  });

  const execute = useCallback(
    (options?: { autoLimit?: number; defaultSchema?: string }) => {
      if (!tab?.sql?.trim()) {
        dispatch(
          setError({
            tabId,
            error: { message: 'Please enter a SQL query' },
          })
        );
        return;
      }

      const request: QueryRequest = {
        query: tab.sql,
        queryType: 'SQL',
        autoLimitRowCount: options?.autoLimit,
        defaultSchema: options?.defaultSchema || tab.defaultSchema,
      };

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
    execute,
    cancel,
    updateSql,
  };
}
