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

import { useEffect, useRef, useCallback } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import type { RootState, AppDispatch } from '../store';
import { restoreQueryState } from '../store/querySlice';
import { restoreUiState } from '../store/uiSlice';
import {
  loadTabState,
  saveTabState,
  loadUiState,
  saveUiState,
} from '../utils/workspacePersistence';
import { cacheResults, getCachedResults } from '../utils/resultsCache';
import { getCacheRows, getCacheMetadata } from '../api/resultCache';
import type { QueryResult } from '../types';

export function useWorkspacePersistence(projectId?: string) {
  const dispatch = useDispatch<AppDispatch>();
  const hasRestoredRef = useRef(false);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const tabs = useSelector((state: RootState) => state.query.tabs);
  const activeTabId = useSelector((state: RootState) => state.query.activeTabId);
  const sidebarCollapsed = useSelector((state: RootState) => state.ui.sidebarCollapsed);
  const sidebarWidth = useSelector((state: RootState) => state.ui.sidebarWidth);
  const editorHeight = useSelector((state: RootState) => state.ui.editorHeight);

  // Restore on mount
  useEffect(() => {
    const persisted = loadTabState(projectId);
    if (persisted) {
      // First pass: restore what we can from the in-memory cache
      const restoredTabs = persisted.tabs.map((t) => {
        const cached = getCachedResults(t.id, projectId);
        return {
          id: t.id,
          name: t.name,
          sql: t.sql,
          defaultSchema: t.defaultSchema,
          results: cached?.result,
          executionTime: cached?.executionTime,
          resultsExpired: !cached && t.sql.trim().length > 0,
          cacheId: cached?.cacheId || t.cacheId,
        };
      });

      // Find max tab counter from restored tab IDs
      let maxCounter = persisted.tabCounter;
      for (const t of persisted.tabs) {
        const match = t.id.match(/^tab-(\d+)$/);
        if (match) {
          maxCounter = Math.max(maxCounter, parseInt(match[1], 10));
        }
      }

      dispatch(restoreQueryState({
        tabs: restoredTabs,
        activeTabId: persisted.activeTabId,
        tabCounter: maxCounter,
      }));

      // Second pass: for tabs with cacheId but no in-memory results,
      // try to restore from the backend cache asynchronously
      for (const tab of restoredTabs) {
        if (!tab.results && tab.cacheId) {
          restoreFromBackendCache(tab.id, tab.cacheId);
        }
      }
    }

    const persistedUi = loadUiState(projectId);
    if (persistedUi) {
      dispatch(restoreUiState(persistedUi));
    }

    // Mark restore complete after a tick to let effects settle
    requestAnimationFrame(() => {
      hasRestoredRef.current = true;
    });

    return () => {
      if (saveTimerRef.current) {
        clearTimeout(saveTimerRef.current);
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dispatch, projectId]);

  // Restore results from backend cache
  const restoreFromBackendCache = useCallback(async (tabId: string, cacheId: string) => {
    try {
      const meta = await getCacheMetadata(cacheId);
      if (!meta) {
        return; // Cache entry expired or gone
      }

      const page = await getCacheRows(cacheId, 0, meta.totalRows);
      if (!page || !page.rows) {
        return;
      }

      const result: QueryResult = {
        columns: meta.columns,
        metadata: meta.metadata,
        rows: page.rows,
        queryId: meta.queryId,
        queryState: meta.queryState,
      };

      // Store in local LRU cache
      cacheResults(tabId, result, 0, projectId, cacheId);

      // Update Redux
      dispatch(restoreQueryState({
        tabs: tabs.map((t) =>
          t.id === tabId
            ? { ...t, results: result, executionTime: 0, resultsExpired: false, cacheId }
            : t
        ),
        activeTabId,
        tabCounter: Math.max(...tabs.map((t) => {
          const m = t.id.match(/^tab-(\d+)$/);
          return m ? parseInt(m[1], 10) : 0;
        })),
      }));
    } catch {
      // Backend cache unavailable — leave as expired
    }
  }, [dispatch, tabs, activeTabId, projectId]);

  // Save tab state (debounced)
  useEffect(() => {
    if (!hasRestoredRef.current) {
      return;
    }

    if (saveTimerRef.current) {
      clearTimeout(saveTimerRef.current);
    }

    saveTimerRef.current = setTimeout(() => {
      // Find max tab counter from current tab IDs
      let maxCounter = 1;
      for (const t of tabs) {
        const match = t.id.match(/^tab-(\d+)$/);
        if (match) {
          maxCounter = Math.max(maxCounter, parseInt(match[1], 10));
        }
      }

      saveTabState(
        {
          tabs: tabs.map((t) => ({
            id: t.id,
            name: t.name,
            sql: t.sql,
            defaultSchema: t.defaultSchema,
            cacheId: t.cacheId,
          })),
          activeTabId,
          tabCounter: maxCounter,
          savedAt: Date.now(),
        },
        projectId,
      );
    }, 500);

    return () => {
      if (saveTimerRef.current) {
        clearTimeout(saveTimerRef.current);
      }
    };
  }, [tabs, activeTabId, projectId]);

  // Save UI state (immediate — changes are infrequent)
  useEffect(() => {
    if (!hasRestoredRef.current) {
      return;
    }
    saveUiState({ sidebarCollapsed, sidebarWidth, editorHeight }, projectId);
  }, [sidebarCollapsed, sidebarWidth, editorHeight, projectId]);

  // Callback for SqlLabPage to cache results when they arrive
  const onResultsCached = useCallback(
    (tabId: string, result: QueryResult, executionTime: number, cacheId?: string) => {
      cacheResults(tabId, result, executionTime, projectId, cacheId);
    },
    [projectId],
  );

  return { onResultsCached };
}
