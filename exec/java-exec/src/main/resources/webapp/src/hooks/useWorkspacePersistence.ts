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
import { useQueryClient } from '@tanstack/react-query';
import { useSelector, useDispatch } from 'react-redux';
import type { RootState, AppDispatch } from '../store';
import { restoreQueryState } from '../store/querySlice';
import { restoreUiState } from '../store/uiSlice';
import {
  loadTabState,
  saveTabState,
  loadUiState,
  saveUiState,
  nextTabCounter,
} from '../utils/workspacePersistence';
import { cacheResults, getCachedResults } from '../utils/resultsCache';
import { listTabs, createTab, updateTab } from '../api/tabs';
import { reconcileTabs } from '../utils/tabReconcile';
import { shouldPromote } from '../utils/tabPromotion';
import { conversationLengthFor } from './useProspector';
import { getCacheRows, getCacheMetadata } from '../api/resultCache';
import type { QueryResult } from '../types';

// Which project the (singleton) Redux tab state currently belongs to.
// This is module-level on purpose: the query editor lives in different route
// subtrees for global (`/query`) vs project (`/projects/:id/query`), so
// navigating between them REMOUNTS this hook. A per-instance ref would reset
// to its "never set" sentinel on that remount, making a real project switch
// look like a first mount — which let the previous project's tabs leak into
// the new one. Module-level state survives the remount, mirroring the
// singleton Redux store it describes. `false` = not yet loaded by any instance.
let loadedProjectId: string | undefined | false = false;

export function useWorkspacePersistence(projectId?: string) {
  const dispatch = useDispatch<AppDispatch>();
  const queryClient = useQueryClient();
  const hasRestoredRef = useRef(false);
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const tabs = useSelector((state: RootState) => state.query.tabs);
  const activeTabId = useSelector((state: RootState) => state.query.activeTabId);
  const editorHeight = useSelector((state: RootState) => state.ui.editorHeight);

  // Refs that mirror current state — used by the cleanup function to
  // synchronously flush the previous project's tabs before switching.
  const tabsRef = useRef(tabs);
  tabsRef.current = tabs;
  const activeTabIdRef = useRef(activeTabId);
  activeTabIdRef.current = activeTabId;

  // Restore on mount and on projectId change
  useEffect(() => {
    hasRestoredRef.current = false;

    // A true switch is when the currently-loaded project differs from this
    // one. Tracked at module scope (see `loadedProjectId`) so it survives the
    // remount that happens when navigating between the global and project
    // editors — a per-instance ref would miss those switches and leak tabs.
    const isProjectSwitch =
      loadedProjectId !== false &&
      loadedProjectId !== projectId;

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
          vizIds: t.vizIds,
          isLocked: t.isLocked,
          lockReason: t.lockReason,
          lockType: t.lockType,
          hidden: t.hidden,
          hasExecuted: t.hasExecuted,
        };
      });

      // Ids are UUIDs now, so the counter comes from the default tab names.
      const maxCounter = Math.max(persisted.tabCounter, nextTabCounter(persisted.tabs));

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
    } else if (isProjectSwitch) {
      // No persisted state AND this is a switch from a different project —
      // reset to a fresh empty tab so the previous project's tabs don't leak
      // into this one. On first mount or re-mount of the SAME project, leave
      // Redux alone (its current state is trustworthy or already-default).
      const freshId = crypto.randomUUID();
      dispatch(restoreQueryState({
        tabs: [{ id: freshId, name: 'Query 1', sql: '' }],
        activeTabId: freshId,
        tabCounter: 1,
      }));
    }
    loadedProjectId = projectId;

    const persistedUi = loadUiState(projectId);
    if (persistedUi) {
      dispatch(restoreUiState(persistedUi));
    }

    // Mark restore complete after two animation frames to ensure
    // the Redux state update has propagated and the save effect
    // has seen the restored state (prevents saving empty default tabs)
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        hasRestoredRef.current = true;
      });
    });

    // Cleanup: when projectId changes (or on unmount), flush the current
    // tabs synchronously to the OLD projectId. This prevents losing edits
    // that hadn't yet been persisted by the debounced save.
    const flushProjectId = projectId;
    return () => {
      if (saveTimerRef.current) {
        clearTimeout(saveTimerRef.current);
        saveTimerRef.current = null;
      }
      const currentTabs = tabsRef.current;
      const currentActiveTabId = activeTabIdRef.current;
      // Only persist if there is something worth saving (some tab has SQL).
      const hasContent = currentTabs.some((t) => t.sql.trim().length > 0);
      if (hasContent) {
        const maxCounter = nextTabCounter(currentTabs);
        saveTabState(
          {
            tabs: currentTabs.map((t) => ({
              id: t.id,
              name: t.name,
              sql: t.sql,
              defaultSchema: t.defaultSchema,
              cacheId: t.cacheId,
              vizIds: t.vizIds,
              isLocked: t.isLocked,
              lockReason: t.lockReason,
              lockType: t.lockType,
              hidden: t.hidden,
              hasExecuted: t.hasExecuted,
              updatedAt: Date.now(),
            })),
            activeTabId: currentActiveTabId,
            tabCounter: maxCounter,
            savedAt: Date.now(),
          },
          flushProjectId,
        );
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
        tabCounter: nextTabCounter(tabs),
      }));
    } catch {
      // Backend cache unavailable — leave as expired
    }
  }, [dispatch, tabs, activeTabId, projectId]);

  // Save tab state (debounced)
  useEffect(() => {
    if (saveTimerRef.current) {
      clearTimeout(saveTimerRef.current);
    }

    saveTimerRef.current = setTimeout(() => {
      // Checked inside the timer, not at effect entry: restore finishes a couple of
      // frames after mount, and returning early would leave nothing to retrigger the
      // save if the user's first edit lands before then.
      if (!hasRestoredRef.current) {
        return;
      }
      // Don't overwrite persisted state with empty default tabs.
      // This guards against a race where the save fires before
      // restore has populated Redux.
      const hasContent = tabs.some((t) => t.sql.trim().length > 0);
      if (!hasContent && tabs.length <= 1) {
        const existing = loadTabState(projectId);
        if (existing && existing.tabs.some((t) => t.sql.trim().length > 0)) {
          return; // Don't clobber persisted data with empty state
        }
      }

      const maxCounter = nextTabCounter(tabs);

      saveTabState(
        {
          tabs: tabs.map((t) => ({
            id: t.id,
            name: t.name,
            sql: t.sql,
            defaultSchema: t.defaultSchema,
            cacheId: t.cacheId,
            vizIds: t.vizIds,
            isLocked: t.isLocked,
            lockReason: t.lockReason,
            lockType: t.lockType,
            hidden: t.hidden,
            hasExecuted: t.hasExecuted,
            updatedAt: Date.now(),
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

  // Ids known to have a server-side record, so promotion does not re-create them.
  // A ref rather than state: it must not retrigger the sync effect that writes it.
  const promotedIdsRef = useRef<Set<string>>(new Set());
  const serverSyncTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Pull the server's tabs once per project and merge them into what is already here.
  useEffect(() => {
    let cancelled = false;
    promotedIdsRef.current = new Set();

    listTabs(projectId).then((serverTabs) => {
      if (cancelled || serverTabs.length === 0) {
        return;
      }
      for (const tab of serverTabs) {
        promotedIdsRef.current.add(tab.id);
      }

      const merged = reconcileTabs(
        tabsRef.current.map((t) => ({
          id: t.id,
          name: t.name,
          sql: t.sql,
          defaultSchema: t.defaultSchema,
          cacheId: t.cacheId,
          vizIds: t.vizIds,
          isLocked: t.isLocked,
          lockReason: t.lockReason,
          lockType: t.lockType,
          hidden: t.hidden,
          updatedAt: Date.now(),
        })),
        serverTabs,
      );

      dispatch(restoreQueryState({
        tabs: merged.map((t) => ({
          id: t.id,
          name: t.name,
          sql: t.sql,
          defaultSchema: t.defaultSchema,
          cacheId: t.cacheId,
          vizIds: t.vizIds,
          isLocked: t.isLocked,
          lockReason: t.lockReason,
          lockType: t.lockType,
          hidden: t.hidden,
          // Anything the server holds was promoted, which means it was executed.
          hasExecuted: true,
          resultsExpired: t.sql.trim().length > 0,
        })),
        activeTabId: activeTabIdRef.current,
        tabCounter: nextTabCounter(merged),
      }));
    });

    return () => {
      cancelled = true;
    };
  }, [dispatch, projectId]);

  // Push promoted tabs to the server, on a slower beat than the local save: SQL
  // changes on every keystroke and each change is now a potential network write.
  useEffect(() => {
    if (serverSyncTimerRef.current) {
      clearTimeout(serverSyncTimerRef.current);
    }

    serverSyncTimerRef.current = setTimeout(() => {
      // Checked here rather than at effect entry: restore completes a couple of frames
      // after mount, and an early return would leave nothing to retrigger the sync.
      if (!hasRestoredRef.current) {
        return;
      }
      for (const tab of tabsRef.current) {
        const alreadyPromoted = promotedIdsRef.current.has(tab.id);
        const promote = shouldPromote({
          hasExecuted: !!tab.hasExecuted,
          // A hidden tab is a closed tab, which is the second promotion trigger: it
          // has left the tab strip, so the project tree is the only way back to it.
          isClosing: !!tab.hidden,
          sql: tab.sql,
          conversationLength: conversationLengthFor(projectId, tab.id),
          alreadyPromoted,
        });

        if (!promote && !alreadyPromoted) {
          continue;
        }

        const payload = {
          id: tab.id,
          projectId,
          name: tab.name,
          sql: tab.sql,
          defaultSchema: tab.defaultSchema,
          hidden: !!tab.hidden,
          vizIds: tab.vizIds,
          locked: tab.isLocked,
          lockReason: tab.lockReason,
          lockType: tab.lockType,
          pinned: tab.isPinned,
          cacheId: tab.cacheId,
        };

        // Every server call is best-effort. localStorage has already been written, so
        // a failure here costs sync, never the user's SQL.
        if (alreadyPromoted) {
          updateTab(tab.id, payload).catch(() => {
            // Retried on the next sync tick.
          });
        } else {
          promotedIdsRef.current.add(tab.id);
          createTab(payload)
            .then(() => {
              // The sidebar caches its tab list, so a newly promoted tab is invisible
              // until the list is refetched. Without this, a project expanded before
              // the first query keeps showing an empty tree.
              queryClient.invalidateQueries({ queryKey: ['project-tabs', projectId] });
            })
            .catch(() => {
              // Allow a later tick to try again.
              promotedIdsRef.current.delete(tab.id);
            });
        }
      }
    }, 1500);

    return () => {
      if (serverSyncTimerRef.current) {
        clearTimeout(serverSyncTimerRef.current);
      }
    };
  }, [tabs, activeTabId, projectId, queryClient]);

  // Save UI state (immediate — changes are infrequent)
  useEffect(() => {
    if (!hasRestoredRef.current) {
      return;
    }
    saveUiState({ editorHeight }, projectId);
  }, [editorHeight, projectId]);

  // Callback for SqlLabPage to cache results when they arrive
  const onResultsCached = useCallback(
    (tabId: string, result: QueryResult, executionTime: number, cacheId?: string) => {
      cacheResults(tabId, result, executionTime, projectId, cacheId);
    },
    [projectId],
  );

  return { onResultsCached };
}
