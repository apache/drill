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
import { createSlice, PayloadAction } from '@reduxjs/toolkit';
import type { QueryResult, QueryError } from '../types';

export interface QueryTab {
  id: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  results?: QueryResult;
  error?: QueryError;
  isExecuting: boolean;
  executionTime?: number;
  resultsExpired?: boolean;
  cacheId?: string; // Backend cache ID for persistent result retrieval
  vizIds?: string[]; // IDs of visualizations created from this tab
  isLocked?: boolean; // Prevents edits, renames, deletion
  lockReason?: string; // Optional note explaining why it's locked
  lockType?: 'manual' | 'api'; // Drives which icon is shown
  isPinned?: boolean; // Pinned tabs get longer cache TTL (2 hours vs 30 min default)
  /**
   * A query has been run from this tab at some point, successfully or not. This is one
   * of the two triggers that promote a tab to server-side storage, so it has to survive
   * a reload — deriving it from `results` would lose it as soon as they expire.
   */
  hasExecuted?: boolean;
  /** Closed but not deleted: out of the tab strip, still in the project tree. */
  hidden?: boolean;
}

interface QueryState {
  tabs: QueryTab[];
  activeTabId: string;
}

// The seed tab gets a UUID like any other, so a "tab-1" can never reach the server.
// Restoring persisted state replaces it before the user sees anything.
const initialTabId = crypto.randomUUID();

const initialTab: QueryTab = {
  id: initialTabId,
  name: 'Query 1',
  sql: '',
  isExecuting: false,
  vizIds: undefined,
  isLocked: false,
};

const initialState: QueryState = {
  tabs: [initialTab],
  activeTabId: initialTabId,
};

// Drives tab NAMES only ("Query 3"). Ids are UUIDs: tabs are durable server-side
// records now, and a per-project counter that resets on reload cannot produce a
// stable identity. See docs/dev/TabPersistence.md.
let tabCounter = 1;

function newTabId(): string {
  return crypto.randomUUID();
}

const querySlice = createSlice({
  name: 'query',
  initialState,
  reducers: {
    setSql: (state, action: PayloadAction<{ tabId: string; sql: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab && !tab.isLocked) {
        tab.sql = action.payload.sql;
      }
    },
    setDefaultSchema: (state, action: PayloadAction<{ tabId: string; schema: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.defaultSchema = action.payload.schema;
      }
    },
    setExecuting: (state, action: PayloadAction<{ tabId: string; isExecuting: boolean }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.isExecuting = action.payload.isExecuting;
        if (action.payload.isExecuting) {
          // Set when the run starts, not when it succeeds: a cancelled or failed
          // query is still work the user will want back.
          tab.hasExecuted = true;
          tab.error = undefined;
        }
      }
    },
    setResults: (
      state,
      action: PayloadAction<{ tabId: string; results: QueryResult; executionTime: number; cacheId?: string }>
    ) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.results = action.payload.results;
        tab.executionTime = action.payload.executionTime;
        tab.isExecuting = false;
        tab.error = undefined;
        tab.resultsExpired = false;
        tab.hasExecuted = true;
        if (action.payload.cacheId) {
          tab.cacheId = action.payload.cacheId;
        }
      }
    },
    setCacheId: (state, action: PayloadAction<{ tabId: string; cacheId: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.cacheId = action.payload.cacheId;
      }
    },
    setError: (state, action: PayloadAction<{ tabId: string; error: QueryError }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.error = action.payload.error;
        tab.isExecuting = false;
        tab.results = undefined;
        tab.hasExecuted = true;
      }
    },
    clearResults: (state, action: PayloadAction<string>) => {
      const tab = state.tabs.find((t) => t.id === action.payload);
      if (tab) {
        tab.results = undefined;
        tab.error = undefined;
        tab.executionTime = undefined;
      }
    },
    addTab: (state, action: PayloadAction<string | undefined>) => {
      tabCounter++;
      const newTab: QueryTab = {
        id: newTabId(),
        name: action.payload || `Query ${tabCounter}`,
        sql: '',
        isExecuting: false,
      };
      state.tabs.push(newTab);
      state.activeTabId = newTab.id;
    },
    duplicateTab: (state, action: PayloadAction<string>) => {
      const sourceTab = state.tabs.find((t) => t.id === action.payload);
      if (!sourceTab) {
        return;
      }
      tabCounter++;
      const newTab: QueryTab = {
        id: newTabId(),
        name: `${sourceTab.name} (copy)`,
        sql: sourceTab.sql,
        defaultSchema: sourceTab.defaultSchema,
        cacheId: sourceTab.cacheId,
        vizIds: sourceTab.vizIds ? [...sourceTab.vizIds] : undefined,
        isLocked: sourceTab.isLocked,
        lockReason: sourceTab.lockReason,
        lockType: sourceTab.lockType,
        isExecuting: false,
      };
      const sourceIndex = state.tabs.findIndex((t) => t.id === action.payload);
      state.tabs.splice(sourceIndex + 1, 0, newTab);
      state.activeTabId = newTab.id;
    },
    removeTab: (state, action: PayloadAction<string>) => {
      const index = state.tabs.findIndex((t) => t.id === action.payload);
      if (index !== -1 && state.tabs.length > 1) {
        state.tabs.splice(index, 1);
        if (state.activeTabId === action.payload) {
          state.activeTabId = state.tabs[Math.max(0, index - 1)].id;
        }
      }
    },
    setActiveTab: (state, action: PayloadAction<string>) => {
      if (state.tabs.some((t) => t.id === action.payload)) {
        state.activeTabId = action.payload;
      }
    },
    renameTab: (state, action: PayloadAction<{ tabId: string; name: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.name = action.payload.name;
      }
    },
    loadQuery: (
      state,
      action: PayloadAction<{ tabId: string; sql: string; name?: string; defaultSchema?: string }>
    ) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab && !tab.isLocked) {
        tab.sql = action.payload.sql;
        if (action.payload.name) {
          tab.name = action.payload.name;
        }
        if (action.payload.defaultSchema) {
          tab.defaultSchema = action.payload.defaultSchema;
        }
        tab.results = undefined;
        tab.error = undefined;
      }
    },
    restoreQueryState: (
      state,
      action: PayloadAction<{
        tabs: {
          id: string;
          name: string;
          sql: string;
          defaultSchema?: string;
          results?: QueryResult;
          executionTime?: number;
          resultsExpired?: boolean;
          cacheId?: string;
          vizIds?: string[];
          isLocked?: boolean;
          lockReason?: string;
          lockType?: 'manual' | 'api';
        }[];
        activeTabId: string;
        tabCounter: number;
      }>
    ) => {
      state.tabs = action.payload.tabs.map((t) => ({
        id: t.id,
        name: t.name,
        sql: t.sql,
        defaultSchema: t.defaultSchema,
        results: t.results,
        executionTime: t.executionTime,
        isExecuting: false,
        resultsExpired: t.resultsExpired,
        cacheId: t.cacheId,
        vizIds: t.vizIds,
        isLocked: t.isLocked,
        lockReason: t.lockReason,
        lockType: t.lockType,
      }));
      state.activeTabId = action.payload.activeTabId;
      tabCounter = action.payload.tabCounter;
    },
    clearResultsExpired: (state, action: PayloadAction<string>) => {
      const tab = state.tabs.find((t) => t.id === action.payload);
      if (tab) {
        tab.resultsExpired = false;
      }
    },
    addVizToTab: (state, action: PayloadAction<{ tabId: string; vizId: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        if (!tab.vizIds) {
          tab.vizIds = [];
        }
        if (!tab.vizIds.includes(action.payload.vizId)) {
          tab.vizIds.push(action.payload.vizId);
        }
      }
    },
    removeVizFromTab: (state, action: PayloadAction<{ tabId: string; vizId: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab && tab.vizIds) {
        tab.vizIds = tab.vizIds.filter((id) => id !== action.payload.vizId);
      }
    },
    lockTab: (state, action: PayloadAction<{ tabId: string; reason?: string; lockType?: 'manual' | 'api' }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.isLocked = true;
        tab.lockReason = action.payload.reason;
        tab.lockType = action.payload.lockType ?? 'manual';
      }
    },
    unlockTab: (state, action: PayloadAction<string>) => {
      const tab = state.tabs.find((t) => t.id === action.payload);
      if (tab) {
        tab.isLocked = false;
        tab.lockReason = undefined;
        tab.lockType = undefined;
      }
    },
    pinTab: (state, action: PayloadAction<string>) => {
      const tab = state.tabs.find((t) => t.id === action.payload);
      if (tab) {
        tab.isPinned = true;
      }
    },
    unpinTab: (state, action: PayloadAction<string>) => {
      const tab = state.tabs.find((t) => t.id === action.payload);
      if (tab) {
        tab.isPinned = false;
      }
    },
  },
});

export const {
  setSql,
  setDefaultSchema,
  setExecuting,
  setResults,
  setError,
  clearResults,
  addTab,
  duplicateTab,
  removeTab,
  setActiveTab,
  renameTab,
  loadQuery,
  restoreQueryState,
  clearResultsExpired,
  setCacheId,
  addVizToTab,
  removeVizFromTab,
  lockTab,
  unlockTab,
  pinTab,
  unpinTab,
} = querySlice.actions;

export default querySlice.reducer;
