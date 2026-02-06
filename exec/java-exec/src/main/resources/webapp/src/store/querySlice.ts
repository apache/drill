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

interface QueryTab {
  id: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  results?: QueryResult;
  error?: QueryError;
  isExecuting: boolean;
  executionTime?: number;
}

interface QueryState {
  tabs: QueryTab[];
  activeTabId: string;
}

const initialTab: QueryTab = {
  id: 'tab-1',
  name: 'Query 1',
  sql: '',
  isExecuting: false,
};

const initialState: QueryState = {
  tabs: [initialTab],
  activeTabId: 'tab-1',
};

let tabCounter = 1;

const querySlice = createSlice({
  name: 'query',
  initialState,
  reducers: {
    setSql: (state, action: PayloadAction<{ tabId: string; sql: string }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
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
          tab.error = undefined;
        }
      }
    },
    setResults: (
      state,
      action: PayloadAction<{ tabId: string; results: QueryResult; executionTime: number }>
    ) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.results = action.payload.results;
        tab.executionTime = action.payload.executionTime;
        tab.isExecuting = false;
        tab.error = undefined;
      }
    },
    setError: (state, action: PayloadAction<{ tabId: string; error: QueryError }>) => {
      const tab = state.tabs.find((t) => t.id === action.payload.tabId);
      if (tab) {
        tab.error = action.payload.error;
        tab.isExecuting = false;
        tab.results = undefined;
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
    addTab: (state) => {
      tabCounter++;
      const newTab: QueryTab = {
        id: `tab-${tabCounter}`,
        name: `Query ${tabCounter}`,
        sql: '',
        isExecuting: false,
      };
      state.tabs.push(newTab);
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
      if (tab) {
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
  removeTab,
  setActiveTab,
  renameTab,
  loadQuery,
} = querySlice.actions;

export default querySlice.reducer;
