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

interface UiState {
  sidebarCollapsed: boolean;
  sidebarWidth: number;
  editorHeight: number;
  selectedSchema?: string;
  selectedTable?: string;
  resultsPanelTab: 'results' | 'visualization' | 'history';
}

const initialState: UiState = {
  sidebarCollapsed: false,
  sidebarWidth: 280,
  editorHeight: 300,
  resultsPanelTab: 'results',
};

const uiSlice = createSlice({
  name: 'ui',
  initialState,
  reducers: {
    toggleSidebar: (state) => {
      state.sidebarCollapsed = !state.sidebarCollapsed;
    },
    setSidebarWidth: (state, action: PayloadAction<number>) => {
      state.sidebarWidth = Math.max(200, Math.min(500, action.payload));
    },
    setEditorHeight: (state, action: PayloadAction<number>) => {
      state.editorHeight = Math.max(150, Math.min(600, action.payload));
    },
    setSelectedSchema: (state, action: PayloadAction<string | undefined>) => {
      state.selectedSchema = action.payload;
    },
    setSelectedTable: (state, action: PayloadAction<string | undefined>) => {
      state.selectedTable = action.payload;
    },
    setResultsPanelTab: (state, action: PayloadAction<'results' | 'visualization' | 'history'>) => {
      state.resultsPanelTab = action.payload;
    },
  },
});

export const {
  toggleSidebar,
  setSidebarWidth,
  setEditorHeight,
  setSelectedSchema,
  setSelectedTable,
  setResultsPanelTab,
} = uiSlice.actions;

export default uiSlice.reducer;
