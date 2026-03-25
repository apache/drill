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

export interface PersistedTab {
  id: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  cacheId?: string; // Backend cache ID for result restoration
  vizIds?: string[]; // IDs of visualizations created from this tab
  isLocked?: boolean; // Prevents edits, renames, deletion
  lockReason?: string; // Optional note explaining why it's locked
  lockType?: 'manual' | 'api'; // Drives which icon is shown
}

export interface PersistedTabState {
  tabs: PersistedTab[];
  activeTabId: string;
  tabCounter: number;
  savedAt: number;
}

export interface PersistedUiState {
  sidebarCollapsed: boolean;
  sidebarWidth: number;
  editorHeight: number;
}

function tabsKey(projectId?: string): string {
  return `drill-sqllab-tabs-${projectId || 'global'}`;
}

function uiKey(projectId?: string): string {
  return `drill-sqllab-ui-${projectId || 'global'}`;
}

export function loadTabState(projectId?: string): PersistedTabState | null {
  try {
    const raw = localStorage.getItem(tabsKey(projectId));
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw) as PersistedTabState;
    if (!parsed.tabs || !Array.isArray(parsed.tabs) || parsed.tabs.length === 0) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

export function saveTabState(state: PersistedTabState, projectId?: string): void {
  try {
    localStorage.setItem(tabsKey(projectId), JSON.stringify(state));
  } catch {
    // Ignore storage errors (quota exceeded, etc.)
  }
}

export function loadUiState(projectId?: string): PersistedUiState | null {
  try {
    const raw = localStorage.getItem(uiKey(projectId));
    if (!raw) {
      return null;
    }
    return JSON.parse(raw) as PersistedUiState;
  } catch {
    return null;
  }
}

export function saveUiState(state: PersistedUiState, projectId?: string): void {
  try {
    localStorage.setItem(uiKey(projectId), JSON.stringify(state));
  } catch {
    // Ignore storage errors
  }
}

export function clearPersistedState(projectId?: string): void {
  try {
    localStorage.removeItem(tabsKey(projectId));
    localStorage.removeItem(uiKey(projectId));
  } catch {
    // Ignore storage errors
  }
}
