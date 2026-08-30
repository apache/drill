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
  /**
   * Last local edit, epoch millis. Reconciliation compares this against the server
   * copy's updatedAt to decide which side wins, so it must be set on every write.
   */
  updatedAt?: number;
  name: string;
  sql: string;
  defaultSchema?: string;
  cacheId?: string; // Backend cache ID for result restoration
  vizIds?: string[]; // IDs of visualizations created from this tab
  isLocked?: boolean; // Prevents edits, renames, deletion
  lockReason?: string; // Optional note explaining why it's locked
  lockType?: 'manual' | 'api'; // Drives which icon is shown
  /**
   * Closed but not deleted. A hidden tab leaves the tab strip and stays listed in the
   * project tree, which is what makes closing non-destructive.
   */
  hidden?: boolean;
}

export interface PersistedTabState {
  tabs: PersistedTab[];
  activeTabId: string;
  tabCounter: number;
  savedAt: number;
}

/** Ids written before tabs became durable records. */
const LEGACY_TAB_ID = /^tab-\d+$/;

/**
 * Rewrites pre-UUID `tab-N` ids, once, before anything else reads the restored state.
 *
 * A tab id used to come from a counter that reset per project and per reload, which is
 * fine for a key into localStorage and useless as the identity of a server-side record.
 * Idempotent: ids that already look like UUIDs are left exactly as they are, so a tab
 * already promoted under its id keeps matching its server copy.
 */
export function migrateTabIds(state: PersistedTabState): PersistedTabState {
  const remapped = new Map<string, string>();
  const tabs = state.tabs.map((tab) => {
    if (!LEGACY_TAB_ID.test(tab.id)) {
      return tab;
    }
    const id = crypto.randomUUID();
    remapped.set(tab.id, id);
    return { ...tab, id };
  });

  let activeTabId = remapped.get(state.activeTabId) ?? state.activeTabId;
  // A saved activeTabId pointing at no tab would leave the editor with nothing
  // selected; fall back to the first tab rather than propagating the dangling id.
  if (!tabs.some((t) => t.id === activeTabId)) {
    activeTabId = tabs.length > 0 ? tabs[0].id : activeTabId;
  }

  return { ...state, tabs, activeTabId };
}

export interface PersistedUiState {
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
    // Migrate here rather than at the call sites: this is the only way persisted tab
    // state enters the app, so nothing downstream ever sees a legacy id.
    const migrated = migrateTabIds(parsed);
    if (migrated.tabs.some((t, i) => t.id !== parsed.tabs[i].id)) {
      saveTabState(migrated, projectId);
    }
    return migrated;
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

const TABS_KEY_PREFIX = 'drill-sqllab-tabs-';

export interface TabStateEntry {
  /** null when this is the global (`/query`) bucket */
  projectId: string | null;
  state: PersistedTabState;
}

/**
 * Enumerate every persisted tab state in localStorage. Used by the Workspace
 * Recovery UI to surface stranded tabs from project-id mix-ups.
 */
export function listAllTabStates(): TabStateEntry[] {
  const out: TabStateEntry[] = [];
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i);
      if (!k || !k.startsWith(TABS_KEY_PREFIX)) {
        continue;
      }
      const id = k.slice(TABS_KEY_PREFIX.length);
      const projectId = id === 'global' ? null : id;
      const raw = localStorage.getItem(k);
      if (!raw) {
        continue;
      }
      try {
        const parsed = JSON.parse(raw) as PersistedTabState;
        if (!parsed.tabs || !Array.isArray(parsed.tabs) || parsed.tabs.length === 0) {
          continue;
        }
        out.push({ projectId, state: parsed });
      } catch {
        // Skip malformed entries
      }
    }
  } catch {
    // localStorage may be unavailable
  }
  return out;
}

/**
 * Move a persisted tab state from one project bucket to another.
 * If `from` and `to` are the same, this is a no-op. Replaces destination.
 * Returns true on success.
 */
export function moveTabState(
  from: { projectId: string | null },
  to: { projectId: string | null },
): boolean {
  try {
    const fromKey = tabsKey(from.projectId ?? undefined);
    const toKey = tabsKey(to.projectId ?? undefined);
    if (fromKey === toKey) {
      return false;
    }
    const raw = localStorage.getItem(fromKey);
    if (!raw) {
      return false;
    }
    localStorage.setItem(toKey, raw);
    localStorage.removeItem(fromKey);
    return true;
  } catch {
    return false;
  }
}

/** Remove a single persisted tab state. */
export function deleteTabState(projectId: string | null): void {
  try {
    localStorage.removeItem(tabsKey(projectId ?? undefined));
  } catch {
    // Ignore storage errors
  }
}
