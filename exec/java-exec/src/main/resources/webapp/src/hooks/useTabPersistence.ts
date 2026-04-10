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

import { useEffect } from 'react';
import { useSelector, useDispatch } from 'react-redux';
import type { RootState, AppDispatch } from '../store';
import { addTab, setSql, setDefaultSchema, setCacheId, addVizToTab } from '../store/querySlice';
import type { QueryTab } from '../store/querySlice';

const TABS_KEY = 'drill_query_tabs';
const ACTIVE_TAB_KEY = 'drill_active_tab';

interface PersistedTabData {
  id: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  cacheId?: string;
  vizIds?: string[];
  isLocked?: boolean;
  isPinned?: boolean;
}

/**
 * Load tabs from localStorage and restore them to Redux state.
 */
export function loadTabsFromStorage(): PersistedTabData[] {
  try {
    const stored = localStorage.getItem(TABS_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch (error) {
    console.error('Failed to load tabs from storage:', error);
    return [];
  }
}

/**
 * Save tabs to localStorage whenever they change.
 */
export function saveTabsToStorage(tabs: QueryTab[]): void {
  try {
    // Only save tabs with SQL content; empty tabs don't need to be persisted
    const data: PersistedTabData[] = tabs
      .filter((tab) => tab.sql?.trim())
      .map((tab) => ({
        id: tab.id,
        name: tab.name,
        sql: tab.sql,
        defaultSchema: tab.defaultSchema,
        cacheId: tab.cacheId,
        vizIds: tab.vizIds,
        isLocked: tab.isLocked,
        isPinned: tab.isPinned,
      }));
    localStorage.setItem(TABS_KEY, JSON.stringify(data));
  } catch (error) {
    console.error('Failed to save tabs to storage:', error);
  }
}

/**
 * Save the active tab ID.
 */
export function saveActiveTabId(tabId: string): void {
  try {
    localStorage.setItem(ACTIVE_TAB_KEY, tabId);
  } catch (error) {
    console.error('Failed to save active tab ID:', error);
  }
}

/**
 * Load the active tab ID.
 */
export function loadActiveTabId(): string | null {
  try {
    return localStorage.getItem(ACTIVE_TAB_KEY);
  } catch (error) {
    console.error('Failed to load active tab ID:', error);
    return null;
  }
}

/**
 * Hook to sync tabs state with localStorage.
 * Call this in the main SqlLabPage to enable tab persistence.
 */
export function useTabPersistence(): void {
  const tabs = useSelector((state: RootState) => state.query.tabs);
  const activeTabId = useSelector((state: RootState) => state.query.activeTabId);

  // Save tabs whenever they change
  useEffect(() => {
    saveTabsToStorage(tabs);
  }, [tabs]);

  // Save active tab whenever it changes
  useEffect(() => {
    saveActiveTabId(activeTabId);
  }, [activeTabId]);
}

/**
 * Hook to restore tabs from localStorage on mount.
 * Call this in the main SqlLabPage to restore persisted tabs.
 */
export function useRestoreTabs(): void {
  const dispatch = useDispatch<AppDispatch>();
  const tabs = useSelector((state: RootState) => state.query.tabs);

  useEffect(() => {
    // Only restore if we're on the initial tab (default state)
    if (tabs.length === 1 && tabs[0].id === 'tab-1' && !tabs[0].sql) {
      const saved = loadTabsFromStorage();
      if (saved.length > 0) {
        // Restore each saved tab
        for (const tabData of saved.slice(1)) {
          // Skip first since tab-1 already exists
          dispatch(addTab(tabData.name));
        }

        // Update the first tab with saved data if it exists
        if (saved.length > 0) {
          const firstTab = saved[0];
          dispatch(setSql({ tabId: 'tab-1', sql: firstTab.sql }));
          if (firstTab.defaultSchema) {
            dispatch(setDefaultSchema({ tabId: 'tab-1', schema: firstTab.defaultSchema }));
          }
          if (firstTab.cacheId) {
            dispatch(setCacheId({ tabId: 'tab-1', cacheId: firstTab.cacheId }));
          }
          // Restore visualization links
          if (firstTab.vizIds) {
            for (const vizId of firstTab.vizIds) {
              dispatch(addVizToTab({ tabId: 'tab-1', vizId }));
            }
          }
        }
      }
    } else if (tabs.length > 1) {
      // Additional tabs have been created, restore their vizIds by matching SQL
      const saved = loadTabsFromStorage();
      if (saved.length > 0) {
        for (const tab of tabs) {
          const savedTab = saved.find((s) => s.sql === tab.sql);
          if (savedTab?.vizIds && tab.vizIds?.length === 0) {
            for (const vizId of savedTab.vizIds) {
              dispatch(addVizToTab({ tabId: tab.id, vizId }));
            }
          }
        }
      }
    }
  }, [dispatch, tabs]);
}
