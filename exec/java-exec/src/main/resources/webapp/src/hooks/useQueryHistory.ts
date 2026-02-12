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
import { useState, useCallback, useEffect, useRef } from 'react';
import type { QueryHistoryEntry } from '../types';

const GLOBAL_STORAGE_KEY = 'drill-sqllab-query-history';
const MAX_HISTORY_ENTRIES = 100;

function getStorageKey(projectId?: string, tabId?: string): string {
  if (projectId && tabId) {
    return `drill-sqllab-query-history-${projectId}-${tabId}`;
  }
  return GLOBAL_STORAGE_KEY;
}

function loadHistory(key: string): QueryHistoryEntry[] {
  try {
    const stored = localStorage.getItem(key);
    if (stored) {
      return JSON.parse(stored);
    }
  } catch {
    // Ignore parse errors
  }
  return [];
}

function saveHistory(key: string, entries: QueryHistoryEntry[]): void {
  try {
    localStorage.setItem(key, JSON.stringify(entries));
  } catch {
    // Ignore storage errors
  }
}

export function useQueryHistory(projectId?: string, tabId?: string) {
  const key = getStorageKey(projectId, tabId);
  const keyRef = useRef(key);
  const [history, setHistory] = useState<QueryHistoryEntry[]>(() => loadHistory(key));

  // Reload history when the storage key changes (tab switch or project change)
  useEffect(() => {
    if (key !== keyRef.current) {
      keyRef.current = key;
      setHistory(loadHistory(key));
    }
  }, [key]);

  const addEntry = useCallback((entry: QueryHistoryEntry) => {
    setHistory((prev) => {
      const updated = [entry, ...prev].slice(0, MAX_HISTORY_ENTRIES);
      saveHistory(keyRef.current, updated);
      return updated;
    });
  }, []);

  const clearHistory = useCallback(() => {
    setHistory([]);
    saveHistory(keyRef.current, []);
  }, []);

  return { history, addEntry, clearHistory };
}
