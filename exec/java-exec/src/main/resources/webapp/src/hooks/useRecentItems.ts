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
import { useState, useCallback, useEffect } from 'react';

export interface RecentItem {
  type: 'query' | 'visualization' | 'dashboard' | 'wiki';
  id: string;
  name: string;
  projectId: string;
  timestamp: number;
}

const STORAGE_KEY = 'drill.sqllab.recent_items';
const MAX_ITEMS = 20;

function loadItems(): RecentItem[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

function saveItems(items: RecentItem[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(items));
}

export function useRecentItems(projectId?: string) {
  const [items, setItems] = useState<RecentItem[]>(loadItems);

  // Sync across tabs
  useEffect(() => {
    const handler = (e: StorageEvent) => {
      if (e.key === STORAGE_KEY) {
        setItems(loadItems());
      }
    };
    window.addEventListener('storage', handler);
    return () => window.removeEventListener('storage', handler);
  }, []);

  const addRecentItem = useCallback((item: Omit<RecentItem, 'timestamp'>) => {
    setItems((prev) => {
      const filtered = prev.filter((i) => !(i.type === item.type && i.id === item.id));
      const next = [{ ...item, timestamp: Date.now() }, ...filtered].slice(0, MAX_ITEMS);
      saveItems(next);
      return next;
    });
  }, []);

  // Filter to current project
  const projectItems = projectId
    ? items.filter((i) => i.projectId === projectId).slice(0, 5)
    : [];

  return { recentItems: projectItems, allRecentItems: items, addRecentItem };
}
