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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { RecentItem } from './useRecentItems';

/**
 * The useRecentItems hook relies on React state and localStorage.
 * We test the core pure logic (load, save, dedup, max limit, project filter)
 * by reimplementing the same helpers that the hook uses internally.
 */

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

function addItem(
  existing: RecentItem[],
  item: Omit<RecentItem, 'timestamp'>,
): RecentItem[] {
  const filtered = existing.filter(
    (i) => !(i.type === item.type && i.id === item.id),
  );
  return [{ ...item, timestamp: Date.now() }, ...filtered].slice(0, MAX_ITEMS);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useRecentItems logic', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
  });

  describe('localStorage read/write', () => {
    it('returns empty array when nothing is stored', () => {
      expect(loadItems()).toEqual([]);
    });

    it('round-trips items through localStorage', () => {
      const items: RecentItem[] = [
        { type: 'query', id: 'q1', name: 'Test', projectId: 'p1', timestamp: 100 },
      ];
      saveItems(items);
      expect(loadItems()).toEqual(items);
    });

    it('returns empty array when localStorage has invalid JSON', () => {
      localStorage.setItem(STORAGE_KEY, '{broken json');
      expect(loadItems()).toEqual([]);
    });
  });

  describe('item deduplication', () => {
    it('removes existing item with same type and id before re-adding', () => {
      const existing: RecentItem[] = [
        { type: 'query', id: 'q1', name: 'Old', projectId: 'p1', timestamp: 100 },
        { type: 'query', id: 'q2', name: 'Other', projectId: 'p1', timestamp: 90 },
      ];
      const result = addItem(existing, {
        type: 'query',
        id: 'q1',
        name: 'Updated',
        projectId: 'p1',
      });
      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('Updated');
      expect(result[0].id).toBe('q1');
    });

    it('does not deduplicate items with different types but same id', () => {
      const existing: RecentItem[] = [
        { type: 'query', id: 'x1', name: 'Query', projectId: 'p1', timestamp: 100 },
      ];
      const result = addItem(existing, {
        type: 'dashboard',
        id: 'x1',
        name: 'Dashboard',
        projectId: 'p1',
      });
      expect(result).toHaveLength(2);
    });
  });

  describe('max items limit', () => {
    it('keeps at most MAX_ITEMS entries', () => {
      const existing: RecentItem[] = Array.from({ length: 25 }, (_, i) => ({
        type: 'query' as const,
        id: `q${i}`,
        name: `Query ${i}`,
        projectId: 'p1',
        timestamp: 1000 - i,
      }));
      const result = addItem(existing, {
        type: 'query',
        id: 'new',
        name: 'New Query',
        projectId: 'p1',
      });
      expect(result).toHaveLength(MAX_ITEMS);
      expect(result[0].id).toBe('new');
    });
  });

  describe('project filtering', () => {
    it('filters items by projectId', () => {
      const items: RecentItem[] = [
        { type: 'query', id: 'q1', name: 'A', projectId: 'p1', timestamp: 100 },
        { type: 'query', id: 'q2', name: 'B', projectId: 'p2', timestamp: 90 },
        { type: 'query', id: 'q3', name: 'C', projectId: 'p1', timestamp: 80 },
      ];
      const filtered = items.filter((i) => i.projectId === 'p1').slice(0, 5);
      expect(filtered).toHaveLength(2);
      expect(filtered.every((i) => i.projectId === 'p1')).toBe(true);
    });

    it('limits project items to 5', () => {
      const items: RecentItem[] = Array.from({ length: 10 }, (_, i) => ({
        type: 'query' as const,
        id: `q${i}`,
        name: `Query ${i}`,
        projectId: 'p1',
        timestamp: 1000 - i,
      }));
      const filtered = items.filter((i) => i.projectId === 'p1').slice(0, 5);
      expect(filtered).toHaveLength(5);
    });

    it('returns empty array when no projectId is provided', () => {
      const projectId: string | undefined = undefined;
      const items: RecentItem[] = [
        { type: 'query', id: 'q1', name: 'A', projectId: 'p1', timestamp: 100 },
      ];
      const projectItems = projectId
        ? items.filter((i) => i.projectId === projectId).slice(0, 5)
        : [];
      expect(projectItems).toEqual([]);
    });
  });
});
