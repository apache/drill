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
import { describe, it, expect } from 'vitest';
import reducer, { addTab, duplicateTab, hideTab, deleteTab, lockTab, setSql } from './querySlice';
import { migrateTabIds } from '../utils/workspacePersistence';
import type { PersistedTabState } from '../utils/workspacePersistence';

const UUID_PREFIX = /^[0-9a-f]{8}-[0-9a-f]{4}-/;

describe('querySlice tab ids', () => {
  it('gives new tabs UUID ids', () => {
    const state = reducer(undefined, addTab(undefined));
    const created = state.tabs[state.tabs.length - 1];
    expect(created.id).toMatch(UUID_PREFIX);
  });

  it('still numbers tab names sequentially', () => {
    let state = reducer(undefined, addTab(undefined));
    state = reducer(state, addTab(undefined));
    expect(state.tabs[state.tabs.length - 1].name).toMatch(/^Query \d+$/);
  });

  it('gives duplicated tabs their own UUID', () => {
    let state = reducer(undefined, addTab(undefined));
    const source = state.tabs[state.tabs.length - 1].id;
    state = reducer(state, duplicateTab(source));
    const copy = state.tabs[state.tabs.length - 1];
    expect(copy.id).toMatch(UUID_PREFIX);
    expect(copy.id).not.toBe(source);
  });

  it('never repeats an id across many new tabs', () => {
    let state = reducer(undefined, addTab(undefined));
    for (let i = 0; i < 25; i++) {
      state = reducer(state, addTab(undefined));
    }
    const ids = state.tabs.map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
  });
});

describe('migrateTabIds', () => {
  it('rewrites legacy tab-N ids and keeps activeTabId pointing at the same tab', () => {
    const migrated = migrateTabIds({
      tabs: [
        { id: 'tab-1', name: 'Query 1', sql: 'SELECT 1' },
        { id: 'tab-2', name: 'Query 2', sql: 'SELECT 2' },
      ],
      activeTabId: 'tab-2',
      tabCounter: 2,
      savedAt: 0,
    } as PersistedTabState);

    expect(migrated.tabs[0].id).not.toBe('tab-1');
    expect(migrated.tabs[1].id).not.toBe('tab-2');
    expect(migrated.tabs[0].id).toMatch(UUID_PREFIX);
    expect(migrated.activeTabId).toBe(migrated.tabs[1].id);
  });

  it('leaves already-migrated state untouched', () => {
    const uuid = '11111111-2222-3333-4444-555555555555';
    const migrated = migrateTabIds({
      tabs: [{ id: uuid, name: 'Query 1', sql: 'SELECT 1' }],
      activeTabId: uuid,
      tabCounter: 1,
      savedAt: 0,
    } as PersistedTabState);

    expect(migrated.tabs[0].id).toBe(uuid);
    expect(migrated.activeTabId).toBe(uuid);
  });

  it('preserves everything else about a tab', () => {
    const migrated = migrateTabIds({
      tabs: [{
        id: 'tab-1',
        name: 'Important',
        sql: 'SELECT 1',
        vizIds: ['v1'],
        isLocked: true,
        lockType: 'api',
      }],
      activeTabId: 'tab-1',
      tabCounter: 1,
      savedAt: 0,
    } as PersistedTabState);

    expect(migrated.tabs[0].name).toBe('Important');
    expect(migrated.tabs[0].vizIds).toEqual(['v1']);
    expect(migrated.tabs[0].isLocked).toBe(true);
    expect(migrated.tabs[0].lockType).toBe('api');
  });

  /**
   * A half-migrated state is possible if a previous load was interrupted. Only the
   * legacy ids should move; the UUID one must keep its identity so any server record
   * already promoted under it still matches.
   */
  it('migrates only the legacy ids in a mixed state', () => {
    const uuid = '11111111-2222-3333-4444-555555555555';
    const migrated = migrateTabIds({
      tabs: [
        { id: uuid, name: 'Already migrated', sql: 'SELECT 1' },
        { id: 'tab-7', name: 'Legacy', sql: 'SELECT 2' },
      ],
      activeTabId: 'tab-7',
      tabCounter: 7,
      savedAt: 0,
    } as PersistedTabState);

    expect(migrated.tabs[0].id).toBe(uuid);
    expect(migrated.tabs[1].id).toMatch(UUID_PREFIX);
    expect(migrated.activeTabId).toBe(migrated.tabs[1].id);
  });

  it('handles an activeTabId that matches no tab', () => {
    const migrated = migrateTabIds({
      tabs: [{ id: 'tab-1', name: 'Query 1', sql: 'SELECT 1' }],
      activeTabId: 'tab-99',
      tabCounter: 1,
      savedAt: 0,
    } as PersistedTabState);

    expect(migrated.activeTabId).toBe(migrated.tabs[0].id);
  });
});

describe('hiding versus deleting', () => {
  it('hides a tab instead of removing it', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[0].id;
    state = reducer(state, hideTab(id));

    expect(state.tabs.find((t) => t.id === id)).toBeDefined();
    expect(state.tabs.find((t) => t.id === id)?.hidden).toBe(true);
  });

  it('moves activeTabId off a tab that is being hidden', () => {
    let state = reducer(undefined, addTab(undefined));
    const active = state.activeTabId;
    state = reducer(state, hideTab(active));

    expect(state.activeTabId).not.toBe(active);
    expect(state.tabs.find((t) => t.id === state.activeTabId)?.hidden).toBeFalsy();
  });

  /** The editor must never be left with nothing selected. */
  it('opens a fresh tab when the last visible one is hidden', () => {
    let state = reducer(undefined, addTab(undefined));
    for (const t of [...state.tabs]) {
      state = reducer(state, hideTab(t.id));
    }

    const visible = state.tabs.filter((t) => !t.hidden);
    expect(visible).toHaveLength(1);
    expect(state.activeTabId).toBe(visible[0].id);
  });

  /** Hiding is harmless, so a lock does not block it. Deleting is another matter. */
  it('allows a locked tab to be hidden', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[state.tabs.length - 1].id;
    state = reducer(state, lockTab({ tabId: id, reason: 'test', lockType: 'manual' }));
    state = reducer(state, hideTab(id));

    expect(state.tabs.find((t) => t.id === id)?.hidden).toBe(true);
  });

  it('deleteTab removes the record outright', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[state.tabs.length - 1].id;
    state = reducer(state, deleteTab(id));

    expect(state.tabs.find((t) => t.id === id)).toBeUndefined();
  });

  it('refuses to delete a locked tab', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[state.tabs.length - 1].id;
    state = reducer(state, lockTab({ tabId: id, reason: 'test', lockType: 'manual' }));
    state = reducer(state, deleteTab(id));

    expect(state.tabs.find((t) => t.id === id)).toBeDefined();
  });

  it('deletes a hidden tab without disturbing the visible ones', () => {
    let state = reducer(undefined, addTab(undefined));
    state = reducer(state, addTab(undefined));
    const [first, second] = state.tabs.map((t) => t.id);
    state = reducer(state, hideTab(first));
    state = reducer(state, deleteTab(first));

    expect(state.tabs.find((t) => t.id === first)).toBeUndefined();
    expect(state.tabs.find((t) => t.id === second)).toBeDefined();
  });

  it('opens a fresh tab when the last tab is deleted', () => {
    let state = reducer(undefined, addTab(undefined));
    for (const t of [...state.tabs]) {
      state = reducer(state, deleteTab(t.id));
    }

    expect(state.tabs.filter((t) => !t.hidden)).toHaveLength(1);
  });

  /** Unhiding is how the project tree reopens a closed tab. */
  it('reopens a hidden tab by hiding it false', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[0].id;
    state = reducer(state, hideTab(id));
    state = reducer(state, { type: 'query/showTab', payload: id });

    expect(state.tabs.find((t) => t.id === id)?.hidden).toBe(false);
    expect(state.activeTabId).toBe(id);
  });

  it('keeps a hidden tab\'s SQL intact', () => {
    let state = reducer(undefined, addTab(undefined));
    const id = state.tabs[0].id;
    state = reducer(state, setSql({ tabId: id, sql: 'SELECT 42' }));
    state = reducer(state, hideTab(id));

    expect(state.tabs.find((t) => t.id === id)?.sql).toBe('SELECT 42');
  });
});
