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
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';

// The server sync debounce is 1500ms, longer than waitFor's 1000ms default.
const SYNC_WAIT = { timeout: 4000 };
import { Provider } from 'react-redux';
import { configureStore } from '@reduxjs/toolkit';
import type { ReactNode } from 'react';
import queryReducer, { setSql, setResults, setError } from '../store/querySlice';
import uiReducer from '../store/uiSlice';
import { useWorkspacePersistence } from './useWorkspacePersistence';
import { listTabs, createTab, updateTab } from '../api/tabs';
import { nextTabCounter } from '../utils/workspacePersistence';

vi.mock('../api/tabs', () => ({
  listTabs: vi.fn(() => Promise.resolve([])),
  createTab: vi.fn((t) => Promise.resolve({ ...t, updatedAt: Date.now() })),
  updateTab: vi.fn((id, t) => Promise.resolve({ ...t, id, updatedAt: Date.now() })),
  deleteTab: vi.fn(() => Promise.resolve()),
}));

vi.mock('../api/resultCache', () => ({
  getCacheRows: vi.fn(() => Promise.resolve(null)),
  getCacheMetadata: vi.fn(() => Promise.resolve(null)),
}));

function makeStore() {
  return configureStore({
    reducer: { query: queryReducer, ui: uiReducer },
    middleware: (getDefault) => getDefault({ serializableCheck: false }),
  });
}

let store: ReturnType<typeof makeStore>;

function wrapper({ children }: { children: ReactNode }) {
  return <Provider store={store}>{children}</Provider>;
}

describe('useWorkspacePersistence server tier', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.clear();
    store = makeStore();
  });

  afterEach(() => {
    localStorage.clear();
  });

  function activeTab() {
    const state = store.getState().query;
    return state.tabs.find((t) => t.id === state.activeTabId)!;
  }

  it('does not promote a tab that has only been typed in', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    await waitFor(() => expect(true).toBe(true));

    act(() => {
      store.dispatch(setSql({ tabId: activeTab().id, sql: 'SELECT 1' }));
    });

    await new Promise((resolve) => setTimeout(resolve, 120));
    expect(createTab).not.toHaveBeenCalled();
  });

  it('promotes a tab once a query has been executed', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;

    act(() => {
      store.dispatch(setSql({ tabId: id, sql: 'SELECT 1' }));
      store.dispatch(setResults({
        tabId: id,
        results: { columns: ['a'], rows: [{ a: 1 }] },
        executionTime: 5,
      }));
    });

    await waitFor(() =>
      expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id })), SYNC_WAIT);
  });

  // A failed query is still work the user will want back in order to fix it.
  it('promotes a tab whose query failed', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;

    act(() => {
      store.dispatch(setSql({ tabId: id, sql: 'SELCT 1' }));
      store.dispatch(setError({ tabId: id, error: { message: 'parse error' } }));
    });

    await waitFor(() =>
      expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id })), SYNC_WAIT);
  });

  /**
   * localStorage is the write-through buffer that makes a failed server write
   * non-destructive. If this regresses, a drillbit blip silently loses SQL.
   */
  it('still writes locally when the server save fails', async () => {
    vi.mocked(createTab).mockRejectedValue(new Error('network'));
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;

    act(() => {
      store.dispatch(setSql({ tabId: id, sql: 'SELECT 1' }));
      store.dispatch(setResults({
        tabId: id,
        results: { columns: ['a'], rows: [{ a: 1 }] },
        executionTime: 5,
      }));
    });

    await waitFor(() => {
      const raw = localStorage.getItem('drill-sqllab-tabs-p1');
      expect(raw).toBeTruthy();
      expect(raw).toContain('SELECT 1');
    }, SYNC_WAIT);
  });

  it('reconciles server tabs into the restored state on mount', async () => {
    vi.mocked(listTabs).mockResolvedValue([{
      id: 'server-only',
      name: 'From server',
      sql: 'SELECT 9',
      hidden: true,
      createdAt: 1,
      updatedAt: 999,
    }]);

    renderHook(() => useWorkspacePersistence('p1'), { wrapper });

    await waitFor(() =>
      expect(store.getState().query.tabs.some((t) => t.id === 'server-only')).toBe(true),
      SYNC_WAIT);
  });

  it('updates rather than re-creates a tab that is already promoted', async () => {
    vi.mocked(listTabs).mockResolvedValue([{
      id: 'known',
      name: 'Known',
      sql: 'SELECT 1',
      hidden: false,
      createdAt: 1,
      updatedAt: 1,
    }]);

    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    await waitFor(() =>
      expect(store.getState().query.tabs.some((t) => t.id === 'known')).toBe(true), SYNC_WAIT);

    act(() => {
      store.dispatch(setSql({ tabId: 'known', sql: 'SELECT 2' }));
      store.dispatch(setResults({
        tabId: 'known',
        results: { columns: ['a'], rows: [] },
        executionTime: 1,
      }));
    });

    await waitFor(() => expect(updateTab).toHaveBeenCalled(), SYNC_WAIT);
    expect(createTab).not.toHaveBeenCalledWith(expect.objectContaining({ id: 'known' }));
  });
});

/**
 * Tab ids became UUIDs, so the counter can no longer be recovered from them. It comes
 * from the tab names instead — without this, the counter resets to 1 on every reload
 * and new tabs collide with existing "Query N" names.
 */
describe('nextTabCounter', () => {
  it('derives the counter from tab names', () => {
    expect(nextTabCounter([
      { id: 'x', name: 'Query 1', sql: '' },
      { id: 'y', name: 'Query 7', sql: '' },
    ])).toBe(7);
  });

  it('ignores renamed tabs', () => {
    expect(nextTabCounter([
      { id: 'x', name: 'Sales report', sql: '' },
      { id: 'y', name: 'Query 3', sql: '' },
    ])).toBe(3);
  });

  it('returns 1 when no tab is numbered', () => {
    expect(nextTabCounter([{ id: 'x', name: 'Sales report', sql: '' }])).toBe(1);
  });

  it('is not fooled by a name that merely contains a number', () => {
    expect(nextTabCounter([{ id: 'x', name: 'Query 3 revised', sql: '' }])).toBe(1);
  });
});
