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
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { configureStore } from '@reduxjs/toolkit';
import type { ReactNode } from 'react';
import queryReducer, { setSql, setResults, setError, hideTab, renameTab } from '../store/querySlice';
import uiReducer from '../store/uiSlice';
import { useWorkspacePersistence } from './useWorkspacePersistence';
import { listTabs, createTab, updateTab } from '../api/tabs';
import { nextTabCounter } from '../utils/workspacePersistence';
import { prospectorChatKey } from './useProspector';

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
let queryClient: QueryClient;

function wrapper({ children }: { children: ReactNode }) {
  return (
    <QueryClientProvider client={queryClient}>
      <Provider store={store}>{children}</Provider>
    </QueryClientProvider>
  );
}

describe('useWorkspacePersistence server tier', () => {
  beforeEach(() => {
    // clearAllMocks resets calls but not implementations, so a mockRejectedValue set
    // by one test would leak into the next. Re-establish the defaults explicitly.
    vi.clearAllMocks();
    vi.mocked(listTabs).mockResolvedValue([]);
    vi.mocked(createTab).mockImplementation((t) =>
      Promise.resolve({ ...t, updatedAt: Date.now() } as never));
    vi.mocked(updateTab).mockImplementation((id, t) =>
      Promise.resolve({ ...t, id, updatedAt: Date.now() } as never));
    localStorage.clear();
    store = makeStore();
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
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

  /**
   * The second promotion trigger. Without it, typing SQL and closing without running
   * leaves content in localStorage that the project tree does not list and the user
   * cannot get back to.
   */
  it('promotes an unexecuted tab when it is closed holding SQL', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;

    act(() => {
      store.dispatch(setSql({ tabId: id, sql: 'SELECT 1' }));
      store.dispatch(hideTab(id));
    });

    await waitFor(() =>
      expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id })), SYNC_WAIT);
  });

  it('promotes nothing when an empty tab is closed', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;

    act(() => {
      store.dispatch(hideTab(id));
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));
    expect(createTab).not.toHaveBeenCalledWith(expect.objectContaining({ id }));
  });

  /**
   * The scenario the conversation clause exists for: a tab whose work happened
   * entirely in the assistant panel. Without this, closing it saves nothing and the
   * conversation becomes unreachable.
   */
  it('promotes a tab closed with only a conversation and no SQL', async () => {
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    const id = activeTab().id;
    localStorage.setItem(prospectorChatKey('p1', id)!,
      JSON.stringify([{ role: 'user', content: 'explain this data' }]));

    act(() => {
      store.dispatch(hideTab(id));
    });

    await waitFor(() =>
      expect(createTab).toHaveBeenCalledWith(expect.objectContaining({ id })), SYNC_WAIT);
  });

  /**
   * The sidebar caches its tab list. Without an invalidation on promotion, a project
   * expanded before the first query caches an empty list and the tab never appears.
   */
  it('invalidates the sidebar tab list after promoting a tab', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
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
      expect(invalidate).toHaveBeenCalledWith({ queryKey: ['project-tabs', 'p1'] }),
      SYNC_WAIT);
  });

  /**
   * The sidebar caches tab names, so a rename that does not invalidate leaves the tree
   * showing the old one until the cache happens to expire.
   */
  it('refreshes the sidebar when a promoted tab is renamed', async () => {
    vi.mocked(listTabs).mockResolvedValue([{
      id: 'known', name: 'Old name', sql: 'SELECT 1',
      hidden: false, createdAt: 1, updatedAt: 1,
    }]);
    renderHook(() => useWorkspacePersistence('p1'), { wrapper });
    await waitFor(() =>
      expect(store.getState().query.tabs.some((t) => t.id === 'known')).toBe(true), SYNC_WAIT);

    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    act(() => {
      store.dispatch(renameTab({ tabId: 'known', name: 'New name' }));
    });

    await waitFor(() =>
      expect(invalidate).toHaveBeenCalledWith({ queryKey: ['project-tabs', 'p1'] }),
      SYNC_WAIT);
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
