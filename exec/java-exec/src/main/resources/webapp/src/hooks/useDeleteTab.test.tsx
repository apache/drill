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
import { renderHook, act } from '@testing-library/react';
import { Provider } from 'react-redux';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { configureStore } from '@reduxjs/toolkit';
import type { ReactNode } from 'react';
import queryReducer, { addTab, setSql } from '../store/querySlice';
import uiReducer from '../store/uiSlice';
import { useDeleteTab } from './useDeleteTab';
import { prospectorChatKey } from './useProspector';
import { deleteTab as deleteServerTab } from '../api/tabs';

vi.mock('../api/tabs', () => ({
  deleteTab: vi.fn(() => Promise.resolve()),
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

describe('useDeleteTab', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(deleteServerTab).mockResolvedValue(undefined);
    localStorage.clear();
    store = makeStore();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  /**
   * The bug this hook exists for: the sidebar used to delete only the server record,
   * so the tab stayed open in the strip and the next sync tick promoted it back.
   */
  it('removes the tab from Redux, not just the server', async () => {
    store.dispatch(addTab(undefined));
    const id = store.getState().query.tabs[store.getState().query.tabs.length - 1].id;
    store.dispatch(setSql({ tabId: id, sql: 'SELECT 1' }));

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current(id); });

    expect(store.getState().query.tabs.find((t) => t.id === id)).toBeUndefined();
    expect(deleteServerTab).toHaveBeenCalledWith(id);
  });

  it('clears the tab conversation', async () => {
    store.dispatch(addTab(undefined));
    const id = store.getState().query.tabs[store.getState().query.tabs.length - 1].id;
    localStorage.setItem(prospectorChatKey('p1', id)!, JSON.stringify([{ role: 'user' }]));

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current(id); });

    expect(localStorage.getItem(prospectorChatKey('p1', id)!)).toBeNull();
  });

  it('refreshes the sidebar list', async () => {
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    store.dispatch(addTab(undefined));
    const id = store.getState().query.tabs[store.getState().query.tabs.length - 1].id;

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current(id); });

    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['project-tabs', 'p1'] });
  });

  /** A server failure must still leave the UI consistent with what the user did. */
  it('still removes the tab locally when the server call fails', async () => {
    vi.mocked(deleteServerTab).mockRejectedValue(new Error('network'));
    store.dispatch(addTab(undefined));
    const id = store.getState().query.tabs[store.getState().query.tabs.length - 1].id;

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current(id); });

    expect(store.getState().query.tabs.find((t) => t.id === id)).toBeUndefined();
  });

  /** Deleting the only tab leaves a fresh empty one, never a blank editor. */
  it('leaves a usable tab behind when the last one is deleted', async () => {
    const id = store.getState().query.activeTabId;

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current(id); });

    const visible = store.getState().query.tabs.filter((t) => !t.hidden);
    expect(visible).toHaveLength(1);
    expect(visible[0].id).not.toBe(id);
    expect(store.getState().query.activeTabId).toBe(visible[0].id);
  });

  /** Redux holds only the open project's tabs, so this must not throw. */
  it('is a no-op in Redux for a tab from another project', async () => {
    const before = store.getState().query.tabs.length;

    const { result } = renderHook(() => useDeleteTab('p1'), { wrapper });
    await act(async () => { await result.current('some-other-projects-tab'); });

    expect(store.getState().query.tabs).toHaveLength(before);
    expect(deleteServerTab).toHaveBeenCalledWith('some-other-projects-tab');
  });
});
