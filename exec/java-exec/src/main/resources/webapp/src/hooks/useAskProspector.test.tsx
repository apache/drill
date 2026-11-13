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
import { renderHook, waitFor, act } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement, type ReactNode } from 'react';
import { useAskProspector } from './useAskProspector';
import { getScratchProjectId } from '../api/conversations';

vi.mock('../api/conversations', () => ({ getScratchProjectId: vi.fn() }));

function wrapper({ children }: { children: ReactNode }) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return createElement(QueryClientProvider, { client }, createElement(MemoryRouter, { initialEntries: ['/query'] }, children));
}

function useBoth(projectId?: string) {
  return { ask: useAskProspector(projectId), location: useLocation() };
}

describe('useAskProspector', () => {
  beforeEach(() => vi.clearAllMocks());

  it('has no ask action while the scratch lookup is in flight', () => {
    vi.mocked(getScratchProjectId).mockReturnValue(new Promise(() => {}));
    const { result } = renderHook(() => useBoth(), { wrapper });
    expect(result.current.ask.ask).toBeUndefined();
    expect(result.current.ask.isLoading).toBe(true);
  });

  it('navigates to the chat with the seed in router state', async () => {
    vi.mocked(getScratchProjectId).mockResolvedValue('scratch_alice');
    const { result } = renderHook(() => useBoth(), { wrapper });
    await waitFor(() => expect(result.current.ask.ask).toBeDefined());
    act(() => result.current.ask.ask!({ prompt: 'Fix it', context: { currentSql: 'select 1' } }));
    expect(result.current.location.pathname).toBe('/projects/scratch_alice/chat');
    expect(result.current.location.state).toEqual({ seed: { prompt: 'Fix it', context: { currentSql: 'select 1' } } });
  });

  it('navigates with no state when there is no seed', async () => {
    vi.mocked(getScratchProjectId).mockResolvedValue('scratch_alice');
    const { result } = renderHook(() => useBoth(), { wrapper });
    await waitFor(() => expect(result.current.ask.ask).toBeDefined());
    act(() => result.current.ask.ask!());
    expect(result.current.location.state).toBeNull();
  });

  it('uses the given project and never looks up Scratch', () => {
    const { result } = renderHook(() => useBoth('proj_1'), { wrapper });
    act(() => result.current.ask.ask!({ prompt: 'p' }));
    expect(result.current.location.pathname).toBe('/projects/proj_1/chat');
    expect(getScratchProjectId).not.toHaveBeenCalled();
  });
});
