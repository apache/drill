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
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createElement, type ReactNode } from 'react';
import { useScratchProject } from './useScratchProject';
import { getScratchProjectId } from '../api/conversations';

vi.mock('../api/conversations', () => ({
  getScratchProjectId: vi.fn(),
}));

let queryClient: QueryClient;
function wrapper({ children }: { children: ReactNode }) {
  return createElement(QueryClientProvider, { client: queryClient }, children);
}

describe('useScratchProject', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  });

  it('returns the scratch project id', async () => {
    vi.mocked(getScratchProjectId).mockResolvedValue('scratch_alice');
    const { result } = renderHook(() => useScratchProject(), { wrapper });
    await waitFor(() => expect(result.current.projectId).toBe('scratch_alice'));
    expect(result.current.isLoading).toBe(false);
  });

  it('caches the lookup across callers', async () => {
    vi.mocked(getScratchProjectId).mockResolvedValue('scratch_alice');
    const first = renderHook(() => useScratchProject(), { wrapper });
    await waitFor(() => expect(first.result.current.projectId).toBe('scratch_alice'));
    const second = renderHook(() => useScratchProject(), { wrapper });
    expect(second.result.current.projectId).toBe('scratch_alice');
    expect(getScratchProjectId).toHaveBeenCalledTimes(1);
  });

  it('reports loading rather than a bad id while the lookup is in flight', async () => {
    let resolve!: (id: string) => void;
    vi.mocked(getScratchProjectId).mockReturnValue(new Promise((r) => { resolve = r; }));
    const { result } = renderHook(() => useScratchProject(), { wrapper });
    expect(result.current.isLoading).toBe(true);
    expect(result.current.projectId).toBeUndefined();
    resolve('scratch_alice');
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.projectId).toBe('scratch_alice');
  });

  it('reports an error with no id when the lookup fails', async () => {
    vi.mocked(getScratchProjectId).mockRejectedValue(new Error('boom'));
    const { result } = renderHook(() => useScratchProject(), { wrapper });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(result.current.projectId).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
  });

  it('does not look up the scratch project when disabled', async () => {
    const { result } = renderHook(() => useScratchProject(false), { wrapper });
    expect(result.current.isLoading).toBe(false);
    expect(getScratchProjectId).not.toHaveBeenCalled();
  });
});
