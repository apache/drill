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
import type { ReactNode } from 'react';
import { ProjectContextProvider, useProjectContext } from './ProjectContext';

vi.mock('../api/projects', () => ({
  getProject: vi.fn(),
}));

import { getProject } from '../api/projects';

const mockedGetProject = vi.mocked(getProject);

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });
}

function createWrapper(projectId: string) {
  const queryClient = createQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <ProjectContextProvider projectId={projectId}>
          {children}
        </ProjectContextProvider>
      </QueryClientProvider>
    );
  };
}

describe('useProjectContext', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('throws when used outside provider', () => {
    const queryClient = createQueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    expect(() => {
      renderHook(() => useProjectContext(), { wrapper });
    }).toThrow('useProjectContext must be used within a ProjectContextProvider');
  });

  it('provides projectId from props', () => {
    mockedGetProject.mockResolvedValue({
      id: 'proj-1',
      name: 'Test Project',
      tags: [],
      owner: 'admin',
      isPublic: true,
      sharedWith: [],
      datasets: [],
      savedQueryIds: [],
      visualizationIds: [],
      dashboardIds: [],
      wikiPages: [],
      createdAt: 0,
      updatedAt: 0,
    });

    const { result } = renderHook(() => useProjectContext(), {
      wrapper: createWrapper('proj-1'),
    });

    expect(result.current.projectId).toBe('proj-1');
  });

  it('computes savedQueryIdSet correctly', async () => {
    mockedGetProject.mockResolvedValue({
      id: 'proj-2',
      name: 'Test',
      tags: [],
      owner: 'admin',
      isPublic: true,
      sharedWith: [],
      datasets: [],
      savedQueryIds: ['sq-1', 'sq-2', 'sq-3'],
      visualizationIds: [],
      dashboardIds: [],
      wikiPages: [],
      createdAt: 0,
      updatedAt: 0,
    });

    const { result } = renderHook(() => useProjectContext(), {
      wrapper: createWrapper('proj-2'),
    });

    await waitFor(() => {
      expect(result.current.project).toBeDefined();
    });

    expect(result.current.savedQueryIdSet).toBeInstanceOf(Set);
    expect(result.current.savedQueryIdSet.has('sq-1')).toBe(true);
    expect(result.current.savedQueryIdSet.has('sq-2')).toBe(true);
    expect(result.current.savedQueryIdSet.has('sq-3')).toBe(true);
    expect(result.current.savedQueryIdSet.has('sq-999')).toBe(false);
  });

  it('computes visualizationIdSet correctly', async () => {
    mockedGetProject.mockResolvedValue({
      id: 'proj-3',
      name: 'Test',
      tags: [],
      owner: 'admin',
      isPublic: true,
      sharedWith: [],
      datasets: [],
      savedQueryIds: [],
      visualizationIds: ['viz-a', 'viz-b'],
      dashboardIds: [],
      wikiPages: [],
      createdAt: 0,
      updatedAt: 0,
    });

    const { result } = renderHook(() => useProjectContext(), {
      wrapper: createWrapper('proj-3'),
    });

    await waitFor(() => {
      expect(result.current.project).toBeDefined();
    });

    expect(result.current.visualizationIdSet).toBeInstanceOf(Set);
    expect(result.current.visualizationIdSet.has('viz-a')).toBe(true);
    expect(result.current.visualizationIdSet.has('viz-b')).toBe(true);
    expect(result.current.visualizationIdSet.has('viz-missing')).toBe(false);
  });

  it('computes dashboardIdSet correctly', async () => {
    mockedGetProject.mockResolvedValue({
      id: 'proj-4',
      name: 'Test',
      tags: [],
      owner: 'admin',
      isPublic: true,
      sharedWith: [],
      datasets: [],
      savedQueryIds: [],
      visualizationIds: [],
      dashboardIds: ['dash-1'],
      wikiPages: [],
      createdAt: 0,
      updatedAt: 0,
    });

    const { result } = renderHook(() => useProjectContext(), {
      wrapper: createWrapper('proj-4'),
    });

    await waitFor(() => {
      expect(result.current.project).toBeDefined();
    });

    expect(result.current.dashboardIdSet).toBeInstanceOf(Set);
    expect(result.current.dashboardIdSet.has('dash-1')).toBe(true);
    expect(result.current.dashboardIdSet.size).toBe(1);
  });
});
