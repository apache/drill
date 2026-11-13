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
import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ProjectNavBar from './ProjectNavBar';
import { getProjects } from '../../api/projects';

vi.mock('../../contexts/ProjectContext', () => ({
  useProjectContext: () => ({ project: { id: 'p1', name: 'Project One' }, projectId: 'p1' }),
}));
vi.mock('../../api/projects', () => ({ getProjects: vi.fn(async () => []) }));
vi.mock('../../hooks/useRecentItems', () => ({ useRecentItems: () => ({ recentItems: [] }) }));

async function renderAt(path: string) {
  const client = new QueryClient();
  const view = render(
    <QueryClientProvider client={client}>
      <MemoryRouter
        initialEntries={[path]}
        future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
      >
        <ProjectNavBar />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  // Let the project-switcher query settle so its state update lands inside the test.
  await waitFor(() => expect(client.isFetching()).toBe(0));
  expect(getProjects).toHaveBeenCalled();
  return view;
}

describe('ProjectNavBar', () => {
  it('lists the tabs with Conversations first and Workflows before Data Sources', async () => {
    await renderAt('/projects/p1/chat');
    const labels = screen.getAllByRole('menuitem').map((el) => el.textContent);
    expect(labels).toEqual([
      'Conversations', 'Query', 'Queries', 'Visualizations', 'Dashboards',
      'Workflows', 'Data Sources', 'Wiki',
    ]);
  });

  it('marks Conversations as the active tab on a conversation URL', async () => {
    await renderAt('/projects/p1/chat/c1');
    const selected = screen.getAllByRole('menuitem').filter((el) =>
      el.classList.contains('ant-menu-item-selected'));
    expect(selected.map((el) => el.textContent)).toEqual(['Conversations']);
  });
});
