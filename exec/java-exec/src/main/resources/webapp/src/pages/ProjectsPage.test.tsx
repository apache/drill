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
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { AppChromeProvider } from '../contexts/AppChromeContext';
import ProjectsPage from './ProjectsPage';
import type { Project } from '../types';

const project = {
  id: 'p1', name: 'Alpha Project', tags: [], owner: 'me', isPublic: false, sharedWith: [],
  datasets: [], savedQueryIds: [], visualizationIds: [], dashboardIds: [], wikiPages: [],
  createdAt: 1, updatedAt: 1,
} as Project;

vi.mock('../api/projects', () => ({
  getProjects: vi.fn(async () => [project]),
  getFavorites: vi.fn(async () => []),
  createProject: vi.fn(),
  deleteProject: vi.fn(),
  restoreProject: vi.fn(),
  updateProject: vi.fn(),
  toggleFavorite: vi.fn(),
}));

function Location() {
  return <div data-testid="location">{useLocation().pathname}</div>;
}

describe('ProjectsPage', () => {
  it('opens a project on its Conversations tab', async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <AppChromeProvider>
          <MemoryRouter
            initialEntries={['/projects']}
            future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
          >
            <ProjectsPage />
            <Location />
          </MemoryRouter>
        </AppChromeProvider>
      </QueryClientProvider>,
    );

    fireEvent.click(await screen.findByText('Alpha Project'));
    expect(screen.getByTestId('location')).toHaveTextContent('/projects/p1/chat');
  });
});
