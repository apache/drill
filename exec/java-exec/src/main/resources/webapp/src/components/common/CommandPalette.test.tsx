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
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import CommandPalette from './CommandPalette';

vi.mock('../../api/projects', () => ({
  getProjects: vi.fn(() => Promise.resolve([])),
}));

vi.mock('../../api/savedQueries', () => ({
  getSavedQueries: vi.fn(() => Promise.resolve([])),
}));

vi.mock('../../api/visualizations', () => ({
  getVisualizations: vi.fn(() => Promise.resolve([])),
}));

vi.mock('../../api/dashboards', () => ({
  getDashboards: vi.fn(() => Promise.resolve([])),
}));

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });
}

function renderWithProviders(ui: ReactNode) {
  const queryClient = createQueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        {ui}
      </MemoryRouter>
    </QueryClientProvider>
  );
}

describe('CommandPalette', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders without crashing', () => {
    const { container } = renderWithProviders(<CommandPalette />);
    expect(container).toBeDefined();
  });

  it('opens on Ctrl+K keydown', async () => {
    renderWithProviders(<CommandPalette />);

    fireEvent.keyDown(window, { key: 'k', ctrlKey: true });

    await waitFor(() => {
      expect(
        screen.getByPlaceholderText(/Search projects, queries, visualizations, dashboards/i)
      ).toBeInTheDocument();
    });
  });

  it('opens modal and shows search input on Ctrl+K', async () => {
    renderWithProviders(<CommandPalette />);

    // Open the palette
    fireEvent.keyDown(window, { key: 'k', ctrlKey: true });

    await waitFor(() => {
      expect(
        screen.getByPlaceholderText(/Search projects, queries, visualizations, dashboards/i)
      ).toBeInTheDocument();
    });
  });
});
