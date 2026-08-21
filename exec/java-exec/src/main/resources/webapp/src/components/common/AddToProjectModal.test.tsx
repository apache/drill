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
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import AddToProjectModal from './AddToProjectModal';

vi.mock('../../api/projects', () => ({
  getProjects: vi.fn(() => Promise.resolve([])),
  addSavedQuery: vi.fn(),
  addVisualization: vi.fn(),
  addDashboard: vi.fn(),
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
      {ui}
    </QueryClientProvider>
  );
}

describe('AddToProjectModal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('renders when open', async () => {
    renderWithProviders(
      <AddToProjectModal
        open={true}
        onClose={vi.fn()}
        itemId="item-1"
        itemType="savedQuery"
      />
    );

    await waitFor(() => {
      expect(screen.getByText('Add to Project')).toBeInTheDocument();
    });
    expect(screen.getByPlaceholderText('Search projects...')).toBeInTheDocument();
  });

  it('does not render when closed', () => {
    renderWithProviders(
      <AddToProjectModal
        open={false}
        onClose={vi.fn()}
        itemId="item-1"
        itemType="savedQuery"
      />
    );

    expect(screen.queryByText('Add to Project')).not.toBeInTheDocument();
  });
});
