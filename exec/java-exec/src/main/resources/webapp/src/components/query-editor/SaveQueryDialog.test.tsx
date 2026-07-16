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
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import SaveQueryDialog from './SaveQueryDialog';
import { createSavedQuery } from '../../api/savedQueries';
import { getViewTargets } from '../../api/metadata';
import type { SavedQuery } from '../../types';

vi.mock('../../api/savedQueries', () => ({
  createSavedQuery: vi.fn(),
}));

vi.mock('../../api/metadata', () => ({
  getViewTargets: vi.fn(() =>
    Promise.resolve([{ name: 'dfs.tmp', type: 'schema', plugin: 'dfs', browsable: true }])
  ),
}));

vi.mock('../../api/projects', () => ({
  addSavedQuery: vi.fn(),
}));

const mockCreateSavedQuery = vi.mocked(createSavedQuery);
const mockGetViewTargets = vi.mocked(getViewTargets);

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

describe('SaveQueryDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockGetViewTargets.mockResolvedValue([
      { name: 'dfs.tmp', type: 'schema', plugin: 'dfs', browsable: true },
    ]);
  });

  it('does not call createSavedQuery when Save is clicked in View mode (Bug 1)', async () => {
    const user = userEvent.setup();
    renderWithProviders(
      <SaveQueryDialog open={true} onClose={vi.fn()} sql="SELECT * FROM foo" />
    );

    await user.click(screen.getByRole('radio', { name: 'View' }));
    await waitFor(() => {
      expect(screen.getByText('Save as View')).toBeInTheDocument();
    });

    // Fill BOTH required view-mode fields so form.validateFields() actually
    // succeeds and handleSave reaches the isViewMode guard, rather than
    // bailing out earlier in the catch block for unrelated reasons.
    await user.type(screen.getByPlaceholderText('e.g. sales_summary'), 'my_view');

    const schemaSelect = screen.getByRole('combobox');
    await user.click(schemaSelect);
    const option = await screen.findByTitle('dfs.tmp');
    await user.click(option);

    await waitFor(() => {
      expect(screen.getByText('dfs.tmp', { selector: '.ant-select-selection-item' }))
        .toBeInTheDocument();
    });

    await user.click(screen.getByRole('button', { name: 'Save' }));

    // Let any pending validation/microtasks resolve before asserting, so a
    // guard removal that lets execution fall through to the (async)
    // mutation actually has a chance to register the call.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(mockCreateSavedQuery).not.toHaveBeenCalled();
  });

  it('resets to Save Query on reopen after a successful save, even if the mode was ' +
    'changed to View while the save was in flight (Bug 2)', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();

    // Resolve createSavedQuery only when we choose to — this lets us flip the mode
    // radio to "View" *after* Save was clicked in Query mode but *before* onSuccess
    // runs, reproducing the race that leaves the permanently-mounted dialog stuck in
    // View mode the next time it opens.
    let resolveSave!: (value: SavedQuery) => void;
    mockCreateSavedQuery.mockReturnValue(
      new Promise<SavedQuery>((resolve) => {
        resolveSave = resolve;
      })
    );

    const queryClient = createQueryClient();
    const { rerender } = render(
      <QueryClientProvider client={queryClient}>
        <SaveQueryDialog open={true} onClose={onClose} sql="SELECT * FROM foo" />
      </QueryClientProvider>
    );

    expect(screen.getByText('Save Query')).toBeInTheDocument();

    await user.type(
      screen.getByPlaceholderText('Enter a name for this query'),
      'my_query'
    );
    await user.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(mockCreateSavedQuery).toHaveBeenCalledTimes(1);
    });

    // The save is still pending. Flip to View mode before it resolves.
    await user.click(screen.getByRole('radio', { name: 'View' }));
    await waitFor(() => {
      expect(screen.getByText('Save as View')).toBeInTheDocument();
    });

    resolveSave({
      id: 'q1',
      name: 'my_query',
      sql: 'SELECT * FROM foo',
      owner: 'user',
      createdAt: '',
      updatedAt: '',
      isPublic: false,
    });

    await waitFor(() => {
      expect(onClose).toHaveBeenCalled();
    });

    // The dialog is permanently mounted — the parent only ever toggles `open`.
    rerender(
      <QueryClientProvider client={queryClient}>
        <SaveQueryDialog open={false} onClose={onClose} sql="SELECT * FROM foo" />
      </QueryClientProvider>
    );
    rerender(
      <QueryClientProvider client={queryClient}>
        <SaveQueryDialog open={true} onClose={onClose} sql="SELECT * FROM foo" />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(screen.getByText('Save Query')).toBeInTheDocument();
    });
    expect(screen.queryByText('Save as View')).not.toBeInTheDocument();
  });
});
