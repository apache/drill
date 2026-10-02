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
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import SaveQueryDialog from './SaveQueryDialog';
import { createSavedQuery } from '../../api/savedQueries';
import { getViewTargets } from '../../api/metadata';
import { executeQuery } from '../../api/queries';
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
  addDataset: vi.fn(),
}));

// View mode now runs createViewFromQuery, which calls executeQuery. Left unmocked, this
// test would fall through to a real axios call from jsdom instead of exercising the
// guard under test, and would drag in several seconds of network-timeout latency.
vi.mock('../../api/queries', () => ({
  executeQuery: vi.fn(),
}));

const mockCreateSavedQuery = vi.mocked(createSavedQuery);
const mockGetViewTargets = vi.mocked(getViewTargets);
const mockExecuteQuery = vi.mocked(executeQuery);

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
    mockExecuteQuery.mockResolvedValue({} as never);
  });

  it('does not call createSavedQuery when Save is clicked in View mode (Bug 1)', async () => {
    // Uses fireEvent, not userEvent. userEvent.click fires a full pointer sequence
    // (pointerover/down/focus/up/click), each wrapped in act() with microtask flushes;
    // on a Modal + Select that re-renders on every event, that compounded into ~4s and
    // tipped this test past the 5s timeout under the full suite's parallel CPU load.
    // Synchronous fireEvent exercises the same code paths without that overhead.
    renderWithProviders(
      <SaveQueryDialog open={true} onClose={vi.fn()} sql="SELECT * FROM foo" />
    );

    fireEvent.click(screen.getByRole('radio', { name: 'View' }));

    // Fill BOTH required view-mode fields so form.validateFields() actually succeeds and
    // handleSave reaches the isViewMode branch. If the schema were left empty, validation
    // would reject and createSavedQuery would be skipped regardless of the branch, making
    // the assertion below vacuous. findByTitle waits for the option to render, and the
    // final waitFor fails loudly if the selection did not take — so this is self-checking.
    fireEvent.change(await screen.findByPlaceholderText('e.g. sales_summary'),
      { target: { value: 'my_view' } });
    fireEvent.mouseDown(screen.getByRole('combobox'));
    fireEvent.click(await screen.findByTitle('dfs.tmp'));

    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    // executeQuery firing proves the view-creation path ran (query mode would call the
    // saved-query mutation instead), so this assertion is a real branch check.
    await waitFor(() => {
      expect(mockExecuteQuery).toHaveBeenCalledTimes(1);
    });

    expect(mockCreateSavedQuery).not.toHaveBeenCalled();
    // Rendering a full antd Modal + Select and driving it takes ~2.5-3.5s here; on slower
    // shared CI runners that can approach the 5s default. This headroom is for that, not
    // for a hang — the test is deterministic and mutation-verified (breaking the
    // isViewMode branch fails it).
  }, 10000);

  it('resets to Save Query on reopen after a successful save, even if the mode was ' +
    'changed to View while the save was in flight (Bug 2)', async () => {
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

    fireEvent.change(
      screen.getByPlaceholderText('Enter a name for this query'),
      { target: { value: 'my_query' } }
    );
    fireEvent.click(screen.getByRole('button', { name: 'Save' }));

    await waitFor(() => {
      expect(mockCreateSavedQuery).toHaveBeenCalledTimes(1);
    });

    // The save is still pending. Flip to View mode before it resolves.
    fireEvent.click(screen.getByRole('radio', { name: 'View' }));
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
  }, 10000);
});
