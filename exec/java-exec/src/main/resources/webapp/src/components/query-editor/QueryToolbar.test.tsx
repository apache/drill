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
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import QueryToolbar from './QueryToolbar';

function renderToolbar(props: Partial<React.ComponentProps<typeof QueryToolbar>> = {}) {
  const onExecute = vi.fn();
  render(
    <QueryToolbar
      onExecute={onExecute}
      onCancel={vi.fn()}
      isExecuting={false}
      {...props}
    />
  );
  return { onExecute };
}

describe('QueryToolbar Run button', () => {
  it('is disabled when the editor is empty', () => {
    renderToolbar({ hasSql: false });
    expect(screen.getByRole('button', { name: /run/i })).toBeDisabled();
  });

  it('is enabled once the editor has SQL', () => {
    renderToolbar({ hasSql: true });
    expect(screen.getByRole('button', { name: /run/i })).toBeEnabled();
  });

  it('does not execute when clicked while empty', () => {
    const { onExecute } = renderToolbar({ hasSql: false });
    fireEvent.click(screen.getByRole('button', { name: /run/i }));
    expect(onExecute).not.toHaveBeenCalled();
  });

  it('executes when clicked with SQL present', () => {
    const { onExecute } = renderToolbar({ hasSql: true });
    fireEvent.click(screen.getByRole('button', { name: /run/i }));
    expect(onExecute).toHaveBeenCalledTimes(1);
  });

  /**
   * While a query is running the button becomes Cancel, which must stay clickable
   * regardless of what the editor holds.
   */
  it('shows an enabled Cancel button while executing, even with an empty editor', () => {
    const onCancel = vi.fn();
    render(
      <QueryToolbar onExecute={vi.fn()} onCancel={onCancel} isExecuting hasSql={false} />
    );
    const cancel = screen.getByRole('button', { name: /cancel/i });
    expect(cancel).toBeEnabled();
    fireEvent.click(cancel);
    expect(onCancel).toHaveBeenCalledTimes(1);
  });
});

describe('QueryToolbar Ask Prospector', () => {
  function renderWithRouter(props: Partial<React.ComponentProps<typeof QueryToolbar>>) {
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter>
          <QueryToolbar onExecute={vi.fn()} onCancel={vi.fn()} isExecuting={false} {...props} />
        </MemoryRouter>
      </QueryClientProvider>
    );
  }

  it('offers Ask Prospector when AI is available', () => {
    renderWithRouter({ prospectorAvailable: true, projectId: 'proj_1' });
    expect(screen.getByRole('button', { name: /ask prospector/i })).toBeEnabled();
  });

  it('hides Ask Prospector when AI is not available', () => {
    renderWithRouter({ prospectorAvailable: false, projectId: 'proj_1' });
    expect(screen.queryByRole('button', { name: /ask prospector/i })).not.toBeInTheDocument();
  });

  async function openMore() {
    fireEvent.click(screen.getByRole('button', { name: /more/i }));
    return screen;
  }

  it('Fix Error with Prospector hands the click to the page', async () => {
    const onFixError = vi.fn();
    renderWithRouter({ prospectorAvailable: true, hasSql: true, hasError: true, onFixError, onExplainQuery: vi.fn() });
    await openMore();
    fireEvent.click(await screen.findByText('Fix Error with Prospector'));
    expect(onFixError).toHaveBeenCalledTimes(1);
  });

  it('offers Fix Error only when there is an error', async () => {
    renderWithRouter({ prospectorAvailable: true, hasSql: true, hasError: false, onFixError: vi.fn() });
    await openMore();
    await screen.findByText('Explain with Prospector');
    expect(screen.queryByText('Fix Error with Prospector')).not.toBeInTheDocument();
  });

  it('disables Explain and Fix while there is no target to send them to', async () => {
    renderWithRouter({ prospectorAvailable: true, hasSql: true, hasError: true });
    await openMore();
    expect((await screen.findByText('Explain with Prospector')).closest('li')).toHaveClass('ant-dropdown-menu-item-disabled');
    expect(screen.getByText('Fix Error with Prospector').closest('li')).toHaveClass('ant-dropdown-menu-item-disabled');
  });
});
