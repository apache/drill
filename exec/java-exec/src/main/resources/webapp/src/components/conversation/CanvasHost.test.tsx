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
import CanvasHost from './CanvasHost';
import type { ResultsGridProps } from '../results/ResultsGrid';
import type { CanvasTarget } from './CanvasHost';
import type { QueryResult } from '../../types';

// The grid is covered by its own tests and mounted for real in ResultsCanvas.test.tsx.
// Here it stands in so these tests can see what the canvas hands it.
vi.mock('../results/ResultsGrid', () => ({
  default: ({ results, sql }: ResultsGridProps) => (
    <div data-testid="grid" data-sql={sql}>
      {results?.columns.join(',')}|{results?.rows.map((r) => JSON.stringify(r)).join(';')}
    </div>
  ),
}));

function result(over: Partial<QueryResult> = {}): QueryResult {
  return {
    columns: ['region', 'amount'],
    metadata: ['VARCHAR', 'INTEGER'],
    rows: [{ region: 'West', amount: 42 }],
    queryId: 'q1',
    ...over,
  };
}

const results = (sql: string, r = result()): CanvasTarget => ({ kind: 'results', sql, result: r });

function host(over: Partial<React.ComponentProps<typeof CanvasHost>> = {}) {
  const props = {
    target: null as CanvasTarget | null,
    pinned: false,
    onTogglePin: vi.fn(),
    onFullScreen: vi.fn(),
    ...over,
  };
  const view = render(<CanvasHost {...props} />);
  return {
    ...props,
    rerender: (next: Partial<typeof props>) =>
      view.rerender(<CanvasHost {...props} {...next} />),
  };
}

describe('CanvasHost', () => {
  it('renders the results viewer for a results target', () => {
    host({ target: results('SELECT region, amount FROM sales') });
    const grid = screen.getByTestId('grid');
    expect(grid).toHaveTextContent('region,amount|{"region":"West","amount":42}');
    expect(grid).toHaveAttribute('data-sql', 'SELECT region, amount FROM sales');
  });

  it('renders an empty state when target is null', () => {
    host({ target: null });
    expect(screen.getByText(/nothing on the canvas yet/i)).toBeInTheDocument();
    expect(screen.queryByRole('tab')).not.toBeInTheDocument();
  });

  it('shows SQL on the SQL secondary tab', async () => {
    host({ target: results('SELECT region, amount FROM sales') });
    expect(screen.queryByText('SELECT region, amount FROM sales')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
    expect(await screen.findByText('SELECT region, amount FROM sales')).toBeInTheDocument();
  });

  it('shows a newer target when not pinned', async () => {
    const h = host({ target: results('SELECT 1 AS first_query') });
    fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
    expect(await screen.findByText('SELECT 1 AS first_query')).toBeInTheDocument();
    h.rerender({ target: results('SELECT 2 AS second_query') });
    expect(await screen.findByText('SELECT 2 AS second_query')).toBeInTheDocument();
  });

  it('does not change the rendered target while pinned', async () => {
    const h = host({ target: results('SELECT 1 AS first_query') });
    fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
    expect(await screen.findByText('SELECT 1 AS first_query')).toBeInTheDocument();

    h.rerender({ pinned: true });
    h.rerender({ target: results('SELECT 2 AS second_query'), pinned: true });
    expect(screen.getByText('SELECT 1 AS first_query')).toBeInTheDocument();
    expect(screen.queryByText('SELECT 2 AS second_query')).not.toBeInTheDocument();

    // Unpinning releases the canvas to the newest target.
    h.rerender({ target: results('SELECT 2 AS second_query'), pinned: false });
    expect(await screen.findByText('SELECT 2 AS second_query')).toBeInTheDocument();
  });

  it('keeps the chosen secondary tab when a newer results target arrives', async () => {
    const h = host({ target: results('SELECT 1 AS first_query') });
    fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
    h.rerender({ target: results('SELECT 2 AS second_query') });
    expect(await screen.findByText('SELECT 2 AS second_query')).toBeInTheDocument();
  });

  it('reports pin and full-screen clicks to the page', () => {
    const h = host({ target: results('SELECT 1') });
    fireEvent.click(screen.getByRole('button', { name: /pin/i }));
    fireEvent.click(screen.getByRole('button', { name: /full screen/i }));
    expect(h.onTogglePin).toHaveBeenCalledTimes(1);
    expect(h.onFullScreen).toHaveBeenCalledTimes(1);
  });

  it('offers nothing to pin or expand while the canvas is empty', () => {
    host({ target: null });
    expect(screen.getByRole('button', { name: /pin/i })).toBeDisabled();
    expect(screen.getByRole('button', { name: /full screen/i })).toBeDisabled();
  });

  it('labels the pin control by its state', () => {
    host({ target: results('SELECT 1'), pinned: true });
    expect(screen.getByRole('button', { name: /unpin/i })).toBeInTheDocument();
  });

  it('names each stubbed kind until its viewer exists', () => {
    host({ target: { kind: 'chart', id: 'viz-9' } });
    expect(screen.getByText(/chart viewer for viz-9/i)).toBeInTheDocument();
  });
});
