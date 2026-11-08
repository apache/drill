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
import { Modal, Dropdown } from 'antd';
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
  const tree = (p: typeof props) => (
    <div>
      <div data-testid="chat">
        <button>chat control</button>
      </div>
      <CanvasHost {...p} />
    </div>
  );
  const view = render(tree(props));
  return {
    ...props,
    rerender: (next: Partial<typeof props>) => view.rerender(tree({ ...props, ...next })),
    unmount: () => view.unmount(),
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

  describe('tab state', () => {
    const activeTab = () => screen.getByTestId('canvas-body').dataset.activeTab;

    it('does not carry one kind\'s tab into another kind', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      expect(activeTab()).toBe('sql');

      h.rerender({ target: { kind: 'chart', id: 'viz-9' } });
      expect(activeTab()).toBe('view');
    });

    it('remembers the tab chosen for a kind when its target returns', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      h.rerender({ target: { kind: 'chart', id: 'viz-9' } });
      h.rerender({ target: results('SELECT 2') });
      expect(activeTab()).toBe('sql');
    });
  });

  describe('full screen', () => {
    const escape = (target: Element | Document = document) =>
      fireEvent.keyDown(target, { key: 'Escape', code: 'Escape', keyCode: 27 });

    it('is a modal dialog only while full screen', () => {
      const h = host({ target: results('SELECT 1') });
      expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
      h.rerender({ isFullScreen: true });
      expect(screen.getByRole('dialog', { name: 'Canvas' })).toHaveAttribute('aria-modal', 'true');
    });

    it('exits on Escape', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      escape();
      expect(h.onFullScreen).toHaveBeenCalledTimes(1);
    });

    it('ignores Escape when not full screen', () => {
      const h = host({ target: results('SELECT 1') });
      escape();
      expect(h.onFullScreen).not.toHaveBeenCalled();
    });

    it('leaves an Escape that something inside already handled', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      const inner = screen.getByTestId('grid');
      inner.addEventListener('keydown', (e) => e.preventDefault());
      escape(inner);
      expect(h.onFullScreen).not.toHaveBeenCalled();
    });

    it('lets an open modal take the Escape instead of leaving full screen', () => {
      const onCancel = vi.fn();
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      render(<Modal open title="Profile" onCancel={onCancel}>modal body</Modal>);
      escape(screen.getByText('modal body'));
      expect(onCancel).toHaveBeenCalledTimes(1);
      expect(h.onFullScreen).not.toHaveBeenCalled();
    });

    it('lets an open dropdown take the Escape instead of leaving full screen', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      render(
        <Dropdown open menu={{ items: [{ key: 'a', label: 'dropdown item' }] }}>
          <button>menu</button>
        </Dropdown>,
      );
      escape(screen.getByText('dropdown item'));
      expect(h.onFullScreen).not.toHaveBeenCalled();
    });

    // jsdom never finishes antd's leave animation, so "gone" here means unmounted.
    it('exits on Escape again once the modal is gone', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      const modal = render(<Modal open title="Profile">modal body</Modal>);
      modal.unmount();
      escape();
      expect(h.onFullScreen).toHaveBeenCalledTimes(1);
    });

    it('moves focus to the exit control on entry and back to the full-screen control on exit', () => {
      const h = host({ target: results('SELECT 1') });
      screen.getByRole('button', { name: 'chat control' }).focus();

      h.rerender({ isFullScreen: true });
      expect(screen.getByRole('button', { name: 'Exit full screen' })).toHaveFocus();

      screen.getByRole('button', { name: 'chat control' }).focus();
      h.rerender({ isFullScreen: false });
      expect(screen.getByRole('button', { name: 'Full screen' })).toHaveFocus();
    });

    it('does not steal focus when it mounts outside full screen', () => {
      host({ target: results('SELECT 1') });
      expect(document.body).toHaveFocus();
    });

    it('makes everything behind the overlay inert, and restores it on exit', () => {
      const h = host({ target: results('SELECT 1') });
      const chat = screen.getByTestId('chat');
      expect(chat).not.toHaveAttribute('inert');

      h.rerender({ isFullScreen: true });
      expect(chat).toHaveAttribute('inert');
      expect(screen.getByRole('dialog', { name: 'Canvas' })).not.toHaveAttribute('inert');

      h.rerender({ isFullScreen: false });
      expect(chat).not.toHaveAttribute('inert');
    });

    it('releases the background if it unmounts while full screen', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      const chat = screen.getByTestId('chat');
      expect(chat).toHaveAttribute('inert');
      h.unmount();
      expect(chat).not.toHaveAttribute('inert');
    });
  });
});
