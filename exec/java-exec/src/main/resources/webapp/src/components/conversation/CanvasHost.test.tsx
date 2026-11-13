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
import { Modal, Dropdown, Select, Popover } from 'antd';
import type { ChatMessage } from '../../types/ai';
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

// The artifact viewers fetch from the server and are covered in ArtifactCanvas.test.tsx. The
// tab sets are the real ones; only the body is replaced, with what the host handed it.
vi.mock('./ArtifactCanvas', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./ArtifactCanvas')>()),
  default: (p: { kind: string; id: string; tab: string; messages?: ChatMessage[] }) => (
    <div data-testid="artifact" data-kind={p.kind} data-id={p.id} data-tab={p.tab}>
      {p.messages?.length ?? 0} messages
    </div>
  ),
}));

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

  it('renders the artifact viewer for an artifact target, with the active tab and the thread', () => {
    const messages: ChatMessage[] = [{ role: 'user', content: 'hi' }, { role: 'assistant', content: 'hello' }];
    host({ target: { kind: 'chart', id: 'viz-9' }, messages });
    const viewer = screen.getByTestId('artifact');
    expect(viewer).toHaveAttribute('data-kind', 'chart');
    expect(viewer).toHaveAttribute('data-id', 'viz-9');
    expect(viewer).toHaveAttribute('data-tab', 'chart');
    expect(viewer).toHaveTextContent('2 messages');
    fireEvent.click(screen.getByRole('tab', { name: 'History' }));
    expect(screen.getByTestId('artifact')).toHaveAttribute('data-tab', 'history');
  });

  describe('full screen control', () => {
    it('hands an artifact to its own page instead of toggling the overlay', () => {
      const onOpenArtifact = vi.fn();
      const h = host({ target: { kind: 'dashboard', id: 'd1' }, onOpenArtifact });
      fireEvent.click(screen.getByRole('button', { name: /full screen/i }));
      expect(onOpenArtifact).toHaveBeenCalledWith('dashboard', 'd1');
      expect(h.onFullScreen).not.toHaveBeenCalled();
    });

    it('keeps the overlay for a results target even when an artifact opener is given', () => {
      const onOpenArtifact = vi.fn();
      const h = host({ target: results('SELECT 1'), onOpenArtifact });
      fireEvent.click(screen.getByRole('button', { name: /full screen/i }));
      expect(h.onFullScreen).toHaveBeenCalledTimes(1);
      expect(onOpenArtifact).not.toHaveBeenCalled();
    });

    it('is still the way out of an overlay that an artifact target arrived in', () => {
      const onOpenArtifact = vi.fn();
      const h = host({ target: { kind: 'wiki', id: 'pg1' }, onOpenArtifact, isFullScreen: true });
      fireEvent.click(screen.getByRole('button', { name: 'Exit full screen' }));
      expect(h.onFullScreen).toHaveBeenCalledTimes(1);
      expect(onOpenArtifact).not.toHaveBeenCalled();
    });

    it('opens the pinned artifact, not the newest target', () => {
      const onOpenArtifact = vi.fn();
      const h = host({ target: { kind: 'chart', id: 'pinned-one' }, pinned: true, onOpenArtifact });
      h.rerender({ target: { kind: 'chart', id: 'newer' }, pinned: true });
      fireEvent.click(screen.getByRole('button', { name: /full screen/i }));
      expect(onOpenArtifact).toHaveBeenCalledWith('chart', 'pinned-one');
    });
  });

  describe('tab state', () => {
    const activeTab = () => screen.getByTestId('canvas-body').dataset.activeTab;

    it('does not carry one kind\'s tab into another kind', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      expect(activeTab()).toBe('sql');

      h.rerender({ target: { kind: 'chart', id: 'viz-9' } });
      expect(activeTab()).toBe('chart');
    });

    it('remembers the tab chosen for a kind when its target returns', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      h.rerender({ target: { kind: 'chart', id: 'viz-9' } });
      h.rerender({ target: results('SELECT 2') });
      expect(activeTab()).toBe('sql');
    });
  });

  // Two viewers with different tab sets: results has Results and SQL, a wiki page has Page
  // and History. A choice made on one must neither leak into the other nor be lost.
  describe('viewers with different tab sets', () => {
    const activeTab = () => screen.getByTestId('canvas-body').dataset.activeTab;
    const tabNames = () => screen.getAllByRole('tab').map((t) => t.textContent);
    const wiki: CanvasTarget = { kind: 'wiki', id: 'pg1' };

    it('shows the other viewer its own default and tabs, not a tab it does not have', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      expect(activeTab()).toBe('sql');

      h.rerender({ target: wiki });
      expect(tabNames()).toEqual(['Page', 'History']);
      expect(activeTab()).toBe('page');
      expect(screen.getByTestId('artifact')).toHaveAttribute('data-tab', 'page');
      expect(screen.queryByRole('tab', { name: 'SQL' })).not.toBeInTheDocument();
    });

    it('gives each viewer back the tab it was left on', () => {
      const h = host({ target: results('SELECT 1') });
      fireEvent.click(screen.getByRole('tab', { name: 'SQL' }));
      h.rerender({ target: wiki });
      fireEvent.click(screen.getByRole('tab', { name: 'History' }));
      expect(activeTab()).toBe('history');

      h.rerender({ target: results('SELECT 2') });
      expect(activeTab()).toBe('sql');
      expect(tabNames()).toEqual(['Results', 'SQL']);
      h.rerender({ target: wiki });
      expect(activeTab()).toBe('history');
    });

    it('does not carry a History choice from one artifact kind to another that also has it', () => {
      const h = host({ target: wiki });
      fireEvent.click(screen.getByRole('tab', { name: 'History' }));
      h.rerender({ target: { kind: 'chart', id: 'viz-1' } });
      expect(activeTab()).toBe('chart');
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

    // antd keeps a popup's body-level container after it closes, so a popup opened BEFORE
    // full screen and reopened inside it renders into a container that already exists. The
    // order below is the whole point: a popup first opened after entry proves nothing.
    describe('popups opened before full screen stay usable inside it', () => {
      const popups: Record<string, [string, (open: boolean) => React.ReactElement]> = {
        dropdown: ['.ant-dropdown', (open) => (
          <Dropdown open={open} menu={{ items: [{ key: 'a', label: 'popup content' }] }}>
            <button>trigger</button>
          </Dropdown>
        )],
        modal: ['.ant-modal-root', (open) => (
          <Modal open={open} title="Profile">popup content</Modal>
        )],
        select: ['.ant-select-dropdown', (open) => (
          <Select open={open} options={[{ value: 'a', label: 'popup content' }]} />
        )],
        popover: ['.ant-popover', (open) => (
          <Popover open={open} content="popup content"><button>trigger</button></Popover>
        )],
      };

      it.each(Object.keys(popups))('%s', (name) => {
        const [selector, popup] = popups[name];
        const h = host({ target: results('SELECT 1') });
        const opened = render(popup(true));
        opened.rerender(popup(false));
        const container = document.querySelector(selector)?.closest('body > *');
        expect(container).toBeTruthy();

        h.rerender({ isFullScreen: true });
        expect(container).not.toHaveAttribute('inert');
        expect(screen.getByTestId('chat')).toHaveAttribute('inert');

        opened.rerender(popup(true));
        expect(screen.getByText('popup content').closest('[inert]')).toBeNull();
      });
    });

    it('releases the background if it unmounts while full screen', () => {
      const h = host({ target: results('SELECT 1'), isFullScreen: true });
      const chat = screen.getByTestId('chat');
      expect(chat).toHaveAttribute('inert');
      h.unmount();
      expect(chat).not.toHaveAttribute('inert');
    });
  });
  describe('resizing', () => {
    it('widens as the left edge is dragged left, clamps at the minimum, and remembers the width', () => {
      // jsdom measures every element as 0 wide, so the drag starts from the stored width.
      window.localStorage.setItem('conversations.canvasWidth', '400');
      host({ target: results('SELECT 1') });
      const handle = screen.getByLabelText('Resize canvas');
      const canvas = screen.getByLabelText('Canvas');

      fireEvent.pointerDown(handle, { clientX: 1000 });
      fireEvent.pointerMove(document, { clientX: 800 });
      expect(canvas).toHaveStyle({ width: '600px' });

      // Dragging well past the right edge stops at the floor rather than collapsing.
      fireEvent.pointerMove(document, { clientX: 2000 });
      expect(canvas).toHaveStyle({ width: '280px' });

      fireEvent.pointerMove(document, { clientX: 800 });
      fireEvent.pointerUp(document);
      expect(window.localStorage.getItem('conversations.canvasWidth')).toBe('600');

      // Released: further movement no longer resizes.
      fireEvent.pointerMove(document, { clientX: 400 });
      expect(canvas).toHaveStyle({ width: '600px' });
    });

    it('has no handle while full screen', () => {
      host({ target: results('SELECT 1'), isFullScreen: true });
      expect(screen.queryByLabelText('Resize canvas')).toBeNull();
    });
  });
});
