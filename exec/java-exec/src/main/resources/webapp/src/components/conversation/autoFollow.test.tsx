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
import ArtifactCard from './ArtifactCard';
import { artifactsOfLastTurn, createdArtifact, nextCanvasTarget } from './autoFollow';
import ChatMessageBubble from '../prospector/ChatMessageBubble';
import type { CanvasTarget } from './CanvasHost';
import type { ChatMessage } from '../../types/ai';

const chart = (id: string): CanvasTarget => ({ kind: 'chart', id });
const results: CanvasTarget = {
  kind: 'results',
  sql: 'SELECT 1',
  result: { columns: [], metadata: [], rows: [], queryId: 'q' },
};

describe('nextCanvasTarget', () => {
  it('settles on the last artifact of a turn, not each in sequence', () => {
    const turn = [chart('a'), { kind: 'dashboard', id: 'b' } as CanvasTarget, chart('c')];
    expect(nextCanvasTarget(turn, null)).toBe(turn[2]);
    expect(nextCanvasTarget(turn, chart('old'))).toBe(turn[2]);
  });

  it('returns the current target when a turn produced no artifacts', () => {
    expect(nextCanvasTarget([], results)).toBe(results);
    expect(nextCanvasTarget([], null)).toBeNull();
  });
});

const call = (id: string, name: string, args: unknown = {}): ChatMessage => (
  { role: 'assistant', content: null, toolCalls: [{ id, name, arguments: JSON.stringify(args) }] }
);
const result = (id: string, name: string, body: unknown): ChatMessage => (
  { role: 'tool', content: JSON.stringify(body), toolCallId: id, name }
);

describe('createdArtifact', () => {
  const asCall = (name: string) => ({ id: 't', name, arguments: '{}' });
  const tool = (body: unknown) => result('t', 'x', body);

  it.each([
    ['create_visualization', 'chart', { id: 'v1', name: 'Sales' }, 'Sales'],
    ['create_dashboard', 'dashboard', { id: 'd1', name: 'Exec view' }, 'Exec view'],
    ['save_query', 'query', { id: 'q1', name: 'Active users' }, 'Active users'],
    // A report is a wiki page, and the tool names it `title`.
    ['save_report', 'wiki', { id: 'pg1', title: 'Q3 report' }, 'Q3 report'],
  ])('reads the id and name %s returns', (toolName, kind, body, name) => {
    expect(createdArtifact(asCall(toolName), tool(body))).toEqual({ kind, id: body.id, name });
  });

  it('is nothing for an exploratory query, whatever it returns', () => {
    expect(createdArtifact(asCall('execute_sql'), tool({ id: 'q', name: 'n', columns: [] }))).toBeNull();
  });

  it('is nothing for a call that failed, has no result yet, or returned no id', () => {
    expect(createdArtifact(asCall('save_query'), tool({ error: 'boom' }))).toBeNull();
    expect(createdArtifact(asCall('save_query'), undefined)).toBeNull();
    expect(createdArtifact(asCall('save_query'), tool({ name: 'no id' }))).toBeNull();
    expect(createdArtifact(asCall('save_query'), { role: 'tool', content: 'not json' })).toBeNull();
  });

  it('is nothing when the result carries an id but the call also reports an error', () => {
    expect(createdArtifact(asCall('create_visualization'), tool({ id: 'v1', name: 'x', error: 'late' }))).toBeNull();
  });
});

describe('artifactsOfLastTurn', () => {
  it('lists what the latest turn created in order, and nothing from earlier turns', () => {
    const messages: ChatMessage[] = [
      { role: 'user', content: 'first' },
      call('t1', 'save_query'), result('t1', 'save_query', { id: 'old', name: 'Old' }),
      { role: 'user', content: 'second' },
      call('t2', 'save_query'), result('t2', 'save_query', { id: 'q2', name: 'Two' }),
      call('t3', 'create_dashboard'), result('t3', 'create_dashboard', { id: 'd3', name: 'Three' }),
      { role: 'assistant', content: 'done' },
    ];
    expect(artifactsOfLastTurn(messages).map((a) => a.id)).toEqual(['q2', 'd3']);
  });

  it('is empty when the latest turn created nothing, even if an earlier one did', () => {
    expect(artifactsOfLastTurn([
      { role: 'user', content: 'first' },
      call('t1', 'save_query'), result('t1', 'save_query', { id: 'old', name: 'Old' }),
      { role: 'user', content: 'second' },
      call('t2', 'execute_sql'), result('t2', 'execute_sql', { columns: ['n'], rowCount: 1 }),
    ])).toEqual([]);
  });
});

describe('ArtifactCard', () => {
  it('names the kind and the artifact and opens it on click', () => {
    const onOpen = vi.fn();
    render(<ArtifactCard kind="query" id="q1" name="Active users" live={false} onOpen={onOpen} />);
    const card = screen.getByRole('button', { name: /saved query.*active users/i });
    fireEvent.click(card);
    expect(onOpen).toHaveBeenCalledTimes(1);
    expect(card).not.toHaveAttribute('aria-current');
  });

  it('shows that the artifact is on the canvas when live', () => {
    render(<ArtifactCard kind="chart" id="v1" name="Sales" live onOpen={vi.fn()} />);
    expect(screen.getByRole('button', { name: /sales/i })).toHaveAttribute('aria-current', 'true');
    expect(screen.getByText('Live')).toBeInTheDocument();
  });
});

describe('cards in the thread', () => {
  const turn: ChatMessage[] = [
    {
      role: 'assistant',
      content: 'Working on it',
      toolCalls: [
        { id: 'a', name: 'execute_sql', arguments: '{"sql":"SELECT 1"}' },
        { id: 'b', name: 'save_query', arguments: '{}' },
        { id: 'c', name: 'create_visualization', arguments: '{}' },
        { id: 'd', name: 'save_query', arguments: '{}' },
      ],
    },
    result('a', 'execute_sql', { columns: ['n'], rowCount: 1 }),
    result('b', 'save_query', { id: 'q1', name: 'Active users' }),
    result('c', 'create_visualization', { id: 'v1', name: 'Sales by day' }),
    result('d', 'save_query', { error: 'name taken' }),
  ];

  const renderBubble = (extra: Partial<React.ComponentProps<typeof ChatMessageBubble>> = {}) => render(
    <MemoryRouter>
      <ChatMessageBubble message={turn[0]} toolResults={turn.slice(1)} onOpenArtifact={vi.fn()} {...extra} />
    </MemoryRouter>,
  );

  it('drops a card for each durable artifact and none for a bare execute_sql or a failed save', () => {
    renderBubble();
    expect(screen.getAllByRole('button', { name: /^(chart|saved query)/i }).map((b) => b.textContent))
      .toEqual([expect.stringContaining('Active users'), expect.stringContaining('Sales by day')]);
  });

  it('drops no card for a turn that only ran SQL', () => {
    const sqlOnly = [turn[0], turn[1]];
    render(
      <MemoryRouter>
        <ChatMessageBubble
          message={{ ...sqlOnly[0], toolCalls: [turn[0].toolCalls![0]] }}
          toolResults={[sqlOnly[1]]}
          onOpenArtifact={vi.fn()}
        />
      </MemoryRouter>,
    );
    expect(screen.queryByRole('button', { name: /^(chart|saved query|dashboard|workflow|wiki page|data source)/i }))
      .not.toBeInTheDocument();
  });

  it('opens the artifact the card names', () => {
    const onOpenArtifact = vi.fn();
    renderBubble({ onOpenArtifact });
    fireEvent.click(screen.getByRole('button', { name: /sales by day/i }));
    expect(onOpenArtifact).toHaveBeenCalledWith('chart', 'v1');
  });

  it('marks the card whose artifact is on the canvas as live, and only that one', () => {
    renderBubble({ liveArtifact: { kind: 'chart', id: 'v1' } });
    expect(screen.getByRole('button', { name: /sales by day/i })).toHaveAttribute('aria-current', 'true');
    expect(screen.getByRole('button', { name: /active users/i })).not.toHaveAttribute('aria-current');
  });

  it('does not treat a different kind with the same id as live', () => {
    renderBubble({ liveArtifact: { kind: 'dashboard', id: 'v1' } });
    expect(screen.getByRole('button', { name: /sales by day/i })).not.toHaveAttribute('aria-current');
  });

  it('shows no cards where the host cannot open artifacts', () => {
    renderBubble({ onOpenArtifact: undefined });
    expect(screen.queryByRole('button', { name: /sales by day/i })).not.toBeInTheDocument();
  });
});
