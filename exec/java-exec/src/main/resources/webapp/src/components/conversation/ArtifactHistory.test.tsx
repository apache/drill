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
import { describe, it, expect } from 'vitest';
import { turnsTouching } from './ArtifactHistory';
import type { ChatMessage } from '../../types/ai';

const call = (id: string, name: string, args: unknown): ChatMessage => (
  { role: 'assistant', content: null, toolCalls: [{ id, name, arguments: JSON.stringify(args) }] }
);
const result = (id: string, body: unknown): ChatMessage => (
  { role: 'tool', content: JSON.stringify(body), toolCallId: id }
);

describe('turnsTouching', () => {
  it('matches a tool result that returns the id', () => {
    const turns = turnsTouching([
      { role: 'user', content: 'make a chart' },
      call('t1', 'create_visualization', { name: 'x' }),
      result('t1', { id: 'viz-1' }),
    ], 'viz-1');
    expect(turns).toEqual([{ key: '0', prompt: 'make a chart', actions: ['Created chart'] }]);
  });

  it('matches an id nested in the arguments', () => {
    const turns = turnsTouching([
      { role: 'user', content: 'dashboard it' },
      call('t1', 'create_dashboard', { panels: [{ visualizationId: 'viz-1' }] }),
      result('t1', { id: 'd9' }),
    ], 'viz-1');
    expect(turns).toHaveLength(1);
    expect(turns[0].actions).toEqual(['Created dashboard']);
  });

  it('does not match an id that merely contains the one asked for', () => {
    expect(turnsTouching([
      { role: 'user', content: 'q' },
      call('t1', 'create_visualization', {}),
      result('t1', { id: 'viz-10' }),
    ], 'viz-1')).toEqual([]);
  });

  it('does not match the id in prose or an unrelated tool result', () => {
    expect(turnsTouching([
      { role: 'user', content: 'tell me about viz-1' },
      { role: 'assistant', content: 'viz-1 is a chart' },
      call('t1', 'list_schemas', {}),
      result('t1', { schemas: ['dfs'] }),
    ], 'viz-1')).toEqual([]);
  });

  it('groups several touching calls in one turn and keeps turns in order', () => {
    const turns = turnsTouching([
      { role: 'user', content: 'first' },
      call('t1', 'create_visualization', {}), result('t1', { id: 'viz-1' }),
      call('t2', 'create_dashboard', { panels: [{ visualizationId: 'viz-1' }] }), result('t2', { id: 'd1' }),
      { role: 'user', content: 'second' },
      call('t3', 'save_query', { id: 'viz-1' }), result('t3', {}),
    ], 'viz-1');
    expect(turns.map((t) => t.prompt)).toEqual(['first', 'second']);
    expect(turns[0].actions).toEqual(['Created chart', 'Created dashboard']);
  });

  it('survives malformed tool arguments and results', () => {
    const messages: ChatMessage[] = [
      { role: 'user', content: 'q' },
      { role: 'assistant', content: null, toolCalls: [{ id: 't1', name: 'x', arguments: '{not json' }] },
      { role: 'tool', content: 'plain text', toolCallId: 't1' },
      { role: 'tool', content: null, toolCallId: 'orphan' },
    ];
    expect(turnsTouching(messages, 'viz-1')).toEqual([]);
  });

  it('names a touching call with no known label after the tool', () => {
    const turns = turnsTouching([
      { role: 'user', content: 'q' },
      call('t1', 'refresh_data_source', { name: 'dfs' }),
    ], 'dfs');
    expect(turns[0].actions).toEqual(['refresh data source']);
  });
});
