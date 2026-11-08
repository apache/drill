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
import { Empty, List, Tag } from 'antd';
import type { ChatMessage } from '../../types/ai';

/** One user turn in the conversation that touched an artifact. */
export interface HistoryTurn {
  /** Index of the user message that opened the turn, so it is stable as the thread grows. */
  key: string;
  /** What the user asked. */
  prompt: string;
  /** What Prospector did to the artifact in that turn, one entry per touching tool call. */
  actions: string[];
}

const ACTION_LABELS: Record<string, string> = {
  execute_sql: 'Ran SQL',
  create_visualization: 'Created chart',
  create_dashboard: 'Created dashboard',
  save_query: 'Saved query',
  save_report: 'Saved report',
};

const actionLabel = (tool: string) => ACTION_LABELS[tool] ?? tool.replace(/_/g, ' ');

function parseJson(text: string | null | undefined): unknown {
  if (!text) {
    return undefined;
  }
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

/** Whether any string anywhere in the value is exactly the id. Whole values only, so viz-10 is not viz-1. */
function mentions(value: unknown, id: string): boolean {
  if (typeof value === 'string') {
    return value === id;
  }
  if (Array.isArray(value)) {
    return value.some((v) => mentions(v, id));
  }
  if (value && typeof value === 'object') {
    return Object.values(value).some((v) => mentions(v, id));
  }
  return false;
}

/**
 * The turns of a conversation that touched an artifact. A tool call touches it when the
 * artifact's id is a value in the call's arguments (a dashboard built from a chart) or in the
 * result it returned (the call that created it). The thread is the only record: nothing
 * versions artifacts, so this says what Prospector did and when, not what changed.
 */
export function turnsTouching(messages: ChatMessage[], id: string): HistoryTurn[] {
  const resultOf = new Map<string, string | null>();
  messages.forEach((m) => {
    if (m.role === 'tool' && m.toolCallId) {
      resultOf.set(m.toolCallId, m.content);
    }
  });

  const turns: HistoryTurn[] = [];
  let turnStart = -1;
  let prompt = '';
  messages.forEach((m, index) => {
    if (m.role === 'user') {
      turnStart = index;
      prompt = m.content ?? '';
      return;
    }
    if (m.role !== 'assistant' || !m.toolCalls) {
      return;
    }
    const actions = m.toolCalls
      .filter((tc) => mentions(parseJson(tc.arguments), id) || mentions(parseJson(resultOf.get(tc.id)), id))
      .map((tc) => actionLabel(tc.name));
    if (actions.length === 0) {
      return;
    }
    const last = turns[turns.length - 1];
    if (last && last.key === String(turnStart)) {
      last.actions.push(...actions);
    } else {
      turns.push({ key: String(turnStart), prompt, actions });
    }
  });
  return turns;
}

/** The History tab body: the conversation turns that touched this artifact. */
export default function ArtifactHistory({ messages, id }: { messages: ChatMessage[]; id: string }) {
  const turns = turnsTouching(messages, id);
  if (turns.length === 0) {
    return (
      <Empty
        image={Empty.PRESENTED_IMAGE_SIMPLE}
        description="No turns in this conversation have touched this artifact yet."
        style={{ marginTop: 48 }}
      />
    );
  }
  return (
    <List
      size="small"
      style={{ height: '100%', overflow: 'auto' }}
      dataSource={turns}
      renderItem={(turn) => (
        <List.Item key={turn.key} style={{ display: 'block' }}>
          <div style={{ marginBottom: 4 }}>{turn.prompt || <em>(no prompt)</em>}</div>
          {turn.actions.map((a, i) => <Tag key={i}>{a}</Tag>)}
        </List.Item>
      )}
    />
  );
}
