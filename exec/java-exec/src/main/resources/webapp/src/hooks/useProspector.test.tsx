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
import { useState } from 'react';
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';
import { message } from 'antd';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { useProspector, prospectorChatKey, conversationLengthFor } from './useProspector';
import { createVisualization } from '../api/visualizations';
import { createDashboard } from '../api/dashboards';
import { addVisualization, addDashboard, getProject } from '../api/projects';
import { executeQuery } from '../api/queries';
import { getSchemas } from '../api/metadata';
import { getAiStatus, streamChat } from '../api/ai';
import {
  getConversation as getServerConversation,
  saveConversationMessages,
} from '../api/conversations';
import type { ChatContext, ChatMessage, ConversationRecord, ToolCall } from '../types/ai';

// useProspector reads/writes the React Query cache (save_report invalidation),
// so every render needs a QueryClientProvider in scope.
function Wrapper({ children }: { children: React.ReactNode }) {
  const [client] = useState(() => new QueryClient({
    defaultOptions: { queries: { retry: false } },
  }));
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

vi.mock('../api/visualizations', () => ({ createVisualization: vi.fn() }));
vi.mock('../api/projects', () => ({
  addVisualization: vi.fn(), addDashboard: vi.fn(), getProject: vi.fn(),
}));
vi.mock('../api/ai', () => ({ streamChat: vi.fn(), getAiStatus: vi.fn() }));
vi.mock('../api/queries', () => ({ executeQuery: vi.fn() }));
vi.mock('../api/metadata', () => ({
  getSchemas: vi.fn(), getTables: vi.fn(), getColumns: vi.fn(), getFunctions: vi.fn(),
}));
vi.mock('../api/dashboards', () => ({ createDashboard: vi.fn() }));
vi.mock('../api/savedQueries', () => ({ createSavedQuery: vi.fn() }));
vi.mock('../api/conversations', () => ({
  getConversation: vi.fn(), saveConversationMessages: vi.fn(),
}));

// Every useProspector mount reads the sendDataToAi setting from /status. Tests that
// are not about that gate still need the fetch to resolve; those that are override it.
// vi.clearAllMocks() clears calls but keeps implementations, so this survives the
// per-describe beforeEach hooks.
beforeEach(() => {
  vi.mocked(getAiStatus).mockResolvedValue(
    { enabled: true, configured: true, sendDataToAi: true } as never);
});

const VIZ = { id: 'viz-1', name: 'Sales by Region' };

const call = (): ToolCall => ({
  id: 'call-1',
  name: 'create_visualization',
  arguments: JSON.stringify({
    name: 'Sales by Region',
    chartType: 'bar',
    config: { xAxis: 'region', yAxis: 'total' },
    sql: 'SELECT region, SUM(amount) AS total FROM sales GROUP BY region',
  }),
});

const ctx = (projectId?: string): ChatContext =>
  ({ feature: 'sql_lab_chat', ...(projectId ? { projectId } : {}) }) as ChatContext;

describe('create_visualization tool', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(createVisualization).mockResolvedValue(VIZ as never);
    vi.mocked(addVisualization).mockResolvedValue({} as never);
  });

  it('adds the visualization to the active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx('proj-42')));

    expect(createVisualization).toHaveBeenCalledOnce();
    expect(addVisualization).toHaveBeenCalledWith('proj-42', 'viz-1');
    expect(out.id).toBe('viz-1');
    expect(out.addedToProject).toBe(true);
  });

  it('creates the visualization without a project when there is no active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx()));

    expect(createVisualization).toHaveBeenCalledOnce();
    expect(addVisualization).not.toHaveBeenCalled();
    expect(out.id).toBe('viz-1');
    expect(out.addedToProject).toBe(false);
  });

  /**
   * The visualization itself exists at this point, so failing the whole tool call
   * would misreport it as never created. Report the partial outcome instead, and
   * make sure the failure is not silent.
   */
  it('reports a project-linking failure without losing the created visualization', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(addVisualization).mockRejectedValue(new Error('project not found'));

    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx('proj-42')));

    expect(out.id).toBe('viz-1');
    expect(out.addedToProject).toBe(false);
    expect(out.projectError).toContain('project not found');
    expect(consoleError).toHaveBeenCalled();
    consoleError.mockRestore();
  });

  it('logs tool failures to the console instead of swallowing them', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(createVisualization).mockRejectedValue(new Error('boom'));

    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx('proj-42')));

    expect(out.error).toContain('boom');
    expect(consoleError).toHaveBeenCalled();
    consoleError.mockRestore();
  });
});

describe('create_dashboard tool', () => {
  const dashCall = (): ToolCall => ({
    id: 'call-3',
    name: 'create_dashboard',
    arguments: JSON.stringify({ name: 'Sales Overview', panels: [] }),
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(createDashboard).mockResolvedValue({ id: 'dash-1', name: 'Sales Overview' } as never);
    vi.mocked(addDashboard).mockResolvedValue({} as never);
  });

  it('adds the dashboard to the active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(dashCall(), ctx('proj-42')));

    expect(addDashboard).toHaveBeenCalledWith('proj-42', 'dash-1');
    expect(out.addedToProject).toBe(true);
  });

  it('creates the dashboard without a project when there is no active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(dashCall(), ctx()));

    expect(createDashboard).toHaveBeenCalledOnce();
    expect(addDashboard).not.toHaveBeenCalled();
    expect(out.addedToProject).toBe(false);
  });
});

describe('get_project_docs tool', () => {
  const docsCall = (pageTitle?: string): ToolCall => ({
    id: 'call-2',
    name: 'get_project_docs',
    arguments: JSON.stringify(pageTitle ? { pageTitle } : {}),
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getProject).mockResolvedValue({
      wikiPages: [
        { title: 'Runbook', content: 'step one ...' },
        { title: 'Glossary', content: '...' },
      ],
    } as never);
  });

  it('lists page titles when given no title', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx('proj-42')));
    expect(out.pages.map((p: { title: string }) => p.title)).toEqual(['Runbook', 'Glossary']);
  });

  it('returns the full content of a named page', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Runbook'), ctx('proj-42')));
    expect(out.title).toBe('Runbook');
    expect(out.content).toContain('step one');
  });

  it('reports a missing page rather than failing silently', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Nope'), ctx('proj-42')));
    expect(out.error).toContain('Nope');
  });

  it('requires an active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx()));
    expect(out.error).toBe('No active project — get_project_docs is only available inside a project.');
    expect(out.error).not.toContain('Unknown tool');
  });
});

/**
 * A tool invoked with no arguments (get_available_functions, or get_project_docs
 * listing page titles) streams no argument deltas at all, so the assembled argument
 * string stays blank. Blank is not valid JSON: it throws in executeToolCall, and the
 * backend serializes it to a null tool_use.input, which the Anthropic API rejects
 * with "messages.N.content.N.tool_use.input: Input should be an object". The other
 * tests here hand executeToolCall a ready-made '{}', so only a test that drives the
 * stream the way the server does catches this.
 */
describe('tool calls that take no arguments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getProject).mockResolvedValue({
      wikiPages: [{ title: 'Runbook', content: 'step one ...' }],
    } as never);
    // Round 1 announces a no-argument tool call and sends no deltas; round 2 is the
    // model's reply to the tool result, which must not loop back into another call.
    let round = 0;
    vi.mocked(streamChat).mockImplementation(((_req, onDelta, onDone) => {
      if (round++ === 0) {
        onDelta({ type: 'tool_call_start', id: 'call-9', name: 'get_project_docs' } as never);
        onDelta({ type: 'tool_call_end', id: 'call-9' } as never);
        onDone({ finish_reason: 'tool_calls' } as never);
      } else {
        onDelta({ type: 'content', content: 'There is one page: Runbook.' } as never);
        onDone({ finish_reason: 'stop' } as never);
      }
      return new AbortController();
    }) as never);
  });

  it('assembles {} rather than a blank string when no argument deltas arrive', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    result.current.sendMessage('what docs exist?', ctx('proj-42'));

    await waitFor(() => expect(streamChat).toHaveBeenCalledTimes(2));

    const followUp = vi.mocked(streamChat).mock.calls[1][0];
    const assistant = followUp.messages.find((m) => m.role === 'assistant');
    expect(assistant?.toolCalls?.[0].arguments).toBe('{}');
  });

  it('runs the tool instead of failing to parse blank arguments', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    result.current.sendMessage('what docs exist?', ctx('proj-42'));

    await waitFor(() => expect(streamChat).toHaveBeenCalledTimes(2));

    const followUp = vi.mocked(streamChat).mock.calls[1][0];
    const toolResult = followUp.messages.find((m) => m.role === 'tool');
    expect(toolResult?.content).toContain('Runbook');
    expect(toolResult?.content).not.toContain('JSON');
  });
});

/**
 * The gate is the server's sendDataToAi setting, read from /status by the hook itself.
 * It used to be a ChatContext field each caller had to remember to set, and the callers
 * that forgot (the global Prospector tab, the dashboard panels) sent rows regardless of
 * the setting. Reading it here means no caller can forget.
 */
describe('execute_sql honours the server sendDataToAi setting', () => {
  const sqlCall = (): ToolCall => ({
    id: 'call-3',
    name: 'execute_sql',
    arguments: JSON.stringify({ sql: 'SELECT * FROM sales' }),
  });

  const status = (sendDataToAi: boolean) =>
    ({ enabled: true, configured: true, sendDataToAi });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(executeQuery).mockResolvedValue({
      columns: ['region', 'amount'],
      metadata: ['VARCHAR', 'INTEGER'],
      rows: [{ region: 'West', amount: 42 }],
    } as never);
  });

  /** Waits for the hook's on-mount status fetch to land in its ref. */
  const renderWithStatus = async (sendDataToAi: boolean) => {
    vi.mocked(getAiStatus).mockResolvedValue(status(sendDataToAi) as never);
    const rendered = renderHook(() => useProspector(), { wrapper: Wrapper });
    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    return rendered;
  };

  it('omits sample rows when the setting is off, keeping columns and count', async () => {
    const { result } = await renderWithStatus(false);
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rows).toBeUndefined();
    expect(out.columns).toBeDefined();
    expect(out.rowCount).toBe(1);
  });

  it('includes sample rows when the setting is on', async () => {
    const { result } = await renderWithStatus(true);
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rows).toBeDefined();
  });

  /**
   * The global Prospector tab passes no privacy hint at all — under the old design that
   * was exactly the path that leaked. Its context must not be able to re-open the gate.
   */
  it('ignores a sendDataToAi hint on the context when the setting is off', async () => {
    const { result } = await renderWithStatus(false);
    const out = JSON.parse(await result.current.executeToolCall(
      sqlCall(), { feature: 'global_chat', sendDataToAi: true } as unknown as ChatContext));
    expect(out.rows).toBeUndefined();
  });

  /** A privacy setting that cannot be read must fail closed, not open. */
  it('withholds sample rows when the status fetch fails', async () => {
    vi.mocked(getAiStatus).mockRejectedValue(new Error('403'));
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rows).toBeUndefined();
  });
});

describe('prospectorChatKey', () => {
  it('scopes a conversation to one tab within a project', () => {
    expect(prospectorChatKey('p1', 'tab-a')).not.toBe(prospectorChatKey('p1', 'tab-b'));
  });

  it('scopes a conversation to one project for the same tab id', () => {
    expect(prospectorChatKey('p1', 'tab-a')).not.toBe(prospectorChatKey('p2', 'tab-a'));
  });

  /** Tabs exist outside a project too, and their conversations should persist. */
  it('still produces a key when there is no project', () => {
    expect(prospectorChatKey(undefined, 'tab-a')).toBeTruthy();
    expect(prospectorChatKey(undefined, 'tab-a')).not.toBe(prospectorChatKey('p1', 'tab-a'));
  });

  it('returns null without a tab, so nothing is stored under a shared key', () => {
    expect(prospectorChatKey('p1', undefined)).toBeNull();
  });

  it('is stable for the same inputs', () => {
    expect(prospectorChatKey('p1', 'tab-a')).toBe(prospectorChatKey('p1', 'tab-a'));
  });
});

describe('conversationLengthFor', () => {
  beforeEach(() => {
    const store: Record<string, string> = {};
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => store[k] ?? null,
      setItem: (k: string, v: string) => { store[k] = v; },
      removeItem: (k: string) => { delete store[k]; },
      clear: () => { for (const k of Object.keys(store)) { delete store[k]; } },
      get length() { return Object.keys(store).length; },
    });
  });

  /**
   * Readable for any tab, not just the open one: the promotion rule has to know
   * whether a tab being closed has a conversation, and that tab is not the one whose
   * conversation is currently loaded in the panel.
   */
  it('counts the messages stored for a tab', () => {
    localStorage.setItem(prospectorChatKey('p1', 'tab-a')!,
      JSON.stringify([{ role: 'user' }, { role: 'assistant' }]));
    expect(conversationLengthFor('p1', 'tab-a')).toBe(2);
  });

  it('returns 0 for a tab that has never been talked to', () => {
    expect(conversationLengthFor('p1', 'tab-none')).toBe(0);
  });

  it('returns 0 rather than throwing on unreadable storage', () => {
    localStorage.setItem(prospectorChatKey('p1', 'tab-bad')!, 'not json');
    expect(conversationLengthFor('p1', 'tab-bad')).toBe(0);
  });

  it('returns 0 without a tab id', () => {
    expect(conversationLengthFor('p1', undefined)).toBe(0);
  });
});

describe('per-project chat persistence', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    const store: Record<string, string> = {};
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => store[k] ?? null,
      setItem: (k: string, v: string) => { store[k] = v; },
      removeItem: (k: string) => { delete store[k]; },
      clear: () => { for (const k of Object.keys(store)) { delete store[k]; } },
      get length() { return Object.keys(store).length; },
    });
  });

  it('loads existing history for the project on mount', () => {
    localStorage.setItem('prospector_chat_p1',
      JSON.stringify([{ role: 'user', content: 'hi' }]));
    const { result } = renderHook(() => useProspector(undefined, undefined, undefined, 'prospector_chat_p1'), { wrapper: Wrapper });
    expect(result.current.messages).toEqual([{ role: 'user', content: 'hi' }]);
  });

  it('swaps history when the project (storageKey) changes', () => {
    localStorage.setItem('prospector_chat_p1', JSON.stringify([{ role: 'user', content: 'in p1' }]));
    localStorage.setItem('prospector_chat_p2', JSON.stringify([{ role: 'user', content: 'in p2' }]));
    const { result, rerender } = renderHook(
      ({ key }) => useProspector(undefined, undefined, undefined, key),
      { initialProps: { key: 'prospector_chat_p1' }, wrapper: Wrapper },
    );
    expect(result.current.messages[0].content).toBe('in p1');
    rerender({ key: 'prospector_chat_p2' });
    expect(result.current.messages[0].content).toBe('in p2');
  });

  it('does not touch storage when no storageKey is given', () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    expect(result.current.messages).toEqual([]);
    expect(localStorage.length).toBe(0);
  });

  /** Switching tabs must show that tab's conversation, not the previous one's. */
  it('keeps a separate conversation per tab', () => {
    localStorage.setItem(prospectorChatKey('p1', 'tab-a')!,
      JSON.stringify([{ role: 'user', content: 'about tab a' }]));
    const { result, rerender } = renderHook(
      ({ key }) => useProspector(undefined, undefined, undefined, key),
      { initialProps: { key: prospectorChatKey('p1', 'tab-a')! }, wrapper: Wrapper },
    );
    expect(result.current.messages[0].content).toBe('about tab a');

    rerender({ key: prospectorChatKey('p1', 'tab-b')! });
    expect(result.current.messages).toEqual([]);
  });

  it('restores a tab conversation when returning to it', () => {
    localStorage.setItem(prospectorChatKey('p1', 'tab-a')!,
      JSON.stringify([{ role: 'user', content: 'about tab a' }]));
    const { result, rerender } = renderHook(
      ({ key }) => useProspector(undefined, undefined, undefined, key),
      { initialProps: { key: prospectorChatKey('p1', 'tab-a')! }, wrapper: Wrapper },
    );
    rerender({ key: prospectorChatKey('p1', 'tab-b')! });
    rerender({ key: prospectorChatKey('p1', 'tab-a')! });
    expect(result.current.messages[0].content).toBe('about tab a');
  });
});

/**
 * Server-primary persistence. With a conversationId the server holds the conversation
 * and localStorage is only a buffer, so a save the server refuses means a turn that
 * exists nowhere else. These tests pin that no refused or failed save is swallowed and
 * that no unsaved turn is ever dropped.
 */
describe('server-primary conversation persistence', () => {
  const DEBOUNCE_MS = 2100;
  let store: Record<string, string>;

  const record = (version: number): ConversationRecord => ({
    id: 'c1', projectId: 'p1', owner: 'me', name: 'n', createdAt: 0, updatedAt: 0,
    messageCount: 0, version,
  });
  const serverHolds = (messages: ChatMessage[], version = 3, cid = 'c1') =>
    vi.mocked(getServerConversation).mockImplementation(((_p: string, id: string) =>
      Promise.resolve({ record: { ...record(version), id: id === cid ? cid : id }, messages })) as never);
  const OLD: ChatMessage[] = [
    { role: 'user', content: 'earlier question' },
    { role: 'assistant', content: 'earlier answer' },
  ];

  const mount = (cid: string | null = 'c1') =>
    renderHook(
      ({ id }) => useProspector(undefined, undefined, undefined, null, null, id, 'p1'),
      { initialProps: { id: cid }, wrapper: Wrapper },
    );
  const settle = (ms = 0) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });
  const send = async (result: { current: ReturnType<typeof useProspector> }, text: string) => {
    act(() => { result.current.sendMessage(text, ctx('p1')); });
    await settle();
  };
  const buffered = (text: string) => Object.values(store).some((v) => v.includes(text));

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    store = {};
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => store[k] ?? null,
      setItem: (k: string, v: string) => { store[k] = v; },
      removeItem: (k: string) => { delete store[k]; },
      clear: () => { store = {}; },
      get length() { return Object.keys(store).length; },
    });
    serverHolds(OLD);
    vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'ok', version: 4 });
    // Every stream replies "ok" at once, so a sent turn settles into user + assistant.
    vi.mocked(streamChat).mockImplementation(((_req, onDelta, onDone) => {
      onDelta({ type: 'content', content: 'ok' } as never);
      onDone({ finish_reason: 'stop' } as never);
      return new AbortController();
    }) as never);
  });

  afterEach(() => {
    vi.useRealTimers();
    // Targeted, not vi.restoreAllMocks(): that would also wipe the vi.fn() implementations
    // that a hook cleanup (flushing a pending save) still runs against after this hook.
    for (const spied of [console.error, message.info, message.warning]) {
      (spied as { mockRestore?: () => void }).mockRestore?.();
    }
  });

  it('loads messages from the server when a conversationId is given', async () => {
    const { result } = mount();
    await settle();

    expect(getServerConversation).toHaveBeenCalledWith('p1', 'c1');
    expect(result.current.messages).toEqual(OLD);
    expect(result.current.conversationId).toBe('c1');
    expect(result.current.saveStatus).toBe('idle');
  });

  it('does not write back what it just loaded', async () => {
    mount();
    await settle();
    await settle(DEBOUNCE_MS);

    expect(saveConversationMessages).not.toHaveBeenCalled();
  });

  it('saves the new turn with the version it loaded', async () => {
    const { result } = mount();
    await settle();
    await send(result, 'second question');
    await settle(DEBOUNCE_MS);

    expect(saveConversationMessages).toHaveBeenCalledTimes(1);
    expect(saveConversationMessages).toHaveBeenCalledWith(
      'p1', 'c1',
      [...OLD, { role: 'user', content: 'second question' }, { role: 'assistant', content: 'ok' }],
      3,
    );
    expect(result.current.saveStatus).toBe('idle');
  });

  /** A stale version is the 409 case, so each save must carry the version the last one returned. */
  it('sends the version returned by the previous save on the next one', async () => {
    vi.mocked(saveConversationMessages)
      .mockResolvedValueOnce({ status: 'ok', version: 4 })
      .mockResolvedValueOnce({ status: 'ok', version: 5 });
    const { result } = mount();
    await settle();
    await send(result, 'one');
    await settle(DEBOUNCE_MS);
    await send(result, 'two');
    await settle(DEBOUNCE_MS);

    const versions = vi.mocked(saveConversationMessages).mock.calls.map((c) => c[3]);
    expect(versions).toEqual([3, 4]);
  });

  /** Two turns inside one debounce window are one save, not two racing on the same version. */
  it('batches turns inside the debounce window into one save', async () => {
    const { result } = mount();
    await settle();
    await send(result, 'one');
    await send(result, 'two');
    await settle(DEBOUNCE_MS);

    expect(saveConversationMessages).toHaveBeenCalledTimes(1);
    expect(vi.mocked(saveConversationMessages).mock.calls[0][2]).toHaveLength(OLD.length + 4);
  });

  /** A save still in flight must finish before the next starts, or both carry the same version. */
  it('does not start a save while the previous one is still in flight', async () => {
    let finishFirst: (r: { status: 'ok'; version: number }) => void = () => {};
    vi.mocked(saveConversationMessages)
      .mockImplementationOnce(() => new Promise((resolve) => { finishFirst = resolve as never; }))
      .mockResolvedValueOnce({ status: 'ok', version: 5 });
    const { result } = mount();
    await settle();
    await send(result, 'one');
    await settle(DEBOUNCE_MS);
    await send(result, 'two');
    await settle(DEBOUNCE_MS);

    expect(saveConversationMessages).toHaveBeenCalledTimes(1);

    finishFirst({ status: 'ok', version: 4 });
    await settle();

    expect(saveConversationMessages).toHaveBeenCalledTimes(2);
    expect(vi.mocked(saveConversationMessages).mock.calls[1][3]).toBe(4);
  });

  describe('when the conversation reaches the size cap', () => {
    beforeEach(() => {
      vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'too-large' });
    });

    it('surfaces saveStatus "too-large"', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'one more');
      await settle(DEBOUNCE_MS);

      expect(result.current.saveStatus).toBe('too-large');
    });

    /** The turn the server refused is the one the user just sent, so it has to stay visible and buffered. */
    it('keeps the refused turn on screen and in the local buffer', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'one more');
      await settle(DEBOUNCE_MS);

      expect(result.current.messages.map((m) => m.content)).toContain('one more');
      expect(buffered('one more')).toBe(true);
    });

    /** Hard stop: a further turn would be refused too, so none is accepted into a thread that cannot hold it. */
    it('refuses further turns instead of accepting ones it cannot save', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'one more');
      await settle(DEBOUNCE_MS);
      vi.mocked(streamChat).mockClear();
      await send(result, 'and another');

      expect(streamChat).not.toHaveBeenCalled();
      expect(result.current.messages.map((m) => m.content)).not.toContain('and another');
    });

    it('does not keep retrying a save the server has already refused', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'one more');
      await settle(DEBOUNCE_MS * 3);

      expect(saveConversationMessages).toHaveBeenCalledTimes(1);
    });
  });

  describe('when another device wrote first', () => {
    beforeEach(() => {
      vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'conflict' });
    });

    it('surfaces saveStatus "conflict"', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'my turn');
      await settle(DEBOUNCE_MS);

      expect(result.current.saveStatus).toBe('conflict');
    });

    /** Reloading on conflict would overwrite the message the user just typed. */
    it('keeps the unsaved turn rather than replacing it with the server copy', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'my turn');
      await settle(DEBOUNCE_MS);

      expect(result.current.messages.map((m) => m.content)).toContain('my turn');
      expect(buffered('my turn')).toBe(true);
      expect(saveConversationMessages).toHaveBeenCalledTimes(1);
    });

    it('takes the server copy only when the user asks to reload', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'my turn');
      await settle(DEBOUNCE_MS);

      const theirs: ChatMessage[] = [...OLD, { role: 'user', content: 'from the other device' }];
      serverHolds(theirs, 9);
      await act(async () => { result.current.reloadConversation(); await vi.advanceTimersByTimeAsync(0); });

      expect(result.current.messages).toEqual(theirs);
      expect(result.current.saveStatus).toBe('idle');
    });
  });

  describe('when a save fails with a network error', () => {
    beforeEach(() => {
      // The hook logs the failure it is keeping the turn through; not output worth showing.
      vi.spyOn(console, 'error').mockImplementation(() => {});
      vi.mocked(saveConversationMessages).mockRejectedValue(new Error('Network Error'));
    });

    afterEach(() => {
      vi.mocked(console.error).mockRestore();
    });

    it('retries the save when the browser comes back online', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'unsaved turn');
      await settle(DEBOUNCE_MS);
      expect(result.current.saveStatus).toBe('error');

      vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'ok', version: 4 });
      await act(async () => { window.dispatchEvent(new Event('online')); await vi.advanceTimersByTimeAsync(0); });

      expect(saveConversationMessages).toHaveBeenCalledTimes(2);
      expect(vi.mocked(saveConversationMessages).mock.calls[1][2].map((m) => m.content))
        .toContain('unsaved turn');
      expect(result.current.saveStatus).toBe('idle');
    });

    it('surfaces saveStatus "error" and keeps the turn in localStorage', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'unsaved turn');
      await settle(DEBOUNCE_MS);

      expect(result.current.saveStatus).toBe('error');
      expect(buffered('unsaved turn')).toBe(true);
    });

    it('replays the buffered turn when the conversation is opened again', async () => {
      const first = mount();
      await settle();
      await send(first.result, 'unsaved turn');
      await settle(DEBOUNCE_MS);
      first.unmount();

      vi.mocked(saveConversationMessages).mockReset();
      vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'ok', version: 4 });
      const second = mount();
      await settle();

      expect(second.result.current.messages.map((m) => m.content)).toContain('unsaved turn');

      await settle(DEBOUNCE_MS);
      expect(saveConversationMessages).toHaveBeenCalledTimes(1);
      expect(vi.mocked(saveConversationMessages).mock.calls[0][2].map((m) => m.content))
        .toContain('unsaved turn');
      expect(vi.mocked(saveConversationMessages).mock.calls[0][3]).toBe(3);
    });

    /** The buffer was taken against an older version, so it cannot simply be saved over what the server has now. */
    it('reports a conflict, not a silent overwrite, when the server moved on meanwhile', async () => {
      const first = mount();
      await settle();
      await send(first.result, 'unsaved turn');
      await settle(DEBOUNCE_MS);
      first.unmount();

      vi.mocked(saveConversationMessages).mockReset();
      serverHolds([...OLD, { role: 'user', content: 'from the other device' }], 7);
      const second = mount();
      await settle();
      await settle(DEBOUNCE_MS);

      expect(second.result.current.saveStatus).toBe('conflict');
      expect(second.result.current.messages.map((m) => m.content)).toContain('unsaved turn');
      expect(saveConversationMessages).not.toHaveBeenCalled();

      // Opening it again must not launder the buffer into a replayable one.
      second.unmount();
      const third = mount();
      await settle();
      await settle(DEBOUNCE_MS);

      expect(third.result.current.saveStatus).toBe('conflict');
      expect(third.result.current.messages.map((m) => m.content)).toContain('unsaved turn');
      expect(saveConversationMessages).not.toHaveBeenCalled();
    });
  });

  describe('a tool call still running when the user moves on', () => {
    let finishTool: (schemas: unknown[]) => void = () => {};
    beforeEach(() => {
      vi.mocked(getSchemas).mockImplementation(
        () => new Promise((resolve) => { finishTool = resolve as never; }) as never);
      // Round 1 asks for a tool and the tool is held open; round 2 must never be reached.
      let round = 0;
      vi.mocked(streamChat).mockImplementation(((_req, onDelta, onDone) => {
        if (round++ === 0) {
          onDelta({ type: 'tool_call_start', id: 'call-1', name: 'list_schemas' } as never);
          onDelta({ type: 'tool_call_end', id: 'call-1' } as never);
          onDone({ finish_reason: 'tool_calls' } as never);
        } else {
          onDelta({ type: 'content', content: 'round two' } as never);
          onDone({ finish_reason: 'stop' } as never);
        }
        return new AbortController();
      }) as never);
    });

    /**
     * The continuation holds an absolute array built from the old conversation. Landing it
     * in the new one would be saved with the new conversation's CORRECT version, so the
     * server's 409 guard could not catch the overwrite.
     */
    it('does not write the old conversation into the new one when the tool finishes', async () => {
      const { result, rerender } = mount('c1');
      await settle();
      await send(result, 'list my schemas');

      const c2: ChatMessage[] = [{ role: 'user', content: 'c2 only' }];
      serverHolds(c2, 5, 'c2');
      rerender({ id: 'c2' });
      await settle();
      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });
      await settle(DEBOUNCE_MS);
      await settle(DEBOUNCE_MS);

      expect(result.current.messages).toEqual(c2);
      expect(vi.mocked(streamChat)).toHaveBeenCalledTimes(1);
      for (const call of vi.mocked(saveConversationMessages).mock.calls.filter((c) => c[1] === 'c2')) {
        expect(call[2].map((m) => m.content)).not.toContain('list my schemas');
      }
    });

    /** Stop used to leave the tool round running; the history must still end consistently. */
    it('stops the round on Stop but still records the tool result it was waiting for', async () => {
      const { result } = mount('c1');
      await settle();
      await send(result, 'list my schemas');
      act(() => { result.current.stopStreaming(); });
      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });

      expect(vi.mocked(streamChat)).toHaveBeenCalledTimes(1);
      expect(result.current.messages.at(-1)?.role).toBe('tool');
    });
  });

  /** The panel's trash button is wired to clearChat; it must not be able to empty the record. */
  describe('clearChat on a server-primary conversation', () => {
    it('does not empty the conversation or write [] to the server', async () => {
      vi.spyOn(message, 'info').mockImplementation((() => {}) as never);
      const { result } = mount();
      await settle();
      act(() => { result.current.clearChat(); });
      await settle();
      await settle(DEBOUNCE_MS);

      expect(result.current.messages).toEqual(OLD);
      expect(saveConversationMessages).not.toHaveBeenCalled();
      expect(message.info).toHaveBeenCalled();
    });

    it('stays hard-stopped at the size cap instead of reopening the conversation to writes', async () => {
      vi.spyOn(message, 'info').mockImplementation((() => {}) as never);
      vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'too-large' });
      const { result } = mount();
      await settle();
      await send(result, 'one more');
      await settle(DEBOUNCE_MS);
      act(() => { result.current.clearChat(); });
      await settle(DEBOUNCE_MS);

      expect(result.current.saveStatus).toBe('too-large');
      expect(result.current.messages.map((m) => m.content)).toContain('one more');
      expect(saveConversationMessages).toHaveBeenCalledTimes(1);
    });
  });

  /**
   * The buffer is the only copy of an unsaved turn. Turn B is added while the write of A is
   * in flight, then the user switches: the write of A finishing must not delete the buffer
   * that still holds B, because the flushed write of A+B can then fail.
   */
  it('keeps the buffer holding turn B when the write of turn A finishes after a switch', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    let finishFirst: (r: { status: 'ok'; version: number }) => void = () => {};
    vi.mocked(saveConversationMessages)
      .mockImplementationOnce(() => new Promise((resolve) => { finishFirst = resolve as never; }))
      .mockRejectedValueOnce(new Error('Network Error'));
    const { result, rerender } = mount('c1');
    await settle();
    await send(result, 'turn A');
    await settle(DEBOUNCE_MS);
    await send(result, 'turn B');
    serverHolds([{ role: 'user', content: 'c2 only' }], 1, 'c2');
    rerender({ id: 'c2' });
    await settle();
    finishFirst({ status: 'ok', version: 4 });
    await settle();
    await settle();

    expect(saveConversationMessages).toHaveBeenCalledTimes(2);
    expect(buffered('turn B')).toBe(true);
  });

  /** A failed buffer write silently removes the only safety net, so the user must be told. */
  it('warns once when the browser refuses the local backup', async () => {
    const warn = vi.spyOn(message, 'warning').mockImplementation((() => {}) as never);
    const { result } = mount();
    await settle();
    vi.stubGlobal('localStorage', {
      getItem: (k: string) => store[k] ?? null,
      removeItem: (k: string) => { delete store[k]; },
      setItem: () => { throw new Error('QuotaExceededError'); },
    });
    await send(result, 'one');
    await send(result, 'two');

    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0][0])).toMatch(/backup/);
  });

  /** ok without a version used to write undefined into the version map and drop every later save. */
  it('reports an error, keeps the buffer and keeps saving when an ok response has no version', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(saveConversationMessages).mockResolvedValue({ status: 'ok' });
    const { result } = mount();
    await settle();
    await send(result, 'one');
    await settle(DEBOUNCE_MS);

    expect(result.current.saveStatus).toBe('error');
    expect(buffered('one')).toBe(true);

    await send(result, 'two');
    await settle(DEBOUNCE_MS);

    expect(saveConversationMessages).toHaveBeenCalledTimes(2);
    expect(vi.mocked(saveConversationMessages).mock.calls[1][3]).toBe(3);
  });

  describe('when the conversationId changes', () => {
    it('saves a pending turn to the conversation it belongs to, not the new one', async () => {
      const { result, rerender } = mount('c1');
      await settle();
      await send(result, 'pending turn');
      // Switch before the debounce fires.
      rerender({ id: 'c2' });
      await settle(DEBOUNCE_MS);

      const calls = vi.mocked(saveConversationMessages).mock.calls;
      const forC1 = calls.filter((c) => c[1] === 'c1');
      expect(forC1).toHaveLength(1);
      expect(forC1[0][2].map((m) => m.content)).toContain('pending turn');
      for (const c of calls.filter((x) => x[1] === 'c2')) {
        expect(c[2].map((m) => m.content)).not.toContain('pending turn');
      }
    });

    it('shows the new conversation, loaded from the server', async () => {
      const { result, rerender } = mount('c1');
      await settle();
      serverHolds([{ role: 'user', content: 'in c2' }], 1, 'c2');
      rerender({ id: 'c2' });
      await settle();

      expect(getServerConversation).toHaveBeenLastCalledWith('p1', 'c2');
      expect(result.current.messages).toEqual([{ role: 'user', content: 'in c2' }]);
    });
  });
});
