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

import { useProspector, TOOL_DEFINITIONS } from './useProspector';
import { createVisualization } from '../api/visualizations';
import { createDashboard } from '../api/dashboards';
import {
  addVisualization, addDashboard, addSavedQuery, getProject, createWikiPage, updateWikiPage,
} from '../api/projects';
import { createSavedQuery } from '../api/savedQueries';
import { createWorkflow, setWorkflowEnabled } from '../api/workflows';
import prospectorSource from './useProspector.ts?raw';
import { executeQuery } from '../api/queries';
import { getSchemas } from '../api/metadata';
import { getAiStatus, streamChat } from '../api/ai';
import {
  getConversation as getServerConversation,
  saveConversationMessages,
} from '../api/conversations';
import type { ChatContext, ChatMessage, ConversationRecord, ToolCall } from '../types/ai';

// useProspector reads/writes the React Query cache (write_wiki_page invalidation),
// so every render needs a QueryClientProvider in scope.
function Wrapper({ children }: { children: React.ReactNode }) {
  const [client] = useState(() => new QueryClient({
    defaultOptions: { queries: { retry: false } },
  }));
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
}

vi.mock('../api/visualizations', () => ({ createVisualization: vi.fn() }));
vi.mock('../api/projects', () => ({
  addVisualization: vi.fn(), addDashboard: vi.fn(), addSavedQuery: vi.fn(), getProject: vi.fn(),
  createWikiPage: vi.fn(), updateWikiPage: vi.fn(),
}));
vi.mock('../api/ai', () => ({ streamChat: vi.fn(), getAiStatus: vi.fn() }));
vi.mock('../api/queries', () => ({ executeQuery: vi.fn() }));
vi.mock('../api/metadata', () => ({
  getSchemas: vi.fn(), getTables: vi.fn(), getColumns: vi.fn(), getFunctions: vi.fn(),
}));
vi.mock('../api/dashboards', () => ({ createDashboard: vi.fn() }));
vi.mock('../api/savedQueries', () => ({ createSavedQuery: vi.fn() }));
vi.mock('../api/workflows', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../api/workflows')>()),
  createWorkflow: vi.fn(), setWorkflowEnabled: vi.fn(),
}));
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
        { id: 'pg-runbook', title: 'Runbook', content: 'step one ...' },
        { id: 'pg-glossary', title: 'Glossary', content: '...' },
      ],
    } as never);
  });

  it('lists page titles when given no title', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx('proj-42')));
    expect(out.pages.map((p: { title: string }) => p.title)).toEqual(['Runbook', 'Glossary']);
  });

  // write_wiki_page updates a page by id, and this listing is the only place the model can
  // learn the id of a page it did not write itself.
  it('lists page ids so write_wiki_page can update an existing page', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx('proj-42')));
    expect(out.pages.map((p: { id: string }) => p.id)).toEqual(['pg-runbook', 'pg-glossary']);
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

describe('write_wiki_page tool', () => {
  const writeCall = (args: Record<string, unknown>): ToolCall => ({
    id: 'call-w',
    name: 'write_wiki_page',
    arguments: JSON.stringify(args),
  });
  const sqlTurn = (sql: string): ChatMessage => ({
    role: 'assistant',
    content: null,
    toolCalls: [{ id: 'q1', name: 'execute_sql', arguments: JSON.stringify({ sql }) }],
  });
  const PAGE = { id: 'pg-1', title: 'Q3 findings', content: '', order: 0, createdAt: 0, updatedAt: 0 };
  const written = (): string => vi.mocked(createWikiPage).mock.calls[0][1].content as string;

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
    vi.mocked(getProject).mockResolvedValue({ wikiPages: [] } as never);
    vi.mocked(createWikiPage).mockResolvedValue(PAGE as never);
    vi.mocked(updateWikiPage).mockResolvedValue({ ...PAGE } as never);
  });

  it('no longer exposes a save_report tool', () => {
    const names = TOOL_DEFINITIONS.map((t) => t.name);
    expect(names).not.toContain('save_report');
    expect(names).toContain('write_wiki_page');
  });

  it('creates a wiki page in the reports folder by default', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall({ title: '  Q3 findings ', content: '# Q3\n\nBody.' }), ctx('proj-42')));

    expect(createWikiPage).toHaveBeenCalledOnce();
    const [projectId, page] = vi.mocked(createWikiPage).mock.calls[0];
    expect(projectId).toBe('proj-42');
    expect(page.title).toBe('Q3 findings');
    expect(page.folder).toBe('Reports');
    expect(page.content).toContain('# Q3\n\nBody.');
    expect(updateWikiPage).not.toHaveBeenCalled();
    // The shape the artifact card reader and the model both rely on.
    expect(out).toMatchObject({
      id: 'pg-1', title: 'Q3 findings', viewPath: '/projects/proj-42/wiki/pg-1',
    });
  });

  it('writes into the folder it is given', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await result.current.executeToolCall(
      writeCall({ title: 'Runbook', content: 'steps', folder: 'Operations' }), ctx('proj-42'));
    expect(vi.mocked(createWikiPage).mock.calls[0][1].folder).toBe('Operations');
  });

  it('appends the queries run only when includeTranscript is true', async () => {
    vi.mocked(getServerConversation).mockResolvedValue({
      record: { id: 'conv-9', projectId: 'proj-42', version: 1 },
      messages: [sqlTurn('SELECT region FROM sales')],
    } as never);
    const { result } = renderHook(
      () => useProspector(undefined, undefined, undefined, 'conv-9', 'proj-42'),
      { wrapper: Wrapper });
    await waitFor(() => expect(result.current.isLoaded).toBe(true));

    await result.current.executeToolCall(
      writeCall({ title: 'A', content: 'body' }), ctx('proj-42'));
    expect(written()).not.toContain('SELECT region FROM sales');

    vi.mocked(createWikiPage).mockClear();
    await result.current.executeToolCall(
      writeCall({ title: 'B', content: 'body', includeTranscript: true }), ctx('proj-42'));
    expect(written()).toContain('## Appendix: queries run');
    expect(written()).toContain('SELECT region FROM sales');
  });

  it('keeps the provenance footer when the transcript is left out', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await result.current.executeToolCall(
      writeCall({ title: 'A', content: 'body', includeTranscript: false }), ctx('proj-42'));
    expect(written()).toContain('Generated by Prospector');
  });

  // In server mode the hook has no tab id, so the footer used to drop the conversation
  // line from every page saved from a conversation.
  it('records the conversation id in the footer of a server-primary conversation', async () => {
    vi.mocked(getServerConversation).mockResolvedValue({
      record: { id: 'conv-9', projectId: 'p1', version: 1 }, messages: [],
    } as never);
    const { result } = renderHook(
      () => useProspector(undefined, undefined, undefined, 'conv-9', 'p1'),
      { wrapper: Wrapper });
    await waitFor(() => expect(getServerConversation).toHaveBeenCalled());

    await result.current.executeToolCall(
      writeCall({ title: 'A', content: 'body' }), ctx('p1'));
    expect(written()).toContain('Conversation `conv-9`');
  });

  it('updates the named page instead of creating a duplicate', async () => {
    vi.mocked(updateWikiPage).mockResolvedValue({ ...PAGE, id: 'pg-7', title: 'Runbook' } as never);
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall({ title: 'Runbook', content: 'new steps', pageId: 'pg-7' }), ctx('proj-42')));

    expect(createWikiPage).not.toHaveBeenCalled();
    expect(updateWikiPage).toHaveBeenCalledOnce();
    const [projectId, pageId, patch] = vi.mocked(updateWikiPage).mock.calls[0];
    expect([projectId, pageId]).toEqual(['proj-42', 'pg-7']);
    expect(patch.title).toBe('Runbook');
    expect(patch.content).toContain('new steps');
    // Updating a page must not move it: the folder is only sent when the model asked.
    expect(patch).not.toHaveProperty('folder');
    expect(out).toMatchObject({
      id: 'pg-7', updated: true, viewPath: '/projects/proj-42/wiki/pg-7',
    });
  });

  it('moves an updated page only when a folder is given', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await result.current.executeToolCall(
      writeCall({ title: 'R', content: 'c', pageId: 'pg-7', folder: 'Archive' }), ctx('proj-42'));
    expect(vi.mocked(updateWikiPage).mock.calls[0][2].folder).toBe('Archive');
  });

  it('says so when the page to update no longer exists, and does not create one', async () => {
    vi.mocked(updateWikiPage).mockRejectedValue(
      Object.assign(new Error('Request failed with status code 404'), { response: { status: 404 } }));
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall({ title: 'Runbook', content: 'c', pageId: 'gone' }), ctx('proj-42')));

    expect(out.error).toContain('gone');
    expect(out.error).toContain('without pageId');
    expect(createWikiPage).not.toHaveBeenCalled();
    (console.error as unknown as { mockRestore: () => void }).mockRestore();
  });

  it('refuses to create a second page with the title of an existing one', async () => {
    vi.mocked(getProject).mockResolvedValue({
      wikiPages: [{ id: 'pg-old', title: 'q3 findings', folder: 'Reports', content: 'x' }],
    } as never);
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall({ title: 'Q3 Findings', content: 'c' }), ctx('proj-42')));

    expect(createWikiPage).not.toHaveBeenCalled();
    expect(out.error).toContain('pg-old');
    expect(out.error).toContain('pageId');
  });

  it('allows the same title in a different folder', async () => {
    vi.mocked(getProject).mockResolvedValue({
      wikiPages: [{ id: 'pg-old', title: 'Q3 findings', folder: 'Reports', content: 'x' }],
    } as never);
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await result.current.executeToolCall(
      writeCall({ title: 'Q3 findings', content: 'c', folder: 'Drafts' }), ctx('proj-42'));
    expect(createWikiPage).toHaveBeenCalledOnce();
  });

  it.each([
    ['title', { content: 'c' }],
    ['content', { title: 'A' }],
    ['a blank title', { title: '   ', content: 'c' }],
  ])('tells the model what is missing when %s is omitted', async (_label, args) => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall(args), ctx('proj-42')));

    expect(out.error).toContain('title and content are required');
    expect(out.error).not.toContain('trim');
    expect(createWikiPage).not.toHaveBeenCalled();
    expect(console.error).not.toHaveBeenCalled();
    (console.error as unknown as { mockRestore: () => void }).mockRestore();
  });

  it('needs an active project', async () => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    const out = JSON.parse(await result.current.executeToolCall(
      writeCall({ title: 'A', content: 'c' }), ctx()));
    expect(out.error).toContain('No active project');
    expect(createWikiPage).not.toHaveBeenCalled();
    expect(updateWikiPage).not.toHaveBeenCalled();
  });

  // The project and project-list caches feed the wiki list and SaveReportModal's
  // collision check. Without the refresh they show the page missing until staleTime.
  it.each([
    ['creating', {}],
    ['updating', { pageId: 'pg-7' }],
  ])('refreshes the project caches after %s a page', async (_label, extra) => {
    const client = new QueryClient();
    const invalidate = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useProspector(), {
      wrapper: ({ children }: { children: React.ReactNode }) =>
        <QueryClientProvider client={client}>{children}</QueryClientProvider>,
    });
    await result.current.executeToolCall(
      writeCall({ title: 'A', content: 'c', ...extra }), ctx('proj-42'));

    const keys = invalidate.mock.calls.map((c) => (c[0] as { queryKey: unknown[] }).queryKey);
    expect(keys).toContainEqual(['project', 'proj-42']);
    expect(keys).toContainEqual(['projects']);
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
describe('save_query tool', () => {
  let errorLog: ReturnType<typeof vi.spyOn>;
  afterEach(() => errorLog.mockRestore());

  const SQL = 'SELECT region, SUM(amount) FROM sales GROUP BY region';
  const sqCall = (): ToolCall => ({
    id: 'call-sq', name: 'save_query',
    arguments: JSON.stringify({ name: 'Sales by region', description: 'd', sql: SQL }),
  });
  const run = async (project?: string) => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    return JSON.parse(await result.current.executeToolCall(sqCall(), ctx(project)));
  };

  beforeEach(() => {
    vi.clearAllMocks();
    errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(createSavedQuery).mockResolvedValue({ id: 'sq-1', name: 'Sales by region' } as never);
    vi.mocked(addSavedQuery).mockResolvedValue({} as never);
  });

  // The project's Saved Queries tab filters on the project's own id list, so a query that is
  // only created never shows up there and cannot be checked from the normal tabs.
  it('adds the saved query to the active project', async () => {
    const out = await run('proj-42');
    expect(addSavedQuery).toHaveBeenCalledWith('proj-42', 'sq-1');
    expect(out).toMatchObject({ id: 'sq-1', name: 'Sales by region', addedToProject: true });
  });

  it('saves the query without a project when there is no active project', async () => {
    const out = await run();
    expect(createSavedQuery).toHaveBeenCalledOnce();
    expect(addSavedQuery).not.toHaveBeenCalled();
    expect(out.addedToProject).toBe(false);
    expect(out).not.toHaveProperty('projectError');
  });

  it('refreshes the project cache that feeds the Saved Queries tab', async () => {
    const client = new QueryClient();
    const invalidate = vi.spyOn(client, 'invalidateQueries');
    const { result } = renderHook(() => useProspector(), {
      wrapper: ({ children }: { children: React.ReactNode }) =>
        <QueryClientProvider client={client}>{children}</QueryClientProvider>,
    });
    await result.current.executeToolCall(sqCall(), ctx('proj-42'));
    const keys = invalidate.mock.calls.map((c) => (c[0] as { queryKey: unknown[] }).queryKey);
    expect(keys).toContainEqual(['project', 'proj-42']);
  });

  // Only a project's owner can add to it, while any member can save a query. The query
  // exists either way, so this is not an error: the card and canvas still need its id.
  it('reports a non-owner as saved but not in the project, and says not to retry', async () => {
    vi.mocked(addSavedQuery).mockRejectedValue(
      { isAxiosError: true, response: { status: 403, data: { message: 'Only the owner can modify this project' } } });
    const out = await run('proj-42');
    expect(out).toMatchObject({ id: 'sq-1', name: 'Sales by region', addedToProject: false });
    expect(out).not.toHaveProperty('error');
    expect(out.projectError).toMatch(/project owner/i);
    expect(out.projectError).toMatch(/do not retry/i);
    expect(createSavedQuery).toHaveBeenCalledOnce();
  });

  it('warns that calling again would save a duplicate when adding fails for another reason', async () => {
    vi.mocked(addSavedQuery).mockRejectedValue(new Error('Network Error'));
    const out = await run('proj-42');
    expect(out).toMatchObject({ id: 'sq-1', addedToProject: false });
    expect(out).not.toHaveProperty('error');
    expect(out.projectError).toContain('Network Error');
    expect(out.projectError).toMatch(/duplicate/i);
  });
});

describe('create_workflow tool', () => {
  let errorLog: ReturnType<typeof vi.spyOn>;
  afterEach(() => errorLog.mockRestore());

  const SQL = 'SELECT COUNT(*) AS n FROM dfs.logs.`errors.json`';
  const wfCall = (args: Record<string, unknown>): ToolCall => ({
    id: 'call-wf', name: 'create_workflow', arguments: JSON.stringify(args),
  });
  const args = (over: Record<string, unknown> = {}) => ({
    name: 'Nightly error check',
    sql: SQL,
    frequency: 'daily',
    timeOfDay: '06:30',
    actions: [{
      id: 'notify', type: 'email', when: 'rowCount > 0',
      config: { to: ['oncall@example.com'], subject: 'Errors: {{rowCount}}', body: 'See the run.' },
    }],
    ...over,
  });
  const sent = () => vi.mocked(createWorkflow).mock.calls[0][0];
  const run = async (a: Record<string, unknown>, project: string | null = 'proj-42') => {
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    return JSON.parse(await result.current.executeToolCall(wfCall(a), ctx(project ?? undefined)));
  };
  const axios422 = (errors: { path: string; message: string }[]) =>
    ({ isAxiosError: true, response: { status: 422, data: { errors } } });

  beforeEach(() => {
    vi.clearAllMocks();
    // The failure paths log through console.error by design; keep the test output clean.
    errorLog = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(createSavedQuery).mockResolvedValue({ id: 'sq-1', name: 'Nightly error check' } as never);
    vi.mocked(addSavedQuery).mockResolvedValue({} as never);
    vi.mocked(createWorkflow).mockImplementation(async (wf) => (
      { ...wf, id: 'wf-1', status: 'paused', schedule: { ...wf.schedule, enabled: false } }));
  });

  it('is offered to the model, with sql and the schedule as the way in', () => {
    const def = TOOL_DEFINITIONS.find((t) => t.name === 'create_workflow');
    expect(def).toBeDefined();
    expect(def!.parameters.required).toEqual(expect.arrayContaining(['name', 'frequency', 'actions']));
    expect(Object.keys(def!.parameters.properties)).toEqual(
      expect.arrayContaining(['sql', 'savedQueryId', 'timeOfDay', 'dayOfWeek', 'dayOfMonth']));
  });

  it('saves the sql as a query in the project, then creates the workflow from it', async () => {
    const out = await run(args());

    expect(createSavedQuery).toHaveBeenCalledWith({
      name: 'Nightly error check', description: undefined, sql: SQL,
    });
    // The server refuses a saved query that is not in the workflow's project.
    expect(addSavedQuery).toHaveBeenCalledWith('proj-42', 'sq-1');
    expect(sent()).toMatchObject({
      projectId: 'proj-42',
      name: 'Nightly error check',
      savedQueryId: 'sq-1',
      schedule: { frequency: 'daily', timeOfDay: '06:30', enabled: false },
    });
    expect(sent().actions).toEqual([{
      id: 'notify', type: 'email', condition: { expr: 'rowCount > 0' },
      config: { to: ['oncall@example.com'], subject: 'Errors: {{rowCount}}', body: 'See the run.' },
    }]);
    // The shape the artifact card reader relies on, and a plain statement of the state.
    expect(out).toMatchObject({ id: 'wf-1', name: 'Nightly error check', savedQueryId: 'sq-1', status: 'paused' });
    expect(out.message).toMatch(/not running/i);
  });

  // Not every surface renders the Activate button (only the Conversations page opens
  // artifacts), so the model must be pointed at a route that exists everywhere.
  it('points the model at the workflow page, not at a button that may not be on screen', async () => {
    const out = await run(args());
    expect(out.viewPath).toBe('/projects/proj-42/workflows/wf-1');
    expect(out.message).toContain(out.viewPath);
    expect(out.message).not.toMatch(/button under this message/i);
    const description = TOOL_DEFINITIONS.find((t) => t.name === 'create_workflow')!.description;
    expect(description).toContain('Workflows page');
    expect(description).not.toMatch(/button that appears under it/i);
  });

  it('uses an existing saved query when given its id, and saves nothing new', async () => {
    const out = await run(args({ sql: undefined, savedQueryId: 'sq-7' }));
    expect(createSavedQuery).not.toHaveBeenCalled();
    expect(sent().savedQueryId).toBe('sq-7');
    expect(out.id).toBe('wf-1');
  });

  it('runs an action unconditionally when it gives no condition', async () => {
    await run(args({ actions: [{ id: 'a', type: 'record_alert', config: { message: 'x' } }] }));
    expect(sent().actions[0].condition).toEqual({ always: true });
  });

  it('passes weekly and monthly day fields through for the server to check', async () => {
    await run(args({ frequency: 'weekly', dayOfWeek: 1 }));
    expect(sent().schedule).toMatchObject({ frequency: 'weekly', dayOfWeek: 1 });
  });

  it('sends the workflow disabled and never asks for it to be enabled, whatever the model passes', async () => {
    // Regression guard only: the server pins every created workflow paused. What keeps the
    // tool out of the gate is that nothing here calls setWorkflowEnabled.
    await run(args({ enabled: true, schedule: { enabled: true }, status: 'active' }));
    expect(sent().schedule.enabled).toBe(false);
    expect(setWorkflowEnabled).not.toHaveBeenCalled();
  });

  it('cannot reach setWorkflowEnabled at all', () => {
    // A cheap tripwire, not a guarantee: it shows only that this one file lacks these names
    // today. It would miss an alias or helper in another module, a split-out executor, a
    // dynamic import, or a raw apiClient call, and a comment could trip it. The protection is
    // the server (create pins paused, the runner skips disabled workflows, update cannot
    // enable) plus the single caller of setWorkflowEnabled, WorkflowActivationCard's confirm.
    expect(prospectorSource).not.toMatch(/setWorkflowEnabled/);
    expect(prospectorSource).not.toMatch(/updateWorkflow|runWorkflowNow/);
  });

  it('is refused with no active project, before anything is saved', async () => {
    const out = await run(args(), null);
    expect(out.error).toMatch(/project/i);
    expect(createSavedQuery).not.toHaveBeenCalled();
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('asks for sql or a saved query id when it has neither', async () => {
    const out = await run(args({ sql: undefined }));
    expect(out.error).toMatch(/sql or savedQueryId/);
    expect(createWorkflow).not.toHaveBeenCalled();
  });

  it('hands the server field errors back so the model can fix them, and keeps the saved query id for the retry', async () => {
    vi.mocked(createWorkflow).mockRejectedValue(axios422([
      { path: 'schedule.dayOfWeek', message: 'Required for weekly schedules' },
      { path: 'actions[0].config.to', message: 'At least 1 required' },
    ]));
    const out = await run(args({ frequency: 'weekly' }));

    expect(out.error).toContain('schedule.dayOfWeek: Required for weekly schedules');
    expect(out.error).toContain('actions[0].config.to: At least 1 required');
    // The query was saved before validation could run. Retrying with sql would save it twice.
    expect(out.savedQueryId).toBe('sq-1');
    expect(out.error).toMatch(/savedQueryId "sq-1"/);
  });

  it('does not tell the model to swap sql for an id it supplied itself', async () => {
    vi.mocked(createWorkflow).mockRejectedValue(axios422([
      { path: 'savedQueryId', message: 'Saved query is not in this project' },
    ]));
    const out = await run(args({ sql: undefined, savedQueryId: 'sq-7' }));
    expect(out.error).toContain('savedQueryId: Saved query is not in this project');
    expect(out.error).not.toMatch(/instead of sql/);
  });

  it('reports a non-validation failure from the server as it is', async () => {
    vi.mocked(createWorkflow).mockRejectedValue(new Error('Network Error'));
    const out = await run(args());
    expect(out.error).toContain('Network Error');
    expect(out.savedQueryId).toBe('sq-1');
  });

  it('stops, without suggesting a retry, when a non-owner cannot add the query to the project', async () => {
    vi.mocked(addSavedQuery).mockRejectedValue(
      { isAxiosError: true, response: { status: 403, data: { message: 'Only the owner can modify this project' } } });
    const out = await run(args());
    expect(createWorkflow).not.toHaveBeenCalled();
    // A retry cannot succeed: with savedQueryId the query is still not in the project.
    expect(out.error).toMatch(/project owner/i);
    expect(out.error).not.toMatch(/call create_workflow again/i);
    expect(out.error).not.toMatch(/instead of sql/i);
    expect(out).not.toHaveProperty('savedQueryId');
  });

  it('allows one more attempt, saying what it costs, when adding the query fails for another reason', async () => {
    vi.mocked(addSavedQuery).mockRejectedValue(new Error('Network Error'));
    const out = await run(args());
    expect(createWorkflow).not.toHaveBeenCalled();
    expect(out.error).toContain('Network Error');
    expect(out.error).toMatch(/saved again/i);
    expect(out.error).not.toMatch(/instead of sql/i);
  });
});

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

/**
 * The conversations canvas shows the result of each query Prospector runs. The model is
 * told only a summary, and rows only when sendDataToAi allows, but the user ran this
 * query, so their own screen gets the whole result whatever that setting says.
 */
describe('execute_sql hands the full result to onQueryResult', () => {
  const FULL = {
    columns: ['region', 'amount'],
    metadata: ['VARCHAR', 'INTEGER'],
    rows: Array.from({ length: 8 }, (_, i) => ({ region: `r${i}`, amount: i })),
    queryId: 'q-1',
  };
  const sqlCall = (sql = 'SELECT * FROM sales'): ToolCall => ({
    id: 'call-q', name: 'execute_sql', arguments: JSON.stringify({ sql }),
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(executeQuery).mockResolvedValue(FULL as never);
  });

  const mount = async (sendDataToAi: boolean, onQueryResult = vi.fn()) => {
    vi.mocked(getAiStatus).mockResolvedValue(
      { enabled: true, configured: true, sendDataToAi } as never);
    const rendered = renderHook(
      () => useProspector(undefined, undefined, undefined, null, null, onQueryResult),
      { wrapper: Wrapper },
    );
    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    return { ...rendered, onQueryResult };
  };

  it('passes the SQL and every row, not the five-row sample the model gets', async () => {
    const { result, onQueryResult } = await mount(true);
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rows).toHaveLength(5);
    expect(onQueryResult).toHaveBeenCalledTimes(1);
    expect(onQueryResult).toHaveBeenCalledWith('SELECT * FROM sales', FULL);
  });

  it('still passes the rows when sendDataToAi withholds them from the model', async () => {
    const { result, onQueryResult } = await mount(false);
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rows).toBeUndefined();
    expect(onQueryResult.mock.calls[0][1].rows).toHaveLength(8);
  });

  it('does not report a query that failed', async () => {
    vi.mocked(executeQuery).mockRejectedValue(new Error('no such table'));
    const { result, onQueryResult } = await mount(true);
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.error).toMatch(/no such table/);
    expect(onQueryResult).not.toHaveBeenCalled();
  });

  it('has nothing to report when the call carried no SQL string', async () => {
    const { result, onQueryResult } = await mount(true);
    await result.current.executeToolCall(
      { id: 'call-q', name: 'execute_sql', arguments: JSON.stringify({}) }, ctx());
    expect(onQueryResult).not.toHaveBeenCalled();
  });

  it('still summarises for the model when no callback is given', async () => {
    vi.mocked(getAiStatus).mockResolvedValue(
      { enabled: true, configured: true, sendDataToAi: true } as never);
    const { result } = renderHook(() => useProspector(), { wrapper: Wrapper });
    await waitFor(() => expect(getAiStatus).toHaveBeenCalled());
    const out = JSON.parse(await result.current.executeToolCall(sqlCall(), ctx()));
    expect(out.rowCount).toBe(8);
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
      ({ id }) => useProspector(undefined, undefined, undefined, id, 'p1'),
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

  it('reports isLoaded only once the conversation has loaded, and again false after a switch', async () => {
    let finish!: (v: unknown) => void;
    vi.mocked(getServerConversation).mockImplementation((() =>
      new Promise((r) => { finish = r; })) as never);
    const { result, rerender } = mount();
    expect(result.current.isLoaded).toBe(false);
    await act(async () => { finish({ record: record(3), messages: OLD }); await vi.advanceTimersByTimeAsync(0); });
    expect(result.current.isLoaded).toBe(true);
    rerender({ id: 'c2' });
    expect(result.current.isLoaded).toBe(false);
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

  /**
   * Both providers answer an unmatched tool_use with a 400, so a conversation persisted
   * with one can never take another turn. These pin that it cannot get there.
   */
  describe('the unanswered tool_use window', () => {
    let finishTool: (schemas: unknown[]) => void = () => {};
    const endsWithToolUse = (messages: ChatMessage[]) => {
      const last = messages[messages.length - 1];
      return last?.role === 'assistant' && (last.toolCalls?.length ?? 0) > 0;
    };

    beforeEach(() => {
      vi.mocked(getSchemas).mockImplementation(
        () => new Promise((resolve) => { finishTool = resolve as never; }) as never);
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

    /** The input is only disabled while isStreaming, so it has to stay true while the tool runs. */
    it('refuses a message typed while a tool is running, so none is built on the open tool_use', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'list my schemas');

      expect(result.current.isStreaming).toBe(true);
      await send(result, 'typed during the tool');
      expect(streamChat).toHaveBeenCalledTimes(1);
      expect(result.current.messages.map((m) => m.content)).not.toContain('typed during the tool');

      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });
      await settle(DEBOUNCE_MS);

      expect(result.current.isStreaming).toBe(false);
      expect(result.current.messages.map((m) => m.role).slice(-4))
        .toEqual(['user', 'assistant', 'tool', 'assistant']);
      const saves = vi.mocked(saveConversationMessages).mock.calls;
      expect(saves.length).toBeGreaterThan(0);
      for (const call of saves) {
        expect(endsWithToolUse(call[2])).toBe(false);
      }
    });

    /** A round cut off by a switch leaves the open tool_use in the buffer; replaying it would brick the thread. */
    it('does not replay an unanswered tool_use from the buffer', async () => {
      const { result, rerender } = mount('c1');
      await settle();
      await send(result, 'list my schemas');
      serverHolds([{ role: 'user', content: 'c2 only' }], 1, 'c2');
      rerender({ id: 'c2' });
      await settle();
      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });
      serverHolds(OLD);
      rerender({ id: 'c1' });
      await settle();
      await settle(DEBOUNCE_MS);

      // The turn the user sent is kept; only the unanswerable tool_use is dropped.
      expect(result.current.messages).toEqual([...OLD, { role: 'user', content: 'list my schemas' }]);
      const saves = vi.mocked(saveConversationMessages).mock.calls.filter((c) => c[1] === 'c1');
      expect(saves).toHaveLength(1);
      expect(endsWithToolUse(saves[0][2])).toBe(false);
      expect(saves[0][2].map((m) => m.content)).toContain('list my schemas');
    });

    it('does not send a provider a history that ends in an unanswered tool_use', async () => {
      serverHolds([
        ...OLD,
        { role: 'user', content: 'list my schemas' },
        { role: 'assistant', content: null, toolCalls: [{ id: 'old', name: 'list_schemas', arguments: '{}' }] },
      ]);
      vi.mocked(streamChat).mockClear();
      vi.mocked(streamChat).mockImplementation(((_req, _onDelta, onDone) => {
        onDone({ finish_reason: 'stop' } as never);
        return new AbortController();
      }) as never);
      const { result } = mount();
      await settle();
      await send(result, 'try again');

      const sent = vi.mocked(streamChat).mock.calls[0][0].messages;
      expect(sent.some((m) => m.toolCalls?.length)).toBe(false);
      expect(sent[sent.length - 1].content).toBe('try again');
    });

    /**
     * Stop leaves the open tool_use in state with isStreaming false, so the debounced
     * flush on a switch can reach the server with it. The prose beside it is kept.
     */
    it('saves a Stopped, switched-away round without its unanswered tool call, keeping the prose', async () => {
      vi.mocked(streamChat).mockImplementation(((_req, onDelta, onDone) => {
        onDelta({ type: 'content', content: 'Let me check the schemas.' } as never);
        onDelta({ type: 'tool_call_start', id: 'call-1', name: 'list_schemas' } as never);
        onDelta({ type: 'tool_call_end', id: 'call-1' } as never);
        onDone({ finish_reason: 'tool_calls' } as never);
        return new AbortController();
      }) as never);
      const { result, rerender } = mount('c1');
      await settle();
      await send(result, 'list my schemas');
      act(() => { result.current.stopStreaming(); });
      serverHolds([{ role: 'user', content: 'c2 only' }], 1, 'c2');
      rerender({ id: 'c2' });
      await settle(DEBOUNCE_MS);
      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });

      const saves = vi.mocked(saveConversationMessages).mock.calls.filter((c) => c[1] === 'c1');
      expect(saves).toHaveLength(1);
      const payload = saves[0][2];
      expect(payload.some((m) => m.toolCalls?.length)).toBe(false);
      expect(payload.slice(-2)).toEqual([
        { role: 'user', content: 'list my schemas' },
        { role: 'assistant', content: 'Let me check the schemas.' },
      ]);
    });

    /** A reload takes the server's copy; a tool round for the discarded copy must not write it back. */
    it('does not let a tool round running during a reload overwrite the reloaded copy', async () => {
      const { result } = mount();
      await settle();
      await send(result, 'list my schemas');

      const theirs: ChatMessage[] = [...OLD, { role: 'user', content: 'from the other device' }];
      serverHolds(theirs, 9);
      await act(async () => { result.current.reloadConversation(); await vi.advanceTimersByTimeAsync(0); });
      await act(async () => { finishTool([{ name: 'dfs' }]); await vi.advanceTimersByTimeAsync(0); });
      await settle(DEBOUNCE_MS);
      await settle(DEBOUNCE_MS);

      expect(result.current.messages).toEqual(theirs);
      expect(result.current.isStreaming).toBe(false);
      expect(streamChat).toHaveBeenCalledTimes(1);
      expect(saveConversationMessages).not.toHaveBeenCalled();
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
