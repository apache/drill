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
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';

import { useProspector } from './useProspector';
import { createVisualization } from '../api/visualizations';
import { addVisualization, getProject } from '../api/projects';
import { executeQuery } from '../api/queries';
import type { ChatContext, ToolCall } from '../types/ai';

vi.mock('../api/visualizations', () => ({ createVisualization: vi.fn() }));
vi.mock('../api/projects', () => ({ addVisualization: vi.fn(), getProject: vi.fn() }));
vi.mock('../api/ai', () => ({ streamChat: vi.fn() }));
vi.mock('../api/queries', () => ({ executeQuery: vi.fn() }));
vi.mock('../api/metadata', () => ({
  getSchemas: vi.fn(), getTables: vi.fn(), getColumns: vi.fn(), getFunctions: vi.fn(),
}));
vi.mock('../api/dashboards', () => ({ createDashboard: vi.fn() }));
vi.mock('../api/savedQueries', () => ({ createSavedQuery: vi.fn() }));

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
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx('proj-42')));

    expect(createVisualization).toHaveBeenCalledOnce();
    expect(addVisualization).toHaveBeenCalledWith('proj-42', 'viz-1');
    expect(out.id).toBe('viz-1');
    expect(out.addedToProject).toBe(true);
  });

  it('creates the visualization without a project when there is no active project', async () => {
    const { result } = renderHook(() => useProspector());
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

    const { result } = renderHook(() => useProspector());
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

    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(call(), ctx('proj-42')));

    expect(out.error).toContain('boom');
    expect(consoleError).toHaveBeenCalled();
    consoleError.mockRestore();
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
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx('proj-42')));
    expect(out.pages.map((p: { title: string }) => p.title)).toEqual(['Runbook', 'Glossary']);
  });

  it('returns the full content of a named page', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Runbook'), ctx('proj-42')));
    expect(out.title).toBe('Runbook');
    expect(out.content).toContain('step one');
  });

  it('reports a missing page rather than failing silently', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(
      await result.current.executeToolCall(docsCall('Nope'), ctx('proj-42')));
    expect(out.error).toContain('Nope');
  });

  it('requires an active project', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(docsCall(), ctx()));
    expect(out.error).toBe('No active project — get_project_docs is only available inside a project.');
    expect(out.error).not.toContain('Unknown tool');
  });
});

describe('execute_sql honours sendDataToAi', () => {
  const sqlCall = (): ToolCall => ({
    id: 'call-3',
    name: 'execute_sql',
    arguments: JSON.stringify({ sql: 'SELECT * FROM sales' }),
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(executeQuery).mockResolvedValue({
      columns: ['region', 'amount'],
      metadata: ['VARCHAR', 'INTEGER'],
      rows: [{ region: 'West', amount: 42 }],
    } as never);
  });

  it('omits sample rows when sendDataToAi is false, keeping columns and count', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(
      sqlCall(), { feature: 'sql_lab_chat', sendDataToAi: false } as ChatContext));
    expect(out.rows).toBeUndefined();
    expect(out.columns).toBeDefined();
    expect(out.rowCount).toBeDefined();
  });

  it('includes sample rows when sendDataToAi is true', async () => {
    const { result } = renderHook(() => useProspector());
    const out = JSON.parse(await result.current.executeToolCall(
      sqlCall(), { feature: 'sql_lab_chat', sendDataToAi: true } as ChatContext));
    expect(out.rows).toBeDefined();
  });
});
