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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor, renderHook, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { AxiosError } from 'axios';
import type { AxiosResponse } from 'axios';
import type { ReactNode } from 'react';
import ErrorBoundary from '../ErrorBoundary';
import { ProjectContextProvider } from '../../contexts/ProjectContext';
import ArtifactCanvas, { artifactPath, artifactTabs, useOpenArtifact } from './ArtifactCanvas';
import { INITIAL_PANELS } from './DashboardCanvas';
import type { ArtifactKind } from './ArtifactCanvas';
import { computeEffectiveQuery } from '../../utils/sqlTransformations';
import { getVisualization } from '../../api/visualizations';
import { getDashboard } from '../../api/dashboards';
import { getSavedQuery } from '../../api/savedQueries';
import { getWorkflow } from '../../api/workflows';
import { getPlugin } from '../../api/storage';
import { getProject } from '../../api/projects';
import { executeQuery } from '../../api/queries';
import type { ChatMessage } from '../../types/ai';
import type { Dashboard, DashboardPanel, Project, SavedQuery, StoragePluginDetail, Visualization } from '../../types';
import type { Workflow } from '../../types/workflow';

vi.mock('../../api/visualizations', () => ({ getVisualization: vi.fn() }));
vi.mock('../../api/dashboards', () => ({ getDashboard: vi.fn() }));
vi.mock('../../api/savedQueries', () => ({ getSavedQuery: vi.fn() }));
vi.mock('../../api/workflows', () => ({ getWorkflow: vi.fn() }));
vi.mock('../../api/storage', () => ({ getPlugin: vi.fn() }));
vi.mock('../../api/projects', () => ({ getProject: vi.fn() }));
vi.mock('../../api/queries', () => ({ executeQuery: vi.fn() }));

// ECharts needs a canvas. What matters here is what the canvas hands the chart.
vi.mock('../visualization/ChartPreview', () => ({
  default: (p: { chartType: string; data: { rows: unknown[] } | null; loading?: boolean }) => (
    <div data-testid="chart" data-type={p.chartType} data-loading={String(!!p.loading)}>
      {p.data?.rows.map((r) => JSON.stringify(r)).join(';')}
    </div>
  ),
}));

function httpError(status: number): AxiosError {
  return new AxiosError('failed', String(status), undefined, undefined, { status } as AxiosResponse);
}

const SQL = 'SELECT ts, amt FROM sales';
const VIZ_CONFIG = { xAxis: 'ts', metrics: ['amt'], chartOptions: { timeGrain: 'day' } };

function viz(over: Partial<Visualization> = {}): Visualization {
  return {
    id: 'viz-1', name: 'Sales by day', savedQueryId: 'sq-1', chartType: 'line', config: VIZ_CONFIG,
    owner: 'me', createdAt: '', updatedAt: '1', isPublic: false, sql: SQL, defaultSchema: 'dfs.tmp',
    ...over,
  };
}

function dashboard(panels: DashboardPanel[], over: Partial<Dashboard> = {}): Dashboard {
  return {
    id: 'd1', name: 'Exec view', panels, owner: 'me', createdAt: '', updatedAt: '1',
    refreshInterval: 0, isPublic: false, ...over,
  };
}

function panel(over: Partial<DashboardPanel> & { id: string }): DashboardPanel {
  return { type: 'title', x: 0, y: 0, width: 6, height: 2, ...over };
}

function workflow(over: Partial<Workflow> = {}): Workflow {
  return {
    id: 'w1', projectId: 'p1', name: 'Nightly failures', savedQueryId: 'sq-9', retentionCount: 10,
    schedule: { frequency: 'daily', timeOfDay: '08:00', timeoutSeconds: 60, enabled: true },
    actions: [
      { id: 'notify', type: 'slack', condition: { always: true }, config: {} },
      { id: 'page', type: 'pagerduty', condition: { expr: 'rowCount > 100' }, config: {} },
    ],
    status: 'active',
    ...over,
  };
}

function project(over: Partial<Project> = {}): Project {
  return {
    id: 'p1', name: 'Proj', tags: [], owner: 'me', isPublic: false, sharedWith: [], datasets: [],
    savedQueryIds: [], visualizationIds: [], dashboardIds: [], createdAt: 1, updatedAt: 1,
    wikiPages: [{ id: 'pg1', title: 'Runbook', content: '# Runbook heading\n\nBody text', order: 0, createdAt: 1, updatedAt: 1 }],
    ...over,
  };
}

function wrap(children: ReactNode, client = new QueryClient({ defaultOptions: { queries: { retry: false } } })) {
  const initialPath = '/projects/p1/chat';
  return (
    <ErrorBoundary>
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[initialPath]} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <ProjectContextProvider projectId="p1">{children}</ProjectContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </ErrorBoundary>
  );
}

function show(kind: ArtifactKind, id: string, tab: string, messages: ChatMessage[] = [], client?: QueryClient) {
  return render(wrap(<ArtifactCanvas kind={kind} id={id} tab={tab} messages={messages} />, client));
}

const tool = (id: string, name: string, args: object, result: object): ChatMessage[] => [
  { role: 'assistant', content: null, toolCalls: [{ id, name, arguments: JSON.stringify(args) }] },
  { role: 'tool', content: JSON.stringify(result), toolCallId: id },
];

beforeEach(() => {
  // Not vi.resetAllMocks(): it would also wipe the window.matchMedia stub antd needs.
  [getVisualization, getDashboard, getSavedQuery, getWorkflow, getPlugin, getProject, executeQuery]
    .forEach((fn) => vi.mocked(fn).mockReset());
  vi.mocked(getProject).mockResolvedValue(project());
});

describe('ArtifactCanvas', () => {
  describe('chart', () => {
    it('runs the config-rewritten query, not the raw SQL, and hands the rows to the chart', async () => {
      vi.mocked(getVisualization).mockResolvedValue(viz());
      vi.mocked(executeQuery).mockResolvedValue({
        columns: ['ts', 'amt'], metadata: [], rows: [{ ts: 'Mon', amt: 7 }], queryId: 'q',
      });
      show('chart', 'viz-1', 'chart');

      const chart = await screen.findByTestId('chart');
      await waitFor(() => expect(chart).toHaveTextContent('{"ts":"Mon","amt":7}'));
      expect(chart).toHaveAttribute('data-type', 'line');
      const effective = computeEffectiveQuery(SQL, VIZ_CONFIG);
      expect(effective).not.toBe(SQL);
      expect(executeQuery).toHaveBeenCalledWith(
        expect.objectContaining({ query: effective, defaultSchema: 'dfs.tmp', queryType: 'SQL' }),
      );
    });

    it('says so when the chart query fails instead of showing an empty chart', async () => {
      vi.mocked(getVisualization).mockResolvedValue(viz());
      vi.mocked(executeQuery).mockRejectedValue(new Error('Table not found: sales'));
      show('chart', 'viz-1', 'chart');
      expect(await screen.findByText(/Table not found: sales/)).toBeInTheDocument();
    });

    it('shows the saved SQL on the SQL tab', async () => {
      vi.mocked(getVisualization).mockResolvedValue(viz());
      show('chart', 'viz-1', 'sql');
      expect(await screen.findByText(SQL)).toBeInTheDocument();
      expect(executeQuery).not.toHaveBeenCalled();
    });

    it('renders a deleted-artifact message with a link back when the id does not resolve', async () => {
      vi.mocked(getVisualization).mockRejectedValue(httpError(404));
      show('chart', 'gone', 'chart');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /visualizations/i })).toHaveAttribute('href', '/projects/p1/visualizations');
      expect(screen.queryByText('Something went wrong')).not.toBeInTheDocument();
      expect(executeQuery).not.toHaveBeenCalled();
      // A 404 is the answer, not a glitch to retry.
      expect(getVisualization).toHaveBeenCalledTimes(1);
    });

    it('does not let a cached copy outlive the deletion', async () => {
      // The app caches queries for minutes. A chart fetched earlier must not be shown after
      // it was deleted from its own tab.
      const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
      client.setQueryData(['visualization', 'viz-1'], viz());
      vi.mocked(getVisualization).mockRejectedValue(httpError(404));
      vi.mocked(executeQuery).mockResolvedValue({ columns: [], metadata: [], rows: [], queryId: 'q' });
      show('chart', 'viz-1', 'chart', [], client);
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
    });

    it('reports a failed fetch as a failure, not as a deletion, and offers a retry', async () => {
      vi.mocked(getVisualization).mockRejectedValueOnce(httpError(500)).mockRejectedValueOnce(httpError(500));
      show('chart', 'viz-1', 'chart');
      expect(await screen.findByText(/could not load/i, undefined, { timeout: 4000 })).toBeInTheDocument();
      expect(screen.queryByText('This artifact was deleted')).not.toBeInTheDocument();

      vi.mocked(getVisualization).mockResolvedValue(viz());
      vi.mocked(executeQuery).mockResolvedValue({ columns: [], metadata: [], rows: [], queryId: 'q' });
      fireEvent.click(screen.getByRole('button', { name: /retry/i }));
      expect(await screen.findByTestId('chart')).toBeInTheDocument();
    });
  });

  describe('dashboard', () => {
    it('shows the panels in grid order with none of the editing controls', async () => {
      vi.mocked(getDashboard).mockResolvedValue(dashboard([
        panel({ id: 'b', y: 2, type: 'markdown', content: 'second panel text' }),
        panel({ id: 'a', y: 0, type: 'markdown', content: 'first panel text' }),
      ]));
      show('dashboard', 'd1', 'dashboard');

      const first = await screen.findByText('first panel text');
      const second = screen.getByText('second panel text');
      expect(first.compareDocumentPosition(second) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
      expect(document.querySelector('.drag-handle')).toBeNull();
      expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
      expect(screen.queryByRole('button', { name: /remove|delete/i })).not.toBeInTheDocument();
    });

    it('renders a deleted-artifact message when the dashboard does not resolve', async () => {
      vi.mocked(getDashboard).mockRejectedValue(httpError(404));
      show('dashboard', 'gone', 'dashboard');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /dashboards/i })).toHaveAttribute('href', '/projects/p1/dashboards');
      expect(screen.queryByText('Something went wrong')).not.toBeInTheDocument();
    });

    it('mounts only the first panels of a large dashboard until asked for the rest', async () => {
      const count = INITIAL_PANELS + 1;
      const many = Array.from({ length: count }, (_, i) =>
        panel({ id: 'p' + i, y: i, type: 'markdown', content: 'panel number ' + i }));
      vi.mocked(getDashboard).mockResolvedValue(dashboard(many));
      show('dashboard', 'd1', 'dashboard');

      await screen.findByText('panel number 0');
      expect(screen.getByText(`panel number ${INITIAL_PANELS - 1}`)).toBeInTheDocument();
      expect(screen.queryByText(`panel number ${INITIAL_PANELS}`)).not.toBeInTheDocument();
      fireEvent.click(screen.getByRole('button', { name: new RegExp(`show all ${count} panels`, 'i') }));
      expect(screen.getByText(`panel number ${INITIAL_PANELS}`)).toBeInTheDocument();
    });

    it('says so when the dashboard has no panels', async () => {
      vi.mocked(getDashboard).mockResolvedValue(dashboard([]));
      show('dashboard', 'd1', 'dashboard');
      expect(await screen.findByText(/no panels/i)).toBeInTheDocument();
    });
  });

  describe('saved query', () => {
    const sq: SavedQuery = {
      id: 'sq-1', name: 'Top regions', description: 'Biggest first', sql: 'SELECT region FROM sales',
      owner: 'me', createdAt: '', updatedAt: '', isPublic: false,
    };

    it('shows the name, description and SQL', async () => {
      vi.mocked(getSavedQuery).mockResolvedValue(sq);
      show('query', 'sq-1', 'sql');
      expect(await screen.findByText('SELECT region FROM sales')).toBeInTheDocument();
      expect(screen.getByText('Top regions')).toBeInTheDocument();
      expect(screen.getByText('Biggest first')).toBeInTheDocument();
    });

    it('renders a deleted-artifact message when the query does not resolve', async () => {
      vi.mocked(getSavedQuery).mockRejectedValue(httpError(404));
      show('query', 'gone', 'sql');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /saved queries/i })).toHaveAttribute('href', '/projects/p1/queries');
    });
  });

  describe('workflow', () => {
    it('summarises the schedule, enabled state and each action with its condition', async () => {
      vi.mocked(getWorkflow).mockResolvedValue(workflow());
      show('workflow', 'w1', 'summary');

      expect(await screen.findByText('Nightly failures')).toBeInTheDocument();
      expect(screen.getByText('Daily at 08:00')).toBeInTheDocument();
      expect(screen.getByText('Enabled')).toBeInTheDocument();
      expect(screen.getByText('slack')).toBeInTheDocument();
      expect(screen.getByText('Always')).toBeInTheDocument();
      expect(screen.getByText('pagerduty')).toBeInTheDocument();
      expect(screen.getByText('rowCount > 100')).toBeInTheDocument();
    });

    it('describes weekly and monthly schedules and a paused workflow', async () => {
      vi.mocked(getWorkflow).mockResolvedValue(workflow({
        schedule: { frequency: 'weekly', dayOfWeek: 1, timeOfDay: '06:30', timeoutSeconds: 60, enabled: false },
        status: 'paused',
      }));
      show('workflow', 'w1', 'summary');
      expect(await screen.findByText('Weekly on Monday at 06:30')).toBeInTheDocument();
      expect(screen.getByText('Paused')).toBeInTheDocument();
    });

    it('renders a structured condition tree readably', async () => {
      vi.mocked(getWorkflow).mockResolvedValue(workflow({
        actions: [{
          id: 'a', type: 'email', config: {},
          condition: { tree: { all: [{ field: 'rowCount', op: '>', value: 5 }, { not: { field: 'status', op: '=', value: 'error' } }] } },
        }],
      }));
      show('workflow', 'w1', 'summary');
      expect(await screen.findByText('rowCount > 5 AND NOT (status = "error")')).toBeInTheDocument();
    });

    it('renders a deleted-artifact message when the workflow does not resolve', async () => {
      vi.mocked(getWorkflow).mockRejectedValue(httpError(404));
      show('workflow', 'gone', 'summary');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /workflows/i })).toHaveAttribute('href', '/projects/p1/workflows');
    });
  });

  describe('wiki page', () => {
    it('renders the page markdown from the project', async () => {
      show('wiki', 'pg1', 'page');
      expect(await screen.findByRole('heading', { name: 'Runbook heading' })).toBeInTheDocument();
      expect(screen.getByText('Body text')).toBeInTheDocument();
    });

    it('renders a deleted-artifact message when the project no longer has the page', async () => {
      show('wiki', 'removed', 'page');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /wiki/i })).toHaveAttribute('href', '/projects/p1/wiki');
    });

    it('does not call the page deleted while the project is still loading', async () => {
      vi.mocked(getProject).mockReturnValue(new Promise(() => undefined));
      show('wiki', 'pg1', 'page');
      await act(async () => { await Promise.resolve(); });
      expect(screen.queryByText('This artifact was deleted')).not.toBeInTheDocument();
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
      expect(document.querySelector('.ant-spin')).not.toBeNull();
    });
  });

  describe('data source', () => {
    it('shows the type and enabled state, and never the credentials in the config', async () => {
      vi.mocked(getPlugin).mockResolvedValue({
        name: 'mysql1',
        config: { type: 'jdbc', enabled: true, username: 'root', password: 'hunter2', url: 'jdbc:mysql://h/db?password=hunter2' },
      });
      show('datasource', 'mysql1', 'summary');
      expect(await screen.findByText('mysql1')).toBeInTheDocument();
      expect(screen.getByText('jdbc')).toBeInTheDocument();
      expect(screen.getByText('Enabled')).toBeInTheDocument();
      expect(document.body.textContent).not.toContain('hunter2');
      expect(document.body.textContent).not.toContain('root');
    });

    // The server answers 200 with a null config for a name it does not know, not a 404.
    it('renders a deleted-artifact message when the server answers 200 with no config', async () => {
      vi.mocked(getPlugin).mockResolvedValue({ name: 'gone', config: null } as unknown as StoragePluginDetail);
      show('datasource', 'gone', 'summary');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /data sources/i })).toHaveAttribute('href', '/projects/p1/datasources');
      expect(screen.queryByText('Disabled')).not.toBeInTheDocument();
    });

    it('renders a deleted-artifact message when the plugin does not resolve', async () => {
      vi.mocked(getPlugin).mockRejectedValue(httpError(404));
      show('datasource', 'gone', 'summary');
      expect(await screen.findByText('This artifact was deleted')).toBeInTheDocument();
      expect(screen.getByRole('link', { name: /data sources/i })).toHaveAttribute('href', '/projects/p1/datasources');
    });
  });

  describe('History tab', () => {
    const thread: ChatMessage[] = [
      { role: 'user', content: 'Chart sales by day' },
      ...tool('t1', 'create_visualization', { name: 'Sales by day' }, { id: 'viz-1', name: 'Sales by day' }),
      { role: 'assistant', content: 'Here is the chart.' },
      { role: 'user', content: 'What is in the schema?' },
      ...tool('t2', 'list_schemas', {}, { schemas: ['dfs'] }),
      { role: 'user', content: 'Put it on a dashboard' },
      ...tool('t3', 'create_dashboard', { name: 'Exec', panels: [{ visualizationId: 'viz-1' }] }, { id: 'd9' }),
    ];

    it('lists the turns that touched the artifact, by its result and by being referenced', async () => {
      vi.mocked(getVisualization).mockResolvedValue(viz());
      show('chart', 'viz-1', 'history', thread);
      expect(await screen.findByText('Chart sales by day')).toBeInTheDocument();
      expect(screen.getByText('Put it on a dashboard')).toBeInTheDocument();
      expect(screen.queryByText('What is in the schema?')).not.toBeInTheDocument();
    });

    it('says so when nothing in the conversation touched it', async () => {
      vi.mocked(getVisualization).mockResolvedValue(viz({ id: 'other' }));
      show('chart', 'other', 'history', thread);
      expect(await screen.findByText(/no turns in this conversation/i)).toBeInTheDocument();
    });

    it('is available for an artifact that has since been deleted', async () => {
      vi.mocked(getVisualization).mockRejectedValue(httpError(404));
      show('chart', 'viz-1', 'history', thread);
      expect(await screen.findByText('Chart sales by day')).toBeInTheDocument();
    });
  });
});

describe('tab sets', () => {
  it.each([
    ['chart', ['chart', 'sql', 'history']],
    ['dashboard', ['dashboard', 'history']],
    ['query', ['sql', 'history']],
    ['workflow', ['summary', 'history']],
    ['wiki', ['page', 'history']],
    ['datasource', ['summary', 'history']],
  ] as [ArtifactKind, string[]][])('%s declares its tabs', (kind, keys) => {
    expect(artifactTabs(kind).map((t) => t.key)).toEqual(keys);
  });
});

describe('artifactPath', () => {
  it.each([
    ['chart', 'v 1', '/projects/p1/visualizations/v%201'],
    ['dashboard', 'd1', '/projects/p1/dashboards/d1'],
    ['workflow', 'w1', '/projects/p1/workflows/w1'],
    ['wiki', 'pg1', '/projects/p1/wiki/pg1'],
    ['datasource', 'my src', '/projects/p1/datasources/my%20src'],
    ['query', 'sq-1', '/projects/p1/query'],
  ] as [ArtifactKind, string, string][])('sends %s to its own editor route', (kind, id, path) => {
    expect(artifactPath('p1', kind, id)).toBe(path);
  });
});

describe('useOpenArtifact', () => {
  function openHook() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={['/projects/p1/chat']} future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          {children}
        </MemoryRouter>
      </QueryClientProvider>
    );
    return renderHook(() => ({ open: useOpenArtifact('p1'), location: useLocation() }), { wrapper });
  }

  it('navigates to the artifact route rather than embedding an editor', async () => {
    const { result } = openHook();
    await act(async () => { await result.current.open('dashboard', 'd1'); });
    expect(result.current.location.pathname).toBe('/projects/p1/dashboards/d1');
  });

  it('opens a saved query in the query editor with the query loaded', async () => {
    const sq = { id: 'sq-1', name: 'Q', sql: 'SELECT 1' } as SavedQuery;
    vi.mocked(getSavedQuery).mockResolvedValue(sq);
    const { result } = openHook();
    await act(async () => { await result.current.open('query', 'sq-1'); });
    expect(result.current.location.pathname).toBe('/projects/p1/query');
    expect(result.current.location.state).toEqual({ loadQuery: sq });
  });

  it('falls back to the saved queries list when the query cannot be loaded', async () => {
    vi.mocked(getSavedQuery).mockRejectedValue(httpError(404));
    const { result } = openHook();
    await act(async () => { await result.current.open('query', 'gone'); });
    expect(result.current.location.pathname).toBe('/projects/p1/queries');
  });
});
