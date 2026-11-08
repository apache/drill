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
import { useCallback } from 'react';
import type { ReactNode } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { UseQueryResult } from '@tanstack/react-query';
import axios from 'axios';
import { Alert, Button, Descriptions, Empty, Spin, Tag, Typography } from 'antd';
import { useProjectContext } from '../../contexts/ProjectContext';
import { useTheme } from '../../hooks/useTheme';
import { getVisualization } from '../../api/visualizations';
import { getDashboard } from '../../api/dashboards';
import { getSavedQuery } from '../../api/savedQueries';
import { getWorkflow } from '../../api/workflows';
import { getPlugin } from '../../api/storage';
import { executeQuery } from '../../api/queries';
import { getEffectiveQuery } from '../../utils/sqlTransformations';
import ChartPreview from '../visualization/ChartPreview';
import MarkdownView from '../MarkdownView';
import ArtifactHistory from './ArtifactHistory';
import DashboardCanvas from './DashboardCanvas';
import SqlBlock from './SqlBlock';
import WorkflowSummary from './WorkflowSummary';
import type { ChatMessage } from '../../types/ai';

export type ArtifactKind = 'chart' | 'dashboard' | 'query' | 'workflow' | 'wiki' | 'datasource';

const HISTORY = { key: 'history', label: 'History' };

/**
 * The secondary tabs each kind supports. Fixed per kind: CanvasHost remembers the chosen tab
 * by kind without checking it against this set, which is only sound while the set never
 * varies with the artifact. A kind whose tabs depend on the artifact needs that revisited.
 */
const TABS: Record<ArtifactKind, { key: string; label: string }[]> = {
  chart: [{ key: 'chart', label: 'Chart' }, { key: 'sql', label: 'SQL' }, HISTORY],
  dashboard: [{ key: 'dashboard', label: 'Dashboard' }, HISTORY],
  query: [{ key: 'sql', label: 'SQL' }, HISTORY],
  workflow: [{ key: 'summary', label: 'Summary' }, HISTORY],
  wiki: [{ key: 'page', label: 'Page' }, HISTORY],
  datasource: [{ key: 'summary', label: 'Summary' }, HISTORY],
};

export const artifactTabs = (kind: ArtifactKind) => TABS[kind];

const LIST_ROUTE: Record<ArtifactKind, { segment: string; label: string }> = {
  chart: { segment: 'visualizations', label: 'visualizations' },
  dashboard: { segment: 'dashboards', label: 'dashboards' },
  query: { segment: 'queries', label: 'saved queries' },
  workflow: { segment: 'workflows', label: 'workflows' },
  wiki: { segment: 'wiki', label: 'wiki' },
  datasource: { segment: 'datasources', label: 'data sources' },
};

/**
 * Where the artifact lives in the app: the route that edits it. A saved query has no page of
 * its own, so it opens in the query editor, which useOpenArtifact loads it into.
 */
export function artifactPath(projectId: string, kind: ArtifactKind, id: string): string {
  const base = `/projects/${projectId}`;
  const enc = encodeURIComponent(id);
  switch (kind) {
    case 'chart': return `${base}/visualizations/${enc}`;
    case 'dashboard': return `${base}/dashboards/${enc}`;
    case 'workflow': return `${base}/workflows/${enc}`;
    case 'wiki': return `${base}/wiki/${enc}`;
    case 'datasource': return `${base}/datasources/${enc}`;
    case 'query': return `${base}/query`;
    default: return base;
  }
}

/**
 * Full screen for an artifact is a hand-off to its own page, not an embedded editor. A saved
 * query goes to the query editor with the query loaded; if it cannot be fetched (deleted
 * meanwhile) the user lands on the saved queries list instead of an empty editor.
 */
export function useOpenArtifact(projectId: string) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  return useCallback(async (kind: ArtifactKind, id: string) => {
    if (kind === 'query') {
      try {
        const loadQuery = await queryClient.fetchQuery({
          queryKey: ['saved-query', id],
          queryFn: () => getSavedQuery(id),
          staleTime: 30_000,
        });
        navigate(artifactPath(projectId, kind, id), { state: { loadQuery } });
      } catch {
        navigate(`/projects/${projectId}/${LIST_ROUTE.query.segment}`);
      }
      return;
    }
    navigate(artifactPath(projectId, kind, id));
  }, [projectId, navigate, queryClient]);
}

const isNotFound = (err: unknown) => axios.isAxiosError(err) && err.response?.status === 404;

/**
 * An artifact fetch. A 404 means the id no longer resolves (the thread still holds ids of
 * things deleted from their own tab), so it is not retried; anything else gets one retry.
 * Always refetched on mount: a cached copy must not outlive a deletion.
 */
function useArtifact<T>(queryKey: unknown[], queryFn: () => Promise<T>): UseQueryResult<T> {
  return useQuery({
    queryKey,
    queryFn,
    staleTime: 0,
    retry: (count, err) => !isNotFound(err) && count < 1,
  });
}

function Deleted({ kind }: { kind: ArtifactKind }) {
  const { projectId } = useProjectContext();
  const list = LIST_ROUTE[kind];
  return (
    <Empty
      image={Empty.PRESENTED_IMAGE_SIMPLE}
      description="This artifact was deleted"
      style={{ marginTop: 48 }}
    >
      <Link to={`/projects/${projectId}/${list.segment}`}>Back to {list.label}</Link>
    </Empty>
  );
}

const centered = (child: ReactNode) => <div style={{ padding: 24, textAlign: 'center' }}>{child}</div>;

/** Loading, deleted and failed states for a fetched artifact; `children` runs once it is there. */
function Resolved<T>({ kind, query, children }: {
  kind: ArtifactKind;
  query: UseQueryResult<T>;
  children: (data: T) => ReactNode;
}) {
  if (isNotFound(query.error)) {
    return <Deleted kind={kind} />;
  }
  if (query.data !== undefined) {
    return <>{children(query.data)}</>;
  }
  if (query.isError) {
    return (
      <div style={{ padding: 12 }}>
        <Alert
          type="error"
          showIcon
          message="Could not load this artifact."
          action={<Button size="small" onClick={() => query.refetch()}>Retry</Button>}
        />
      </div>
    );
  }
  return centered(<Spin />);
}

function Titled({ title, description, children }: { title: string; description?: string; children: ReactNode }) {
  return (
    <div style={{ padding: 12, height: '100%', overflow: 'auto' }}>
      <Typography.Title level={5} style={{ marginTop: 0 }}>{title}</Typography.Title>
      {description && <Typography.Paragraph type="secondary">{description}</Typography.Paragraph>}
      {children}
    </div>
  );
}

const CHART_HEIGHT = 360;

function ChartViewer({ id, tab }: { id: string; tab: string }) {
  const { isDark } = useTheme();
  const vizQuery = useArtifact(['visualization', id], () => getVisualization(id));
  const viz = isNotFound(vizQuery.error) ? undefined : vizQuery.data;

  // The saved SQL goes through the config's rewrite (time grain, aggregation) before it runs,
  // as on the visualization page: the raw SQL would draw a different chart. The result is
  // cached for a short while, keyed by what could change it, so flipping tabs or coming back
  // to the chart does not re-run the query every time.
  const dataQuery = useQuery({
    queryKey: ['canvas-chart-data', id, viz?.updatedAt, viz?.sql],
    queryFn: async () => {
      const query = await getEffectiveQuery(viz!.sql!, viz!.config || {});
      return executeQuery({
        query,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: viz!.defaultSchema || 'public',
      });
    },
    enabled: tab === 'chart' && !!viz?.sql?.trim(),
    staleTime: 30_000,
    retry: false,
  });

  return (
    <Resolved kind="chart" query={vizQuery}>
      {(v) => {
        if (tab === 'sql') {
          return <SqlBlock sql={v.sql} />;
        }
        let body: ReactNode;
        if (!v.sql?.trim()) {
          body = <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="This chart has no query." />;
        } else if (dataQuery.isError) {
          body = (
            <Alert
              type="error"
              showIcon
              message="The chart's query failed."
              description={dataQuery.error instanceof Error ? dataQuery.error.message : 'Failed to run query'}
            />
          );
        } else {
          body = (
            <ChartPreview
              chartType={v.chartType}
              config={v.config}
              data={dataQuery.data ?? null}
              loading={dataQuery.isLoading}
              height={CHART_HEIGHT}
              sql={v.sql}
              darkMode={isDark}
            />
          );
        }
        return <Titled title={v.name} description={v.description}>{body}</Titled>;
      }}
    </Resolved>
  );
}

function DashboardViewer({ id }: { id: string }) {
  const query = useArtifact(['dashboard', id], () => getDashboard(id));
  return <Resolved kind="dashboard" query={query}>{(d) => <DashboardCanvas dashboard={d} />}</Resolved>;
}

function QueryViewer({ id }: { id: string }) {
  const query = useArtifact(['saved-query', id], () => getSavedQuery(id));
  return (
    <Resolved kind="query" query={query}>
      {(q) => (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
          <div style={{ padding: '12px 12px 0' }}>
            <Typography.Title level={5} style={{ marginTop: 0 }}>{q.name}</Typography.Title>
            {q.description && <Typography.Paragraph type="secondary">{q.description}</Typography.Paragraph>}
          </div>
          <div style={{ flex: 1, minHeight: 0 }}><SqlBlock sql={q.sql} /></div>
        </div>
      )}
    </Resolved>
  );
}

function WorkflowViewer({ id }: { id: string }) {
  const query = useArtifact(['workflow', id], () => getWorkflow(id));
  return <Resolved kind="workflow" query={query}>{(wf) => <WorkflowSummary workflow={wf} />}</Resolved>;
}

/** Wiki pages live on the project record, so this reads them from the project, not a page endpoint. */
function WikiViewer({ id }: { id: string }) {
  const { project, isLoading, error } = useProjectContext();
  if (isLoading) {
    return centered(<Spin />);
  }
  if (error || !project) {
    return (
      <div style={{ padding: 12 }}>
        <Alert type="error" showIcon message="Could not load this artifact." />
      </div>
    );
  }
  const page = project.wikiPages?.find((p) => p.id === id);
  if (!page) {
    return <Deleted kind="wiki" />;
  }
  return (
    <Titled title={page.title}>
      <MarkdownView allowHtml>{page.content}</MarkdownView>
    </Titled>
  );
}

/**
 * Only the type, enabled state and workspace names. A plugin config holds connection
 * strings and credentials; a summary beside a chat has no business showing them.
 */
function DataSourceViewer({ id }: { id: string }) {
  const query = useArtifact(['storage-plugin', id], () => getPlugin(id));
  return (
    <Resolved kind="datasource" query={query}>
      {(plugin) => {
        const config = plugin.config ?? {};
        const workspaces = config.workspaces && typeof config.workspaces === 'object'
          ? Object.keys(config.workspaces as Record<string, unknown>)
          : [];
        return (
          <Titled title={plugin.name}>
            <Descriptions size="small" column={1} bordered>
              <Descriptions.Item label="Type">{String(config.type ?? 'unknown')}</Descriptions.Item>
              <Descriptions.Item label="Status">
                <Tag color={config.enabled ? 'green' : 'default'}>{config.enabled ? 'Enabled' : 'Disabled'}</Tag>
              </Descriptions.Item>
              {workspaces.length > 0 && (
                <Descriptions.Item label="Workspaces">{workspaces.join(', ')}</Descriptions.Item>
              )}
            </Descriptions>
          </Titled>
        );
      }}
    </Resolved>
  );
}

interface ArtifactCanvasProps {
  kind: ArtifactKind;
  id: string;
  /** The active secondary tab, one of artifactTabs(kind). */
  tab: string;
  /** The conversation so far; History is read from it, as nothing versions artifacts. */
  messages?: ChatMessage[];
}

/**
 * A project artifact on the canvas, read-only. Each kind mounts the component the app already
 * uses for it where one exists (chart preview, dashboard panel cards, markdown view) and a
 * small summary where none does (workflow, data source). An id that no longer resolves shows
 * a deleted notice rather than throwing, since the thread outlives the artifacts it names.
 */
export default function ArtifactCanvas({ kind, id, tab, messages = [] }: ArtifactCanvasProps) {
  if (tab === 'history') {
    return <ArtifactHistory messages={messages} id={id} />;
  }
  switch (kind) {
    case 'chart': return <ChartViewer id={id} tab={tab} />;
    case 'dashboard': return <DashboardViewer id={id} />;
    case 'query': return <QueryViewer id={id} />;
    case 'workflow': return <WorkflowViewer id={id} />;
    case 'wiki': return <WikiViewer id={id} />;
    case 'datasource': return <DataSourceViewer id={id} />;
    default: return null;
  }
}
