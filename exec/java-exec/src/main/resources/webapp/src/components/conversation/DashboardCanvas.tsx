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
import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Button, Empty, Spin, Typography } from 'antd';
import ChartPreview from '../visualization/ChartPreview';
import { getVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import { getEffectiveQuery } from '../../utils/sqlTransformations';
import { useTheme } from '../../hooks/useTheme';
import type { Dashboard, DashboardPanel } from '../../types';

/**
 * How many tiles mount before the rest are asked for. Every chart tile runs its own query,
 * so a dashboard with dozens would fire them all at once beside the chat.
 */
export const INITIAL_PANELS = 8;

/** The drawing area of a tile. Enough to recognise a shape, not to read a value off it. */
const TILE_CHART_HEIGHT = 84;

const byPosition = (a: DashboardPanel, b: DashboardPanel) => a.y - b.y || a.x - b.x;

const PANEL_LABELS: Record<string, string> = {
  executiveSummary: 'AI summary',
  markdown: 'Note',
  image: 'Image',
  title: 'Title',
  aiQnA: 'AI Q&A',
  aiAlerts: 'AI alerts',
  nlFilter: 'Filter',
};

/**
 * One chart, small. The thumbnail is ChartPreview's own mini mode, which drops the axes,
 * legend and tooltip, so what is left is the shape of the data.
 */
function ChartTile({ id, onOpen }: { id: string; onOpen?: () => void }) {
  const { isDark } = useTheme();
  const vizQuery = useQuery({
    queryKey: ['visualization', id],
    queryFn: () => getVisualization(id),
    staleTime: 60_000,
    retry: false,
  });
  const viz = vizQuery.data;

  const dataQuery = useQuery({
    queryKey: ['canvas-chart-data', id, viz?.updatedAt, viz?.sql],
    queryFn: async () => {
      const query = await getEffectiveQuery(viz!.sql!, viz!.config || {});
      return executeQuery({
        query,
        queryType: 'SQL',
        // A thumbnail needs a shape, not every row.
        autoLimitRowCount: 2000,
        defaultSchema: viz!.defaultSchema,
      });
    },
    enabled: !!viz?.sql?.trim(),
    staleTime: 30_000,
    retry: false,
  });

  let body;
  if (vizQuery.isError) {
    body = <Typography.Text type="secondary" style={{ fontSize: 11 }}>Chart unavailable</Typography.Text>;
  } else if (dataQuery.isError) {
    body = <Typography.Text type="secondary" style={{ fontSize: 11 }}>Query failed</Typography.Text>;
  } else if (!viz || dataQuery.isLoading) {
    body = <Spin size="small" />;
  } else {
    body = (
      <ChartPreview
        mini
        chartType={viz.chartType}
        config={viz.config}
        data={dataQuery.data ?? null}
        height={TILE_CHART_HEIGHT}
        darkMode={isDark}
      />
    );
  }

  return (
    <Tile title={viz?.name ?? 'Loading…'} onOpen={onOpen}>
      <div style={{
        height: TILE_CHART_HEIGHT,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        overflow: 'hidden',
      }}>
        {body}
      </div>
    </Tile>
  );
}

/** A panel that is not a chart: there is nothing to draw, so it says what it is. */
function OtherTile({ panel }: { panel: DashboardPanel }) {
  const label = PANEL_LABELS[panel.type] ?? panel.type;
  return (
    <Tile title={label}>
      <div style={{ height: TILE_CHART_HEIGHT, overflow: 'hidden' }}>
        <Typography.Paragraph
          type="secondary"
          style={{ fontSize: 11, margin: 0 }}
          ellipsis={{ rows: 4 }}
        >
          {panel.content || 'Written when the dashboard is opened.'}
        </Typography.Paragraph>
      </div>
    </Tile>
  );
}

function Tile({ title, onOpen, children }: { title: string; onOpen?: () => void; children: React.ReactNode }) {
  return (
    <div
      role={onOpen ? 'button' : undefined}
      tabIndex={onOpen ? 0 : undefined}
      onClick={onOpen}
      onKeyDown={onOpen ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen(); } } : undefined}
      style={{
        border: '1px solid var(--color-border, #f0f0f0)',
        borderRadius: 6,
        padding: 8,
        cursor: onOpen ? 'pointer' : 'default',
        background: 'var(--color-bg-container, transparent)',
      }}
    >
      <Typography.Text ellipsis style={{ display: 'block', fontSize: 12, fontWeight: 500, marginBottom: 4 }}>
        {title}
      </Typography.Text>
      {children}
    </div>
  );
}

/**
 * A dashboard beside the chat, as a contact sheet rather than a dashboard: the canvas is a
 * few hundred pixels wide, where a real dashboard is unreadable however it is scaled. Each
 * panel becomes a thumbnail, so the user can see what was built and recognise each chart;
 * clicking one puts that chart on the canvas at full width, where it can actually be read.
 * The dashboard itself is a full-screen hand-off to its own page, as for every artifact.
 */
export default function DashboardCanvas({
  dashboard,
  onOpenChart,
}: {
  dashboard: Dashboard;
  /** Puts one of the dashboard's charts on the canvas. Without it the tiles are inert. */
  onOpenChart?: (visualizationId: string) => void;
}) {
  const [showAll, setShowAll] = useState(false);

  // Tabs flatten into sections in tab order, root panels first, so nothing is hidden
  // just because the canvas has no tab strip for it.
  const ordered = useMemo(() => {
    const tabs = [...(dashboard.tabs ?? [])].sort((a, b) => a.order - b.order);
    const known = new Set(tabs.map((t) => t.id));
    const root = dashboard.panels.filter((p) => !p.tabId || !known.has(p.tabId)).sort(byPosition);
    const sections = tabs.map((t) => ({
      name: t.name,
      panels: dashboard.panels.filter((p) => p.tabId === t.id).sort(byPosition),
    }));
    return [{ name: '', panels: root }, ...sections].filter((s) => s.panels.length > 0);
  }, [dashboard.panels, dashboard.tabs]);

  const total = dashboard.panels.length;
  if (total === 0) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="This dashboard has no panels." style={{ marginTop: 48 }} />;
  }

  let budget = showAll ? Infinity : INITIAL_PANELS;
  return (
    <div style={{ padding: 12, height: '100%', overflow: 'auto' }}>
      <Typography.Title level={5} style={{ marginTop: 0, marginBottom: 2 }}>{dashboard.name}</Typography.Title>
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        {total} {total === 1 ? 'panel' : 'panels'} · open a tile to read it
      </Typography.Text>
      {ordered.map((section) => {
        const shown = section.panels.slice(0, Math.max(budget, 0));
        budget -= shown.length;
        return shown.length === 0 ? null : (
          <div key={section.name || 'root'}>
            {section.name && <Typography.Title level={5}>{section.name}</Typography.Title>}
            <div style={{
              display: 'grid',
              // Two across a narrow canvas, more as the user drags it wider.
              gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))',
              gap: 8,
              marginTop: 8,
            }}>
              {shown.map((panel) => (
                panel.type === 'visualization' && panel.visualizationId ? (
                  <ChartTile
                    key={panel.id}
                    id={panel.visualizationId}
                    onOpen={onOpenChart ? () => onOpenChart(panel.visualizationId!) : undefined}
                  />
                ) : (
                  <OtherTile key={panel.id} panel={panel} />
                )
              ))}
            </div>
          </div>
        );
      })}
      {!showAll && total > INITIAL_PANELS && (
        <Button block style={{ marginTop: 8 }} onClick={() => setShowAll(true)}>
          Show all {total} panels
        </Button>
      )}
    </div>
  );
}
