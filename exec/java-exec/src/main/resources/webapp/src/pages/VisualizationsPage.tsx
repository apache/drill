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
import { useState, useMemo, useCallback, useEffect, useRef } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  Input,
  Button,
  Space,
  Tag,
  Popconfirm,
  Tooltip,
  Spin,
  Modal,
  Alert,
  Dropdown,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  AreaChartOutlined,
  BarChartOutlined,
  LineChartOutlined,
  PieChartOutlined,
  DotChartOutlined,
  TableOutlined,
  HeatMapOutlined,
  FundOutlined,
  DashboardOutlined,
  FilterOutlined,
  GlobalOutlined as MapOutlined,
  FieldNumberOutlined,
  PlayCircleOutlined,
  CodeOutlined,
  BranchesOutlined,
  RadarChartOutlined,
  ExperimentOutlined,
  RiseOutlined,
  SunOutlined,
  StockOutlined,
  CalendarOutlined,
  ClusterOutlined,
  ApartmentOutlined,
  AppstoreOutlined,
  FolderOutlined,
  CheckOutlined,
  MoreOutlined,
  PlusOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { getVisualizations, deleteVisualization, restoreVisualization } from '../api/visualizations';
import { useUndoableDelete } from '../hooks/useUndoableDelete';
import { executeQuery } from '../api/queries';
import { getEffectiveQuery } from '../utils/sqlTransformations';
import { ChartPreview, VisualizationEditor } from '../components/visualization';
import { useCurrentUser } from '../hooks/useCurrentUser';
import { usePageChrome } from '../contexts/AppChromeContext';
import type { Visualization, ChartType, QueryResult } from '../types';
import AddToProjectModal from '../components/common/AddToProjectModal';
import BulkAddToProjectModal from '../components/common/BulkAddToProjectModal';

const chartIcons: Record<ChartType, React.ReactNode> = {
  area: <AreaChartOutlined />,
  bar: <BarChartOutlined />,
  line: <LineChartOutlined />,
  pie: <PieChartOutlined />,
  scatter: <DotChartOutlined />,
  table: <TableOutlined />,
  pivot: <AppstoreOutlined />,
  heatmap: <HeatMapOutlined />,
  treemap: <FundOutlined />,
  gauge: <DashboardOutlined />,
  funnel: <FilterOutlined />,
  map: <MapOutlined />,
  choropleth: <GlobalOutlined />,
  bigNumber: <FieldNumberOutlined />,
  sankey: <BranchesOutlined />,
  radar: <RadarChartOutlined />,
  boxplot: <ExperimentOutlined />,
  waterfall: <RiseOutlined />,
  sunburst: <SunOutlined />,
  candlestick: <StockOutlined />,
  calendar: <CalendarOutlined />,
  bubble: <ClusterOutlined />,
  parallel: <ApartmentOutlined />,
};

const chartColors: Record<ChartType, string> = {
  area: '#73c0de',
  bar: '#5470c6',
  line: '#91cc75',
  pie: '#fac858',
  scatter: '#ee6666',
  table: '#73c0de',
  pivot: '#5470c6',
  heatmap: '#3ba272',
  treemap: '#fc8452',
  gauge: '#9a60b4',
  funnel: '#ea7ccc',
  map: '#3b82f6',
  choropleth: '#10b981',
  bigNumber: '#ff7a45',
  sankey: '#5470c6',
  radar: '#91cc75',
  boxplot: '#3ba272',
  waterfall: '#52c41a',
  sunburst: '#fac858',
  candlestick: '#52c41a',
  calendar: '#722ed1',
  bubble: '#73c0de',
  parallel: '#9a60b4',
};

function formatRelative(ts: number | string): string {
  const date = typeof ts === 'number' ? new Date(ts) : new Date(ts);
  const diff = Date.now() - date.getTime();
  const min = Math.floor(diff / 60000);
  if (min < 60) {
    return min < 1 ? 'just now' : `${min}m ago`;
  }
  const hr = Math.floor(min / 60);
  if (hr < 24) {
    return `${hr}h ago`;
  }
  const days = Math.floor(hr / 24);
  if (days < 7) {
    return `${days}d ago`;
  }
  if (days < 30) {
    return `${Math.floor(days / 7)}w ago`;
  }
  return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

/**
 * Mini chart preview that runs the viz SQL and renders a thumbnail.
 * Falls back to a tinted icon glyph when no SQL or on error.
 */
function MiniVizPreview({ viz, height = 180 }: { viz: Visualization; height?: number }) {
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);

  const configKey = JSON.stringify(viz.config || {});

  useEffect(() => {
    if (!viz.sql) {
      setFailed(true);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setFailed(false);

    getEffectiveQuery(viz.sql, JSON.parse(configKey))
      .then((query) =>
        executeQuery({
          query,
          queryType: 'SQL',
          autoLimitRowCount: 100,
          defaultSchema: viz.defaultSchema,
        }),
      )
      .then((result) => {
        if (!cancelled && result) {
          setQueryResult(result);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setFailed(true);
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [viz.id, viz.sql, viz.defaultSchema, configKey]);

  if (failed || (!loading && !queryResult)) {
    const tint = chartColors[viz.chartType] || '#5470c6';
    return (
      <div
        className="viz-tile-fallback"
        style={{
          height,
          background: `linear-gradient(135deg, ${tint}30 0%, ${tint}18 100%)`,
        }}
      >
        <span className="viz-tile-fallback-icon" style={{ color: tint }}>
          {chartIcons[viz.chartType] || <BarChartOutlined />}
        </span>
      </div>
    );
  }

  return (
    <div className="viz-tile-chart" style={{ height }}>
      <ChartPreview
        chartType={viz.chartType as ChartType}
        config={viz.config}
        data={queryResult}
        loading={loading}
        height={height}
        mini
      />
    </div>
  );
}

interface VisualizationsPageProps {
  filterIds?: string[];
  projectId?: string;
  projectOwner?: string;
  onAdd?: () => void;
}

export default function VisualizationsPage({ filterIds, projectId, projectOwner, onAdd }: VisualizationsPageProps = {}) {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const navState = location.state as { viewVizId?: string } | null;
  const autoOpenDoneRef = useRef(false);
  const { canEdit, canView, isProjectOwner } = useCurrentUser();
  const [searchText, setSearchText] = useState('');
  const [chartTypeFilter, setChartTypeFilter] = useState<ChartType | 'all'>('all');
  const [editBuilderViz, setEditBuilderViz] = useState<Visualization | null>(null);
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewingViz, setViewingViz] = useState<Visualization | null>(null);
  const [viewQueryResult, setViewQueryResult] = useState<QueryResult | null>(null);
  const [viewQueryLoading, setViewQueryLoading] = useState(false);
  const [viewQueryError, setViewQueryError] = useState<string | null>(null);
  const [showSql, setShowSql] = useState(false);
  const [addToProjectId, setAddToProjectId] = useState<string | null>(null);
  const [bulkSelected, setBulkSelected] = useState<Set<string>>(new Set());
  const [bulkProjectModalOpen, setBulkProjectModalOpen] = useState(false);

  const { data: visualizations, isLoading, error } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
  });

  const undoable = useUndoableDelete();

  const deleteVizWithUndo = (viz: Visualization) => undoable.run({
    label: `Deleted "${viz.name}"`,
    capture: () => viz,
    remove: () => deleteVisualization(viz.id),
    restore: (snap) => restoreVisualization(snap.id),
    onDeleted: () => queryClient.invalidateQueries({ queryKey: ['visualizations'] }),
    onRestored: () => queryClient.invalidateQueries({ queryKey: ['visualizations'] }),
  });

  const bulkDeleteVizWithUndo = (ids: string[]) => {
    if (ids.length === 0) {
      return;
    }
    undoable.run<string[]>({
      label: `Deleted ${ids.length} ${ids.length === 1 ? 'visualization' : 'visualizations'}`,
      capture: () => [...ids],
      remove: async () => { await Promise.all(ids.map((id) => deleteVisualization(id))); },
      restore: async (snap) => { await Promise.all(snap.map((id) => restoreVisualization(id))); },
      onDeleted: () => queryClient.invalidateQueries({ queryKey: ['visualizations'] }),
      onRestored: () => queryClient.invalidateQueries({ queryKey: ['visualizations'] }),
    });
  };

  // Visible after permission + project filter (before chart-type/search filtering — used for chip computation)
  const visibleVisualizations = useMemo(() => {
    if (!visualizations) {
      return [];
    }
    let result = visualizations.filter((v) => canView(v.owner, v.isPublic));
    if (filterIds) {
      const idSet = new Set(filterIds);
      result = result.filter((v) => idSet.has(v.id));
    }
    return result;
  }, [visualizations, filterIds, canView]);

  // Chart type chips (only for types actually present in this view)
  const presentChartTypes = useMemo(() => {
    const types = new Set<ChartType>();
    visibleVisualizations.forEach((v) => types.add(v.chartType as ChartType));
    return Array.from(types).sort();
  }, [visibleVisualizations]);

  const filteredVisualizations = useMemo(() => {
    let result = visibleVisualizations;
    if (chartTypeFilter !== 'all') {
      result = result.filter((v) => v.chartType === chartTypeFilter);
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (v) =>
          v.name.toLowerCase().includes(lower) ||
          v.chartType.toLowerCase().includes(lower) ||
          (v.description && v.description.toLowerCase().includes(lower)),
      );
    }
    return [...result].sort((a, b) => {
      const at = typeof a.updatedAt === 'number' ? a.updatedAt : new Date(a.updatedAt).getTime();
      const bt = typeof b.updatedAt === 'number' ? b.updatedAt : new Date(b.updatedAt).getTime();
      return bt - at;
    });
  }, [visibleVisualizations, chartTypeFilter, searchText]);

  const handleView = useCallback(async (viz: Visualization) => {
    setViewingViz(viz);
    setViewModalOpen(true);
    setViewQueryResult(null);
    setViewQueryError(null);
    setShowSql(false);

    if (!viz.sql) {
      setViewQueryError('This visualization has no saved SQL query. It may have been created before SQL storage was supported.');
      return;
    }

    setViewQueryLoading(true);
    try {
      const query = await getEffectiveQuery(viz.sql, viz.config || {});
      const result = await executeQuery({
        query,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: viz.defaultSchema,
      });
      setViewQueryResult(result);
    } catch (err) {
      setViewQueryError(`Failed to execute query: ${(err as Error).message}`);
    } finally {
      setViewQueryLoading(false);
    }
  }, []);

  // Auto-open visualization when navigated with viewVizId state
  useEffect(() => {
    if (autoOpenDoneRef.current || !navState?.viewVizId || !visualizations) {
      return;
    }
    const target = visualizations.find((v) => v.id === navState.viewVizId);
    if (target) {
      autoOpenDoneRef.current = true;
      handleView(target);
      window.history.replaceState({}, '', location.pathname + location.search);
    }
  }, [visualizations, navState?.viewVizId, handleView, location.pathname, location.search]);

  const closeView = () => {
    setViewModalOpen(false);
    setViewingViz(null);
    setViewQueryResult(null);
    setViewQueryError(null);
    setShowSql(false);
  };

  const toggleBulk = (id: string) => {
    setBulkSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  // Toolbar actions registered in the unified shell toolbar
  const toolbarActions = useMemo(
    () => (
      <Space size={4}>
        {onAdd && (
          <Tooltip title="Add existing">
            <Button type="text" size="small" icon={<PlusOutlined />} onClick={onAdd} />
          </Tooltip>
        )}
        <Button
          type="primary"
          size="small"
          icon={<PlusOutlined />}
          onClick={() => navigate(projectId ? `/projects/${projectId}/query` : '/query')}
        >
          New Visualization
        </Button>
      </Space>
    ),
    [navigate, onAdd, projectId],
  );
  usePageChrome({ toolbarActions });

  if (error) {
    return (
      <div className="page-visualizations-error">
        <h2>Couldn't load visualizations</h2>
        <p>{(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="page-visualizations">
      {undoable.contextHolder}
      <header className="page-visualizations-header">
        <div>
          <h1 className="page-visualizations-title">Visualizations</h1>
          <p className="page-visualizations-subtitle">
            {visualizations === undefined
              ? 'Loading…'
              : visibleVisualizations.length === 0
                ? 'No visualizations yet'
                : filteredVisualizations.length === visibleVisualizations.length
                  ? `${visibleVisualizations.length} ${visibleVisualizations.length === 1 ? 'item' : 'items'}`
                  : `Showing ${filteredVisualizations.length} of ${visibleVisualizations.length}`}
          </p>
        </div>
      </header>

      <div className="page-visualizations-toolbar">
        <Input
          placeholder="Search visualizations…"
          prefix={<SearchOutlined style={{ color: 'var(--color-text-tertiary)' }} />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          className="page-visualizations-search"
        />

        {presentChartTypes.length > 1 && (
          <div className="page-visualizations-chips">
            <button
              type="button"
              className={`page-visualizations-chip${chartTypeFilter === 'all' ? ' is-active' : ''}`}
              onClick={() => setChartTypeFilter('all')}
            >
              All
            </button>
            {presentChartTypes.map((t) => (
              <button
                key={t}
                type="button"
                className={`page-visualizations-chip${chartTypeFilter === t ? ' is-active' : ''}`}
                onClick={() => setChartTypeFilter(t)}
                style={chartTypeFilter === t ? undefined : undefined}
              >
                <span className="page-visualizations-chip-icon" style={{ color: chartColors[t] }}>
                  {chartIcons[t]}
                </span>
                <span style={{ textTransform: 'capitalize' }}>{t}</span>
              </button>
            ))}
          </div>
        )}

        {bulkSelected.size > 0 && (
          <div className="page-visualizations-bulkbar">
            <span>{bulkSelected.size} selected</span>
            {!projectId && (
              <Button size="small" onClick={() => setBulkProjectModalOpen(true)}>Add to project</Button>
            )}
            <Popconfirm
              title={`Delete ${bulkSelected.size} ${bulkSelected.size === 1 ? 'visualization' : 'visualizations'}?`}
              onConfirm={() => {
                bulkDeleteVizWithUndo(Array.from(bulkSelected));
                setBulkSelected(new Set());
              }}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Button size="small" danger>Delete</Button>
            </Popconfirm>
            <Button size="small" type="text" onClick={() => setBulkSelected(new Set())}>Clear</Button>
          </div>
        )}
      </div>

      {isLoading ? (
        <div className="page-visualizations-loading">
          <Spin size="large" />
        </div>
      ) : filteredVisualizations.length === 0 ? (
        <div className="page-visualizations-empty">
          <BarChartOutlined className="page-visualizations-empty-glyph" />
          <h2>
            {searchText || chartTypeFilter !== 'all'
              ? 'No matches'
              : visibleVisualizations.length === 0
                ? 'No visualizations yet'
                : 'No visualizations'}
          </h2>
          <p>
            {searchText || chartTypeFilter !== 'all'
              ? 'Try a different search or chart type.'
              : 'Run a query in SQL Lab and turn the results into a chart.'}
          </p>
          {!searchText && chartTypeFilter === 'all' && (
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => navigate(projectId ? `/projects/${projectId}/query` : '/query')}
            >
              Create from a query
            </Button>
          )}
          {(searchText || chartTypeFilter !== 'all') && (
            <Button
              onClick={() => {
                setSearchText('');
                setChartTypeFilter('all');
              }}
            >
              Clear filters
            </Button>
          )}
        </div>
      ) : (
        <div className="page-visualizations-grid">
          {filteredVisualizations.map((viz) => {
            const isBulk = bulkSelected.has(viz.id);
            const editable = canEdit(viz.owner) || (projectOwner ? isProjectOwner(projectOwner) : false);
            const tint = chartColors[viz.chartType] || '#5470c6';

            const moreItems: MenuProps['items'] = [
              editable
                ? { key: 'edit', icon: <EditOutlined />, label: 'Edit', onClick: () => setEditBuilderViz(viz) }
                : null,
              !projectId
                ? { key: 'addto', icon: <FolderOutlined />, label: 'Add to project…', onClick: () => setAddToProjectId(viz.id) }
                : null,
              editable ? { type: 'divider' as const } : null,
              editable
                ? {
                    key: 'delete',
                    icon: <DeleteOutlined />,
                    label: 'Delete',
                    danger: true,
                    onClick: () => {
                      Modal.confirm({
                        title: 'Delete this visualization?',
                        content: 'This action cannot be undone.',
                        okText: 'Delete',
                        okButtonProps: { danger: true },
                        onOk: () => deleteVizWithUndo(viz),
                      });
                    },
                  }
                : null,
            ].filter(Boolean) as MenuProps['items'];

            return (
              <div
                key={viz.id}
                className={`viz-tile${isBulk ? ' is-bulk-selected' : ''}`}
                onClick={() => handleView(viz)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    handleView(viz);
                  }
                }}
              >
                <div className="viz-tile-thumb-wrap">
                  <MiniVizPreview viz={viz} />

                  <button
                    type="button"
                    className={`viz-tile-check${isBulk ? ' is-on' : ''}`}
                    aria-label={isBulk ? 'Deselect' : 'Select'}
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleBulk(viz.id);
                    }}
                  >
                    {isBulk && <CheckOutlined />}
                  </button>

                  <div className="viz-tile-actions" onClick={(e) => e.stopPropagation()}>
                    <Dropdown menu={{ items: moreItems }} placement="bottomRight" trigger={['click']}>
                      <button type="button" className="viz-tile-action-btn" aria-label="More">
                        <MoreOutlined />
                      </button>
                    </Dropdown>
                  </div>
                </div>

                <div className="viz-tile-meta">
                  <div className="viz-tile-name-row">
                    <h3 className="viz-tile-name" title={viz.name}>{viz.name}</h3>
                    {viz.isPublic ? (
                      <GlobalOutlined className="viz-tile-vis-icon" title="Public" />
                    ) : (
                      <LockOutlined className="viz-tile-vis-icon" title="Private" />
                    )}
                  </div>
                  <div className="viz-tile-sub">
                    <span className="viz-tile-type" style={{ color: tint }}>
                      <span className="viz-tile-type-icon">{chartIcons[viz.chartType]}</span>
                      <span style={{ textTransform: 'capitalize' }}>{viz.chartType}</span>
                    </span>
                    <span className="viz-tile-time">{formatRelative(viz.updatedAt)}</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Edit via VisualizationEditor */}
      <VisualizationEditor
        open={!!editBuilderViz}
        visualization={editBuilderViz}
        onClose={() => setEditBuilderViz(null)}
      />

      {/* Add to Project Modal */}
      <AddToProjectModal
        open={!!addToProjectId}
        onClose={() => setAddToProjectId(null)}
        itemId={addToProjectId || ''}
        itemType="visualization"
      />

      <BulkAddToProjectModal
        open={bulkProjectModalOpen}
        onClose={() => setBulkProjectModalOpen(false)}
        itemIds={Array.from(bulkSelected)}
        itemType="visualization"
      />

      {/* Quick Look-style View Modal */}
      <Modal
        open={viewModalOpen}
        onCancel={closeView}
        title={
          viewingViz ? (
            <Space>
              <span style={{ color: chartColors[viewingViz.chartType] }}>
                {chartIcons[viewingViz.chartType]}
              </span>
              {viewingViz.name}
            </Space>
          ) : 'Visualization'
        }
        footer={
          <Space>
            {viewingViz?.sql && (
              <Button icon={<CodeOutlined />} onClick={() => setShowSql(!showSql)}>
                {showSql ? 'Hide SQL' : 'Show SQL'}
              </Button>
            )}
            {viewingViz?.sql && (
              <Button icon={<PlayCircleOutlined />} onClick={() => viewingViz && handleView(viewingViz)}>
                Re-run
              </Button>
            )}
            {viewingViz?.sql && (
              <Button
                type="primary"
                ghost
                icon={<CodeOutlined />}
                onClick={() => {
                  navigate(projectId ? `/projects/${projectId}/query` : '/query', { state: { initialSql: viewingViz.sql } });
                }}
              >
                Edit in SQL Lab
              </Button>
            )}
            <Button onClick={closeView}>Close</Button>
          </Space>
        }
        width={860}
        destroyOnClose
      >
        {viewingViz && (
          <div>
            {viewingViz.description && (
              <p className="viz-view-desc">{viewingViz.description}</p>
            )}
            <Space wrap style={{ marginBottom: 14 }}>
              <Tag bordered={false} style={{ background: `${chartColors[viewingViz.chartType]}20`, color: chartColors[viewingViz.chartType] }}>
                <span style={{ marginRight: 4 }}>{chartIcons[viewingViz.chartType]}</span>
                <span style={{ textTransform: 'capitalize' }}>{viewingViz.chartType}</span>
              </Tag>
              {viewingViz.isPublic ? (
                <Tag bordered={false} icon={<GlobalOutlined />} color="success">Public</Tag>
              ) : (
                <Tag bordered={false} icon={<LockOutlined />}>Private</Tag>
              )}
              <span style={{ color: 'var(--color-text-tertiary)', fontSize: 12 }}>
                {viewingViz.owner} · Updated {formatRelative(viewingViz.updatedAt)}
              </span>
            </Space>

            {showSql && viewingViz.sql && (
              <pre className="viz-view-sql">{viewingViz.sql}</pre>
            )}

            {viewQueryError && (
              <Alert type="warning" message={viewQueryError} style={{ marginBottom: 16 }} showIcon />
            )}

            <div className="viz-view-chart">
              <ChartPreview
                chartType={viewingViz.chartType as ChartType}
                config={viewingViz.config}
                data={viewQueryResult}
                loading={viewQueryLoading}
                height={420}
                sql={viewingViz.sql}
              />
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
