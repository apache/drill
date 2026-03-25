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
import { useCallback, useMemo, useState } from 'react';
import { useQuery, useQueries } from '@tanstack/react-query';
import { Card, Alert, Spin, Typography, Button, Tooltip, Switch, Form, InputNumber, Popover } from 'antd';
import {
  DragOutlined,
  DeleteOutlined,
  ReloadOutlined,
  FileMarkdownOutlined,
  PictureOutlined,
  FontSizeOutlined,
  BarChartOutlined,
  BulbOutlined,
  ClockCircleOutlined,
  FilterOutlined,
  CommentOutlined,
  AlertOutlined,
  SearchOutlined,
  GlobalOutlined,
} from '@ant-design/icons';
import { getVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import { applyDashboardFilters, computeEffectiveQuery } from '../../utils/sqlTransformations';
import ChartPreview from '../visualization/ChartPreview';
import MarkdownPanel from './MarkdownPanel';
import ImagePanel from './ImagePanel';
import TitlePanel from './TitlePanel';
import ExecutiveSummaryPanel from './ExecutiveSummaryPanel';
import AiQnAPanel from './AiQnAPanel';
import AiAlertsPanel from './AiAlertsPanel';
import NlFilterPanel from './NlFilterPanel';
import type { DashboardPanel, DashboardFilter } from '../../types';
import type { DashboardDataContext } from '../../types/ai';

const { Text } = Typography;

const PANEL_TYPE_ICONS: Record<string, React.ReactNode> = {
  visualization: <BarChartOutlined />,
  markdown: <FileMarkdownOutlined />,
  image: <PictureOutlined />,
  title: <FontSizeOutlined />,
  executiveSummary: <BulbOutlined />,
  aiQnA: <CommentOutlined />,
  aiAlerts: <AlertOutlined />,
  nlFilter: <SearchOutlined />,
};

const PANEL_TYPE_LABELS: Record<string, string> = {
  visualization: 'Chart',
  markdown: 'Markdown',
  image: 'Image',
  title: 'Title',
  executiveSummary: 'AI Summary',
  aiQnA: 'AI Q&A',
  aiAlerts: 'AI Alerts',
  nlFilter: 'NL Filter',
};

interface DashboardPanelCardProps {
  panel: DashboardPanel;
  editMode: boolean;
  refreshInterval?: number;
  dashboardUpdatedAt?: number | string;
  darkMode?: boolean;
  filters?: DashboardFilter[];
  allPanels?: DashboardPanel[];
  onRemove?: (panelId: string) => void;
  onPanelChange?: (panel: DashboardPanel) => void;
  onChartClick?: (column: string, value: string, vizId: string, isTemporal?: boolean, isNumeric?: boolean) => void;
  onApplyFilters?: (filters: DashboardFilter[]) => void;
}

export default function DashboardPanelCard({
  panel,
  editMode,
  refreshInterval,
  dashboardUpdatedAt,
  darkMode,
  filters,
  allPanels,
  onRemove,
  onPanelChange,
  onChartClick,
  onApplyFilters,
}: DashboardPanelCardProps) {
  const panelType = panel.type || 'visualization';
  const isVisualization = panelType === 'visualization';
  const isExecutiveSummary = panelType === 'executiveSummary';
  const isAiQnA = panelType === 'aiQnA';
  const isAiAlerts = panelType === 'aiAlerts';
  const isNlFilter = panelType === 'nlFilter';
  const needsDashboardData = isExecutiveSummary || isAiQnA || isAiAlerts || isNlFilter;
  const [summaryRefreshKey, setSummaryRefreshKey] = useState(0);

  // Fetch the visualization metadata (only for visualization panels)
  const {
    data: visualization,
    isLoading: vizLoading,
    error: vizError,
  } = useQuery({
    queryKey: ['visualization', panel.visualizationId],
    queryFn: () => getVisualization(panel.visualizationId!),
    staleTime: 60000,
    enabled: isVisualization && !!panel.visualizationId,
  });

  // Determine applicable cross-filters for this panel
  const ignoreCrossFilters = panel.config?.ignoreCrossFilters === 'true';
  const applicableFilters = useMemo(() => {
    if (!filters || filters.length === 0 || ignoreCrossFilters) {
      return [];
    }
    return filters.filter((f) => f.sourceVizId !== panel.visualizationId);
  }, [filters, ignoreCrossFilters, panel.visualizationId]);

  // Build effective SQL: inject cross-filter WHERE conditions into the
  // original SQL first (so filter columns resolve against the source table),
  // then apply time grain / aggregation wrapping on top.
  const effectiveSql = useMemo(() => {
    if (!visualization?.sql) {
      return undefined;
    }
    const filtered = applicableFilters.length > 0
      ? applyDashboardFilters(visualization.sql, applicableFilters)
      : visualization.sql;
    return computeEffectiveQuery(filtered, {
      xAxis: visualization.config?.xAxis,
      metrics: visualization.config?.metrics,
      dimensions: visualization.config?.dimensions,
      chartOptions: visualization.config?.chartOptions,
    });
  }, [visualization?.sql, visualization?.config, applicableFilters]);

  // Execute the visualization's SQL query
  const {
    data: queryResult,
    isLoading: queryLoading,
    error: queryError,
    refetch,
  } = useQuery({
    queryKey: ['dashboard-panel-data', panel.visualizationId, effectiveSql],
    queryFn: () => {
      if (!effectiveSql) {
        throw new Error('No SQL query configured for this visualization');
      }
      return executeQuery({
        query: effectiveSql,
        queryType: 'SQL',
        defaultSchema: visualization?.defaultSchema,
      });
    },
    enabled: isVisualization && !!effectiveSql,
    staleTime: 30000,
    refetchInterval: refreshInterval && refreshInterval > 0 ? refreshInterval * 1000 : false,
  });

  // For AI panels: collect data from sibling visualization panels
  const siblingVizPanels = useMemo(() => {
    if (!needsDashboardData || !allPanels) {
      return [];
    }
    let vizPanels = allPanels.filter(
      (p) => p.type === 'visualization' && p.visualizationId && p.id !== panel.id,
    );
    // A3: Panel scope filtering for executive summary
    if (isExecutiveSummary && panel.config?.selectedPanelIds) {
      const selectedIds = panel.config.selectedPanelIds.split(',').filter(Boolean);
      if (selectedIds.length > 0) {
        vizPanels = vizPanels.filter((p) => selectedIds.includes(p.visualizationId || ''));
      }
    }
    return vizPanels;
  }, [needsDashboardData, allPanels, panel.id, panel.config?.selectedPanelIds, isExecutiveSummary]);

  // Fetch all sibling visualization metadata
  const siblingVizQueries = useQueries({
    queries: siblingVizPanels.map((p) => ({
      queryKey: ['visualization', p.visualizationId],
      queryFn: () => getVisualization(p.visualizationId!),
      staleTime: 60000,
      enabled: needsDashboardData,
    })),
  });

  // Fetch all sibling visualization data
  const siblingDataQueries = useQueries({
    queries: siblingVizPanels.map((p, i) => {
      const viz = siblingVizQueries[i]?.data;
      let sql: string | undefined;
      if (viz?.sql) {
        const filtered =
          filters && filters.length > 0
            ? applyDashboardFilters(viz.sql, filters.filter((f) => f.sourceVizId !== p.visualizationId))
            : viz.sql;
        sql = computeEffectiveQuery(filtered, {
          xAxis: viz.config?.xAxis,
          metrics: viz.config?.metrics,
          dimensions: viz.config?.dimensions,
          chartOptions: viz.config?.chartOptions,
        });
      }
      return {
        queryKey: ['dashboard-panel-data', p.visualizationId, sql],
        queryFn: () =>
          executeQuery({
            query: sql!,
            queryType: 'SQL',
            defaultSchema: viz?.defaultSchema,
          }),
        staleTime: 30000,
        enabled: needsDashboardData && !!sql,
        refetchInterval: refreshInterval && refreshInterval > 0 ? refreshInterval * 1000 : false,
      };
    }),
  });

  // Build dashboard data context for AI panels
  const dashboardData: DashboardDataContext[] = useMemo(() => {
    if (!needsDashboardData) {
      return [];
    }
    const result: DashboardDataContext[] = [];
    for (let i = 0; i < siblingVizPanels.length; i++) {
      const viz = siblingVizQueries[i]?.data;
      const queryData = siblingDataQueries[i]?.data;
      if (viz && queryData) {
        const columns = queryData.columns || [];
        const columnTypes = queryData.metadata || [];
        const rows = queryData.rows || [];
        result.push({
          panelName: viz.name,
          sql: viz.sql || '',
          columns,
          columnTypes,
          rowCount: rows.length,
          sampleRows: rows.slice(0, 5),
        });
      }
    }
    return result;
  }, [needsDashboardData, siblingVizPanels, siblingVizQueries, siblingDataQueries]);

  // Build allPanelNames for executive summary scope selector (A3)
  const allPanelNames = useMemo(() => {
    if (!isExecutiveSummary) {
      return undefined;
    }
    return siblingVizQueries
      .map((q, i) => q.data ? { id: siblingVizPanels[i]?.visualizationId || '', name: q.data.name } : null)
      .filter((p): p is { id: string; name: string } => p !== null);
  }, [isExecutiveSummary, siblingVizQueries, siblingVizPanels]);

  const handleRemove = useCallback(() => {
    if (onRemove) {
      onRemove(panel.id);
    }
  }, [onRemove, panel.id]);

  const handleContentChange = useCallback((content: string) => {
    if (onPanelChange) {
      onPanelChange({ ...panel, content });
    }
  }, [onPanelChange, panel]);

  const handleConfigChange = useCallback((config: Record<string, string>) => {
    if (onPanelChange) {
      onPanelChange({ ...panel, config });
    }
  }, [onPanelChange, panel]);

  const handleChartClick = useCallback((column: string, value: string, isTemporal?: boolean, isNumeric?: boolean) => {
    if (onChartClick && panel.visualizationId) {
      onChartClick(column, value, panel.visualizationId, isTemporal, isNumeric);
    }
  }, [onChartClick, panel.visualizationId]);

  const handleToggleIgnoreCrossFilters = useCallback((checked: boolean) => {
    if (onPanelChange) {
      const newConfig = { ...(panel.config || {}), ignoreCrossFilters: checked ? 'true' : 'false' };
      onPanelChange({ ...panel, config: newConfig });
    }
  }, [onPanelChange, panel]);

  // Merge panel-level choropleth overrides into visualization config
  const mergedConfig = useMemo(() => {
    if (!visualization?.config) return visualization?.config;
    const base = { ...visualization.config };
    const panelZoom = panel.config?.choroplethZoom;
    const panelCenter = panel.config?.choroplethCenter;
    if (panelZoom || panelCenter) {
      const overrides: Record<string, unknown> = {};
      if (panelZoom) overrides.choroplethZoom = parseFloat(panelZoom);
      if (panelCenter) {
        const [lon, lat] = panelCenter.split(',').map(Number);
        overrides.choroplethCenter = [lon, lat] as [number, number];
      }
      base.chartOptions = { ...(base.chartOptions || {}), ...overrides };
    }
    return base;
  }, [visualization, panel.config?.choroplethZoom, panel.config?.choroplethCenter]);

  // Determine title based on panel type
  const getPanelTitle = () => {
    if (isVisualization) {
      return visualization?.name || 'Loading...';
    }
    return PANEL_TYPE_LABELS[panelType] || panelType;
  };

  const getPanelIcon = () => {
    return PANEL_TYPE_ICONS[panelType] || <BarChartOutlined />;
  };

  // Choropleth map settings
  const isChoropleth = visualization?.chartType === 'choropleth';
  const panelZoomValue = panel.config?.choroplethZoom
    ? parseFloat(panel.config.choroplethZoom) : undefined;
  const panelCenterLon = panel.config?.choroplethCenter
    ? parseFloat(panel.config.choroplethCenter.split(',')[0]) : undefined;
  const panelCenterLat = panel.config?.choroplethCenter
    ? parseFloat(panel.config.choroplethCenter.split(',')[1]) : undefined;
  const vizDefaultZoom = visualization?.config?.chartOptions?.choroplethZoom as number | undefined;

  const handleMapViewChange = useCallback((field: 'zoom' | 'lon' | 'lat', val: number | null) => {
    const newConfig = { ...(panel.config || {}) };
    if (field === 'zoom') {
      if (val != null) newConfig.choroplethZoom = String(val);
      else delete newConfig.choroplethZoom;
    } else {
      const lon = field === 'lon' ? (val ?? 0) : (panelCenterLon ?? 0);
      const lat = field === 'lat' ? (val ?? 0) : (panelCenterLat ?? 0);
      newConfig.choroplethCenter = `${lon},${lat}`;
    }
    onPanelChange?.({ ...panel, config: newConfig });
  }, [panel, panelCenterLon, panelCenterLat, onPanelChange]);

  const handleResetMapView = useCallback(() => {
    const newConfig = { ...(panel.config || {}) };
    delete newConfig.choroplethZoom;
    delete newConfig.choroplethCenter;
    onPanelChange?.({ ...panel, config: newConfig });
  }, [panel, onPanelChange]);

  // Render content based on panel type
  const renderContent = () => {
    switch (panelType) {
      case 'markdown':
        return (
          <MarkdownPanel
            content={panel.content || ''}
            editMode={editMode}
            onContentChange={handleContentChange}
          />
        );
      case 'image':
        return (
          <ImagePanel
            content={panel.content || ''}
            config={panel.config}
            editMode={editMode}
            onContentChange={handleContentChange}
            onConfigChange={handleConfigChange}
          />
        );
      case 'title':
        return (
          <TitlePanel
            content={panel.content || ''}
            config={panel.config}
            editMode={editMode}
            onContentChange={handleContentChange}
            onConfigChange={handleConfigChange}
          />
        );
      case 'executiveSummary':
        return (
          <ExecutiveSummaryPanel
            content={panel.content || ''}
            config={panel.config}
            editMode={editMode}
            darkMode={darkMode}
            refreshKey={summaryRefreshKey}
            dashboardData={dashboardData}
            allPanelNames={allPanelNames}
            onContentChange={handleContentChange}
            onConfigChange={handleConfigChange}
          />
        );
      case 'aiQnA':
        return (
          <AiQnAPanel
            config={panel.config}
            editMode={editMode}
            darkMode={darkMode}
            dashboardData={dashboardData}
            onConfigChange={handleConfigChange}
          />
        );
      case 'aiAlerts':
        return (
          <AiAlertsPanel
            config={panel.config}
            editMode={editMode}
            darkMode={darkMode}
            dashboardData={dashboardData}
            onConfigChange={handleConfigChange}
          />
        );
      case 'nlFilter':
        return (
          <NlFilterPanel
            config={panel.config}
            editMode={editMode}
            darkMode={darkMode}
            dashboardData={dashboardData}
            onConfigChange={handleConfigChange}
            onApplyFilters={onApplyFilters}
          />
        );
      case 'visualization':
      default: {
        const isLoading = vizLoading || queryLoading;
        const error = vizError || queryError;

        if (error) {
          return (
            <Alert
              message="Error loading panel"
              description={error instanceof Error ? error.message : 'Unknown error'}
              type="error"
              showIcon
            />
          );
        }

        if (isLoading) {
          return (
            <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: 150 }}>
              <Spin tip="Loading..." />
            </div>
          );
        }

        if (visualization) {
          return (
            <ChartPreview
              chartType={visualization.chartType}
              config={mergedConfig!}
              data={queryResult || null}
              height={Math.max((panel.height * 120) - 60, 150)}
              darkMode={darkMode}
              onChartClick={onChartClick ? handleChartClick : undefined}
            />
          );
        }

        return null;
      }
    }
  };

  const formattedUpdatedAt = useMemo(() => {
    if (!dashboardUpdatedAt) {
      return null;
    }
    const date = typeof dashboardUpdatedAt === 'number'
      ? new Date(dashboardUpdatedAt)
      : new Date(dashboardUpdatedAt);
    return date.toLocaleString();
  }, [dashboardUpdatedAt]);

  return (
    <Card
      className="dashboard-panel-card"
      size="small"
      title={
        <div className="dashboard-panel-header">
          {editMode && <DragOutlined className="drag-handle" />}
          {!isVisualization && <span style={{ marginRight: 4 }}>{getPanelIcon()}</span>}
          <Text ellipsis style={{ flex: 1, color: 'inherit' }}>
            {getPanelTitle()}
          </Text>
        </div>
      }
      extra={
        <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
          {editMode && isVisualization && (
            <Tooltip title="Ignore cross-filters">
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 2 }}>
                <FilterOutlined style={{ fontSize: 12, opacity: ignoreCrossFilters ? 0.4 : 1 }} />
                <Switch
                  size="small"
                  checked={ignoreCrossFilters}
                  onChange={handleToggleIgnoreCrossFilters}
                />
              </span>
            </Tooltip>
          )}
          {editMode && isChoropleth && (
            <Popover
              trigger="click"
              title="Map Starting View"
              content={
                <div style={{ width: 220 }}>
                  <Form layout="vertical" size="small">
                    <Form.Item label="Starting Zoom" style={{ marginBottom: 8 }}>
                      <InputNumber
                        min={1} max={20} step={0.5} style={{ width: '100%' }}
                        placeholder={`Default (${vizDefaultZoom ?? 'auto'})`}
                        value={panelZoomValue}
                        onChange={(val) => handleMapViewChange('zoom', val)}
                      />
                    </Form.Item>
                    <Form.Item label="Center Longitude" style={{ marginBottom: 8 }}>
                      <InputNumber
                        min={-180} max={180} step={0.1} style={{ width: '100%' }}
                        placeholder="Longitude"
                        value={panelCenterLon}
                        onChange={(val) => handleMapViewChange('lon', val)}
                      />
                    </Form.Item>
                    <Form.Item label="Center Latitude" style={{ marginBottom: 4 }}>
                      <InputNumber
                        min={-90} max={90} step={0.1} style={{ width: '100%' }}
                        placeholder="Latitude"
                        value={panelCenterLat}
                        onChange={(val) => handleMapViewChange('lat', val)}
                      />
                    </Form.Item>
                    <Button
                      size="small" type="link" style={{ padding: 0 }}
                      onClick={handleResetMapView}
                    >
                      Reset to visualization defaults
                    </Button>
                  </Form>
                </div>
              }
            >
              <Tooltip title="Map starting view">
                <GlobalOutlined style={{ cursor: 'pointer' }} />
              </Tooltip>
            </Popover>
          )}
          {isVisualization && (
            <Tooltip title="Refresh">
              <Button
                type="text"
                size="small"
                icon={<ReloadOutlined />}
                onClick={() => refetch()}
                disabled={vizLoading || queryLoading}
              />
            </Tooltip>
          )}
          {isExecutiveSummary && !editMode && (
            <Tooltip title="Regenerate summary">
              <Button
                type="text"
                size="small"
                icon={<ReloadOutlined />}
                onClick={() => setSummaryRefreshKey((k) => k + 1)}
              />
            </Tooltip>
          )}
          {editMode && (
            <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={handleRemove} />
          )}
        </div>
      }
    >
      {renderContent()}
      {formattedUpdatedAt && (
        <div className="dashboard-panel-footer">
          <ClockCircleOutlined /> Last updated: {formattedUpdatedAt}
        </div>
      )}
    </Card>
  );
}
