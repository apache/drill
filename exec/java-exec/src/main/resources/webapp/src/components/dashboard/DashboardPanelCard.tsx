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
import { useQuery } from '@tanstack/react-query';
import { Card, Alert, Spin, Typography, Button, Tooltip } from 'antd';
import {
  DragOutlined,
  DeleteOutlined,
  ReloadOutlined,
  FileMarkdownOutlined,
  PictureOutlined,
  FontSizeOutlined,
  BarChartOutlined,
} from '@ant-design/icons';
import { getVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import ChartPreview from '../visualization/ChartPreview';
import MarkdownPanel from './MarkdownPanel';
import ImagePanel from './ImagePanel';
import TitlePanel from './TitlePanel';
import type { DashboardPanel } from '../../types';

const { Text } = Typography;

const PANEL_TYPE_ICONS: Record<string, React.ReactNode> = {
  visualization: <BarChartOutlined />,
  markdown: <FileMarkdownOutlined />,
  image: <PictureOutlined />,
  title: <FontSizeOutlined />,
};

const PANEL_TYPE_LABELS: Record<string, string> = {
  visualization: 'Chart',
  markdown: 'Markdown',
  image: 'Image',
  title: 'Title',
};

interface DashboardPanelCardProps {
  panel: DashboardPanel;
  editMode: boolean;
  refreshInterval?: number;
  onRemove?: (panelId: string) => void;
  onPanelChange?: (panel: DashboardPanel) => void;
}

export default function DashboardPanelCard({
  panel,
  editMode,
  refreshInterval,
  onRemove,
  onPanelChange,
}: DashboardPanelCardProps) {
  const panelType = panel.type || 'visualization';
  const isVisualization = panelType === 'visualization';

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

  // Execute the visualization's SQL query (only for visualization panels)
  const {
    data: queryResult,
    isLoading: queryLoading,
    error: queryError,
    refetch,
  } = useQuery({
    queryKey: ['dashboard-panel-data', panel.visualizationId, visualization?.sql],
    queryFn: () => {
      if (!visualization?.sql) {
        throw new Error('No SQL query configured for this visualization');
      }
      return executeQuery({
        query: visualization.sql,
        queryType: 'SQL',
        defaultSchema: visualization.defaultSchema,
      });
    },
    enabled: isVisualization && !!visualization?.sql,
    staleTime: 30000,
    refetchInterval: refreshInterval && refreshInterval > 0 ? refreshInterval * 1000 : false,
  });

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
              config={visualization.config}
              data={queryResult || null}
              height={Math.max((panel.height * 120) - 60, 150)}
            />
          );
        }

        return null;
      }
    }
  };

  return (
    <Card
      className="dashboard-panel-card"
      size="small"
      title={
        <div className="dashboard-panel-header">
          {editMode && <DragOutlined className="drag-handle" />}
          {!isVisualization && <span style={{ marginRight: 4 }}>{getPanelIcon()}</span>}
          <Text ellipsis style={{ flex: 1 }}>
            {getPanelTitle()}
          </Text>
        </div>
      }
      extra={
        <div style={{ display: 'flex', gap: 4 }}>
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
          {editMode && (
            <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={handleRemove} />
          )}
        </div>
      }
    >
      {renderContent()}
    </Card>
  );
}
