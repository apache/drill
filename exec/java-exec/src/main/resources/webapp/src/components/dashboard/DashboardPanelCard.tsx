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
import { DragOutlined, DeleteOutlined, ReloadOutlined } from '@ant-design/icons';
import { getVisualization } from '../../api/visualizations';
import { executeQuery } from '../../api/queries';
import ChartPreview from '../visualization/ChartPreview';
import type { DashboardPanel } from '../../types';

const { Text } = Typography;

interface DashboardPanelCardProps {
  panel: DashboardPanel;
  editMode: boolean;
  refreshInterval?: number;
  onRemove?: (panelId: string) => void;
}

export default function DashboardPanelCard({
  panel,
  editMode,
  refreshInterval,
  onRemove,
}: DashboardPanelCardProps) {
  // Fetch the visualization metadata
  const {
    data: visualization,
    isLoading: vizLoading,
    error: vizError,
  } = useQuery({
    queryKey: ['visualization', panel.visualizationId],
    queryFn: () => getVisualization(panel.visualizationId),
    staleTime: 60000,
  });

  // Execute the visualization's SQL query
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
    enabled: !!visualization?.sql,
    staleTime: 30000,
    refetchInterval: refreshInterval && refreshInterval > 0 ? refreshInterval * 1000 : false,
  });

  const handleRemove = useCallback(() => {
    if (onRemove) {
      onRemove(panel.id);
    }
  }, [onRemove, panel.id]);

  const isLoading = vizLoading || queryLoading;
  const error = vizError || queryError;

  if (error) {
    return (
      <Card
        className="dashboard-panel-card"
        size="small"
        title={
          <div className="dashboard-panel-header">
            {editMode && <DragOutlined className="drag-handle" />}
            <Text ellipsis style={{ flex: 1 }}>
              {visualization?.name || 'Error'}
            </Text>
          </div>
        }
        extra={editMode && (
          <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={handleRemove} />
        )}
      >
        <Alert
          message="Error loading panel"
          description={error instanceof Error ? error.message : 'Unknown error'}
          type="error"
          showIcon
        />
      </Card>
    );
  }

  return (
    <Card
      className="dashboard-panel-card"
      size="small"
      title={
        <div className="dashboard-panel-header">
          {editMode && <DragOutlined className="drag-handle" />}
          <Text ellipsis style={{ flex: 1 }}>
            {visualization?.name || 'Loading...'}
          </Text>
        </div>
      }
      extra={
        <div style={{ display: 'flex', gap: 4 }}>
          <Tooltip title="Refresh">
            <Button
              type="text"
              size="small"
              icon={<ReloadOutlined />}
              onClick={() => refetch()}
              disabled={isLoading}
            />
          </Tooltip>
          {editMode && (
            <Button type="text" size="small" danger icon={<DeleteOutlined />} onClick={handleRemove} />
          )}
        </div>
      }
    >
      {isLoading ? (
        <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: 150 }}>
          <Spin tip="Loading..." />
        </div>
      ) : visualization ? (
        <ChartPreview
          chartType={visualization.chartType}
          config={visualization.config}
          data={queryResult || null}
          height={Math.max((panel.height * 120) - 60, 150)}
        />
      ) : null}
    </Card>
  );
}
