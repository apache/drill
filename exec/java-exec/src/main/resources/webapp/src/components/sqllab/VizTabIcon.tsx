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

import { Tooltip, Popover, Button, Divider, Space, message } from 'antd';
import { BarChartOutlined, ExclamationCircleOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { updateVisualization } from '../../api/visualizations';
import { useQueryClient } from '@tanstack/react-query';
import type { Visualization } from '../../types';
import { normalizeSql } from '../../utils/sqlTransformations';

interface VizTabIconProps {
  vizs: Visualization[];
  isStale: boolean;
  projectId?: string;
  currentSql?: string;
  onUpdateSql?: (vizId: string, newSql: string) => void;
  onRemoveLink: (vizId: string) => void;
}

export const VizTabIcon = ({
  vizs,
  isStale,
  projectId,
  currentSql,
  onUpdateSql,
  onRemoveLink,
}: VizTabIconProps) => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const handleNavigateToViz = (vizId: string) => {
    const path = projectId ? `/projects/${projectId}/visualizations` : '/visualizations';
    navigate(path, { state: { viewVizId: vizId } });
  };

  const handleUpdateVizSql = async (vizId: string) => {
    if (!currentSql) {
      message.error('No SQL available to update');
      return;
    }
    try {
      await updateVisualization(vizId, { sql: currentSql });
      queryClient.invalidateQueries({ queryKey: ['visualizations'] });
      message.success('Visualization SQL updated');
      onUpdateSql?.(vizId, currentSql);
    } catch (error) {
      message.error('Failed to update visualization SQL');
    }
  };

  const handleRemoveLink = (vizId: string) => {
    onRemoveLink(vizId);
    message.info('Visualization link removed');
  };

  // Single viz, not stale — simple blue icon
  if (vizs.length === 1 && !isStale) {
    return (
      <Tooltip title={`View visualization: ${vizs[0].name}`}>
        <span
          className="tab-viz-btn"
          onClick={(e) => {
            e.stopPropagation();
            handleNavigateToViz(vizs[0].id);
          }}
          style={{ display: 'inline-flex', marginLeft: 4, cursor: 'pointer' }}
        >
          <BarChartOutlined style={{ color: '#5470c6', fontSize: 12 }} />
        </span>
      </Tooltip>
    );
  }

  // Single viz, stale — amber icon with warning badge
  if (vizs.length === 1 && isStale) {
    const viz = vizs[0];
    const content = (
      <div style={{ minWidth: 220 }}>
        <div style={{ marginBottom: 12 }}>
          <strong>{viz.name}</strong>
          <div style={{ fontSize: 12, color: '#999', marginTop: 4 }}>
            Query SQL has changed since this visualization was created.
          </div>
        </div>
        <Divider style={{ margin: '8px 0' }} />
        <Space direction="vertical" style={{ width: '100%' }}>
          <Button
            type="primary"
            size="small"
            block
            onClick={(e) => {
              e.stopPropagation();
              handleNavigateToViz(viz.id);
            }}
          >
            View Visualization (Original SQL)
          </Button>
          <Button
            size="small"
            block
            onClick={(e) => {
              e.stopPropagation();
              handleUpdateVizSql(viz.id);
            }}
          >
            Update SQL
          </Button>
          <Button
            danger
            size="small"
            block
            onClick={(e) => {
              e.stopPropagation();
              handleRemoveLink(viz.id);
            }}
          >
            Remove Link
          </Button>
        </Space>
      </div>
    );

    return (
      <Popover trigger="click" content={content} title="Stale Visualization">
        <Tooltip title="Visualization uses different SQL — click for options">
          <span
            className="tab-viz-btn"
            onClick={(e) => e.stopPropagation()}
            style={{
              display: 'inline-flex',
              alignItems: 'center',
              marginLeft: 4,
              cursor: 'pointer',
              color: '#faad14',
            }}
          >
            <BarChartOutlined style={{ fontSize: 12 }} />
            <ExclamationCircleOutlined style={{ fontSize: 10, marginLeft: 2 }} />
          </span>
        </Tooltip>
      </Popover>
    );
  }

  // Multiple vizs — icon with count badge and popover list
  const content = (
    <div style={{ minWidth: 220 }}>
      <div style={{ marginBottom: 12 }}>
        <strong>{vizs.length} visualizations</strong>
      </div>
      <Divider style={{ margin: '8px 0' }} />
      {vizs.map((viz) => {
        const vizIsStale = viz.sql && normalizeSql(viz.sql) !== normalizeSql(currentSql ?? '');
        return (
          <div
            key={viz.id}
            style={{
              padding: '8px',
              marginBottom: 8,
              border: '1px solid #f0f0f0',
              borderRadius: 4,
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span style={{ fontSize: 12, flex: 1, marginRight: 8 }}>
                {viz.name}
                {vizIsStale && (
                  <ExclamationCircleOutlined style={{ marginLeft: 6, color: '#faad14' }} />
                )}
              </span>
            </div>
            <Space size="small" style={{ marginTop: 6, width: '100%' }}>
              <Button
                type="primary"
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  handleNavigateToViz(viz.id);
                }}
              >
                View
              </Button>
              {vizIsStale && (
                <Button
                  size="small"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleUpdateVizSql(viz.id);
                  }}
                >
                  Update
                </Button>
              )}
              <Button
                danger
                size="small"
                onClick={(e) => {
                  e.stopPropagation();
                  handleRemoveLink(viz.id);
                }}
              >
                Remove
              </Button>
            </Space>
          </div>
        );
      })}
    </div>
  );

  return (
    <Popover trigger="click" content={content} title="Saved Visualizations">
      <Tooltip title={`${vizs.length} visualizations for this query`}>
        <span
          className="tab-viz-btn"
          onClick={(e) => e.stopPropagation()}
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            marginLeft: 4,
            cursor: 'pointer',
            opacity: 0.7,
            transition: 'opacity 0.15s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLElement).style.opacity = '1';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLElement).style.opacity = '0.7';
          }}
        >
          <BarChartOutlined style={{ color: '#5470c6', fontSize: 12 }} />
          <span style={{ fontSize: 10, marginLeft: 2, color: '#5470c6' }}>{vizs.length}</span>
        </span>
      </Tooltip>
    </Popover>
  );
};
