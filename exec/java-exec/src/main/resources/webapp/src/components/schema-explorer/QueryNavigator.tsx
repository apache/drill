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
import { Collapse, Spin, Tooltip, Badge, Dropdown } from 'antd';
import { ThunderboltOutlined, BgColorsOutlined } from '@ant-design/icons';
import { getCacheIndicator } from '../../utils/queryCacheIndicator';
import type { Visualization } from '../../types';
import { normalizeSql } from '../../utils/sqlTransformations';

export interface QueryItem {
  id: string;
  name: string;
  description?: string;
  sql?: string;
}

interface QueryNavigatorProps {
  pinnedQueries: QueryItem[];
  recentQueries: QueryItem[];
  workflowQueries: QueryItem[];
  visualizationQueries: QueryItem[];
  onSelectQuery: (query: QueryItem) => void;
  visualizations?: Visualization[];
  onOpenVisualization?: (vizId: string) => void;
  isLoading?: boolean;
}

export default function QueryNavigator({
  pinnedQueries,
  recentQueries,
  workflowQueries,
  visualizationQueries,
  onSelectQuery,
  visualizations = [],
  onOpenVisualization,
  isLoading,
}: QueryNavigatorProps) {
  // Expand recent queries by default if available, otherwise expand pinned
  const defaultExpanded = recentQueries.length > 0 ? ['recent'] : pinnedQueries.length > 0 ? ['pinned'] : [];
  const [expandedKeys, setExpandedKeys] = useState<string[]>(defaultExpanded);

  if (isLoading) {
    return <div style={{ padding: 16, textAlign: 'center' }}><Spin size="small" /></div>;
  }

  const hasContent =
    pinnedQueries.length > 0 ||
    recentQueries.length > 0 ||
    workflowQueries.length > 0 ||
    visualizationQueries.length > 0;

  if (!hasContent) {
    return null;
  }

  const renderQueryItem = (item: QueryItem) => {
    const cacheIndicator = item.sql ? getCacheIndicator(item.sql) : null;

    // Find visualizations linked to this query
    const linkedVizs = item.sql
      ? visualizations.filter(
          (v) => v.sql && normalizeSql(v.sql) === normalizeSql(item.sql || '')
        )
      : [];

    const vizMenu = linkedVizs.length > 0 ? {
      items: linkedVizs.map((v) => ({
        label: v.name,
        key: v.id,
        onClick: () => onOpenVisualization?.(v.id),
      })),
    } : null;

    return (
      <div
        key={item.id}
        onClick={() => onSelectQuery(item)}
        style={{
          padding: '6px 8px',
          cursor: 'pointer',
          borderRadius: 4,
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          marginBottom: 4,
          transition: 'background-color 0.2s',
        }}
        onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = 'var(--color-bg-secondary)')}
        onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'transparent')}
      >
        <div style={{ flex: 1, minWidth: 0 }}>
          <div
            style={{
              fontSize: 12,
              fontWeight: 500,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              color: 'var(--color-text)',
            }}
          >
            {item.name}
          </div>
          {item.description && (
            <div
              style={{
                fontSize: 11,
                color: 'var(--color-text-tertiary)',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {item.description}
            </div>
          )}
        </div>
        {cacheIndicator?.hasCached && (
          <Tooltip title={cacheIndicator.tooltip}>
            <ThunderboltOutlined style={{ fontSize: 12, color: '#faad14' }} />
          </Tooltip>
        )}
        {vizMenu && (
          <div onClick={(e: React.MouseEvent) => e.stopPropagation()}>
            <Dropdown menu={vizMenu} trigger={['click']}>
              <Tooltip title={`${linkedVizs.length} visualization${linkedVizs.length !== 1 ? 's' : ''} linked`}>
                <BgColorsOutlined style={{ fontSize: 12, color: '#1890ff', cursor: 'pointer' }} />
              </Tooltip>
            </Dropdown>
          </div>
        )}
      </div>
    );
  };

  const items = [];

  if (pinnedQueries.length > 0) {
    items.push({
      key: 'pinned',
      label: (
        <span>
          📌 Pinned Queries
          <Badge count={pinnedQueries.length} style={{ marginLeft: 8 }} />
        </span>
      ),
      children: <div>{pinnedQueries.map((q) => renderQueryItem(q))}</div>,
    });
  }

  if (recentQueries.length > 0) {
    items.push({
      key: 'recent',
      label: (
        <span>
          🕐 Recent Queries
          <Badge count={recentQueries.length} style={{ marginLeft: 8 }} />
        </span>
      ),
      children: <div>{recentQueries.map((q) => renderQueryItem(q))}</div>,
    });
  }

  if (workflowQueries.length > 0) {
    items.push({
      key: 'workflow',
      label: (
        <span>
          ⏱️ Workflow Queries
          <Badge count={workflowQueries.length} style={{ marginLeft: 8 }} />
        </span>
      ),
      children: <div>{workflowQueries.map((q) => renderQueryItem(q))}</div>,
    });
  }

  if (visualizationQueries.length > 0) {
    items.push({
      key: 'viz',
      label: (
        <span>
          📊 Visualization Queries
          <Badge count={visualizationQueries.length} style={{ marginLeft: 8 }} />
        </span>
      ),
      children: <div>{visualizationQueries.map((q) => renderQueryItem(q))}</div>,
    });
  }

  return (
    <div style={{ padding: '8px', borderBottom: '1px solid var(--color-border)' }}>
      <Collapse
        items={items}
        expandIconPosition="end"
        activeKey={expandedKeys}
        onChange={(keys) => setExpandedKeys(keys as string[])}
        style={{ fontSize: 12 }}
      />
    </div>
  );
}
