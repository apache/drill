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

import { useMemo, useCallback, useState } from 'react';
import {
  Tabs,
  Button,
  Space,
  List,
  Statistic,
  Row,
  Col,
  Divider,
  InputNumber,
  Select,
  Progress,
  message,
  Empty,
} from 'antd';
import { DeleteOutlined, PushpinOutlined } from '@ant-design/icons';
import type { ColumnApi } from 'ag-grid-community';
import {
  calculateColumnStats,
  type ColumnTransformation,
} from '../../utils/sqlTransformations';

export interface ColumnMenuProps {
  columnName: string;
  rowData: Record<string, unknown>[];
  columnApi?: ColumnApi;
  onTransformColumn?: (columnName: string, transformation: ColumnTransformation) => void;
}

export default function ColumnMenu({
  columnName,
  rowData,
  columnApi,
  onTransformColumn,
}: ColumnMenuProps) {
  const [truncateLength, setTruncateLength] = useState(50);
  const [castType, setCastType] = useState('VARCHAR');

  // Calculate statistics
  const stats = useMemo(() => {
    if (!rowData || rowData.length === 0) {
      return null;
    }
    return calculateColumnStats(rowData, columnName);
  }, [rowData, columnName]);

  // Handle column visibility
  const handleHideColumn = useCallback(() => {
    if (columnApi && columnName) {
      columnApi.setColumnVisible(columnName, false);
    }
  }, [columnApi, columnName]);

  // Handle column pinning
  const handlePinColumn = useCallback(
    (pinTo: 'left' | 'right' | null) => {
      if (columnApi && columnName) {
        columnApi.setColumnPinned(columnName, pinTo);
      }
    },
    [columnApi, columnName]
  );

  // Get top 10 values
  const topValues = useMemo(() => {
    if (!rowData || rowData.length === 0) {
      return [];
    }

    const values = rowData
      .map((row) => row[columnName])
      .filter((v) => v != null);

    const counts = new Map<string, number>();
    values.forEach((v) => {
      const key = String(v);
      counts.set(key, (counts.get(key) || 0) + 1);
    });

    return Array.from(counts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([value, count]) => ({ value, count }));
  }, [rowData, columnName]);

  // Handle transformation
  const handleTransformation = useCallback(
    (type: ColumnTransformation['type']) => {
      const transformation: ColumnTransformation = {
        type,
        columnName,
      };

      if (type === 'cast') {
        transformation.targetType = castType;
      } else if (type === 'truncate' || type === 'substring') {
        transformation.length = truncateLength;
      }

      onTransformColumn?.(columnName, transformation);
    },
    [columnName, onTransformColumn, castType, truncateLength]
  );

  // Handle export column as CSV
  const handleExportCsv = useCallback(() => {
    if (!rowData || rowData.length === 0) {
      message.warning('No data to export');
      return;
    }

    const values = rowData
      .map((row) => {
        const val = row[columnName];
        if (val == null) return '';
        if (typeof val === 'object') return JSON.stringify(val);
        return String(val);
      })
      .join('\n');

    const csv = `${columnName}\n${values}`;
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${columnName}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    message.success('Column exported as CSV');
  }, [rowData, columnName]);

  // Handle copy column to clipboard
  const handleCopyToClipboard = useCallback(async () => {
    if (!rowData || rowData.length === 0) {
      message.warning('No data to copy');
      return;
    }

    const values = rowData
      .map((row) => {
        const val = row[columnName];
        if (val == null) return '';
        if (typeof val === 'object') return JSON.stringify(val);
        return String(val);
      })
      .join('\n');

    try {
      await navigator.clipboard.writeText(values);
      message.success('Column values copied to clipboard');
    } catch (err) {
      message.error('Failed to copy to clipboard');
    }
  }, [rowData, columnName]);

  return (
    <Tabs
      defaultActiveKey="actions"
      style={{ width: 350 }}
      tabBarStyle={{ marginBottom: 12 }}
      items={[
        {
          key: 'actions',
          label: 'Actions',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button
                block
                size="small"
                danger
                icon={<DeleteOutlined />}
                onClick={handleHideColumn}
              >
                Hide Column
              </Button>
              <Divider style={{ margin: '8px 0' }} />
              <div style={{ fontSize: 12, marginBottom: 8, fontWeight: 'bold' }}>
                Pin Column
              </div>
              <Space style={{ width: '100%' }}>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  icon={<PushpinOutlined />}
                  onClick={() => handlePinColumn('left')}
                >
                  Pin Left
                </Button>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  icon={<PushpinOutlined />}
                  onClick={() => handlePinColumn('right')}
                >
                  Pin Right
                </Button>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  onClick={() => handlePinColumn(null)}
                >
                  Unpin
                </Button>
              </Space>
            </Space>
          ),
        },
        {
          key: 'statistics',
          label: 'Statistics',
          children: stats ? (
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              <div>
                <Row gutter={16}>
                  <Col span={12}>
                    <Statistic title="Count" value={stats.count} />
                  </Col>
                  <Col span={12}>
                    <Statistic title="Null Count" value={stats.nullCount} />
                  </Col>
                </Row>
              </div>
              <div>
                <Row gutter={16}>
                  <Col span={12}>
                    <Statistic title="Distinct" value={stats.distinctCount} />
                  </Col>
                  <Col span={12}>
                    <Statistic
                      title="Uniqueness"
                      value={stats.uniquenessPercentage}
                      suffix="%"
                      precision={1}
                    />
                  </Col>
                </Row>
              </div>
              <div>
                <div style={{ fontSize: 12, marginBottom: 8 }}>Non-Null Data</div>
                <Progress
                  percent={Math.round(stats.nonNullPercentage)}
                  status={stats.nonNullPercentage < 50 ? 'exception' : 'normal'}
                />
              </div>
              {stats.min !== undefined && (
                <div>
                  <Row gutter={16}>
                    <Col span={8}>
                      <Statistic
                        title="Min"
                        value={stats.min}
                        precision={2}
                      />
                    </Col>
                    <Col span={8}>
                      <Statistic
                        title="Max"
                        value={stats.max}
                        precision={2}
                      />
                    </Col>
                    <Col span={8}>
                      <Statistic
                        title="Avg"
                        value={stats.avg}
                        precision={2}
                      />
                    </Col>
                  </Row>
                </div>
              )}
            </Space>
          ) : (
            <Empty description="No data" />
          ),
        },
        {
          key: 'filters',
          label: 'Filters',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ fontSize: 12, color: '#999' }}>
                Use column filters in the grid to filter data
              </div>
              {topValues.length > 0 && (
                <>
                  <Divider style={{ margin: '8px 0' }} />
                  <div style={{ fontSize: 12, fontWeight: 'bold', marginBottom: 8 }}>
                    Top Values (Preview)
                  </div>
                  <List
                    dataSource={topValues}
                    renderItem={(item) => (
                      <List.Item
                        style={{ padding: '4px 0' }}
                      >
                        <div style={{ fontSize: 12 }}>
                          <strong>{item.value}</strong> ({item.count})
                        </div>
                      </List.Item>
                    )}
                    size="small"
                  />
                </>
              )}
            </Space>
          ),
        },
        {
          key: 'transform',
          label: 'Transform',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ fontSize: 12, fontWeight: 'bold' }}>Text Transformations</div>
              <Space style={{ width: '100%' }}>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  onClick={() => handleTransformation('uppercase')}
                >
                  UPPER
                </Button>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  onClick={() => handleTransformation('lowercase')}
                >
                  LOWER
                </Button>
              </Space>
              <Button
                block
                size="small"
                onClick={() => handleTransformation('trim')}
              >
                TRIM
              </Button>

              <Divider style={{ margin: '8px 0' }} />

              <div style={{ fontSize: 12, fontWeight: 'bold' }}>Type Casting</div>
              <Select
                size="small"
                value={castType}
                onChange={setCastType}
                style={{ width: '100%' }}
                options={[
                  { label: 'VARCHAR', value: 'VARCHAR' },
                  { label: 'INTEGER', value: 'INTEGER' },
                  { label: 'BIGINT', value: 'BIGINT' },
                  { label: 'DOUBLE', value: 'DOUBLE' },
                  { label: 'TIMESTAMP', value: 'TIMESTAMP' },
                  { label: 'DATE', value: 'DATE' },
                ]}
              />
              <Button
                block
                size="small"
                onClick={() => handleTransformation('cast')}
              >
                CAST
              </Button>

              <Divider style={{ margin: '8px 0' }} />

              <div style={{ fontSize: 12, fontWeight: 'bold' }}>String Operations</div>
              <div style={{ fontSize: 11, marginBottom: 8 }}>
                Length:
                <InputNumber
                  min={1}
                  max={1000}
                  value={truncateLength}
                  onChange={(v) => setTruncateLength(v ?? 50)}
                  style={{ width: '100%', marginTop: 4 }}
                  size="small"
                />
              </div>
              <Space style={{ width: '100%' }}>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  onClick={() => handleTransformation('truncate')}
                >
                  TRUNCATE
                </Button>
                <Button
                  size="small"
                  style={{ flex: 1 }}
                  onClick={() => handleTransformation('substring')}
                >
                  SUBSTRING
                </Button>
              </Space>
            </Space>
          ),
        },
        {
          key: 'export',
          label: 'Export',
          children: (
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button
                block
                size="small"
                onClick={handleCopyToClipboard}
              >
                Copy Column Values
              </Button>
              <Button
                block
                size="small"
                onClick={handleExportCsv}
              >
                Export as CSV
              </Button>
            </Space>
          ),
        },
      ]}
    />
  );
}
