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
import { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  Button,
  Card,
  Space,
  Spin,
  Alert,
  Descriptions,
  Tag,
  Empty,
  Collapse,
  Input,
  Modal,
  message,
} from 'antd';
import {
  ArrowLeftOutlined,
  EditOutlined,
  PlayCircleOutlined,
  CopyOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import { getVisualization, deleteVisualization } from '../api/visualizations';
import { executeQuery } from '../api/queries';
import { getEffectiveQuery } from '../utils/sqlTransformations';
import ChartPreview from '../components/visualization/ChartPreview';
import type { QueryResult } from '../types';

const chartColors: Record<string, string> = {
  bar: '#1890ff',
  line: '#52c41a',
  pie: '#faad14',
  scatter: '#f5222d',
  table: '#722ed1',
  number: '#eb2f96',
};

interface VisualizationDetailPageProps {
  projectId?: string;
}

export default function VisualizationDetailPage({ projectId: propProjectId }: VisualizationDetailPageProps = {}) {
  const { vizId } = useParams<{ vizId: string }>();
  const navigate = useNavigate();
  const projectId = propProjectId;
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const ranQueryRef = useRef<string | null>(null);

  const { data: viz, isLoading, error } = useQuery({
    queryKey: ['visualization', vizId],
    queryFn: () => getVisualization(vizId || ''),
    enabled: !!vizId,
  });

  const handleRunQuery = useCallback(async () => {
    if (!viz?.sql) {
      message.warning('No SQL query available');
      return;
    }

    setQueryLoading(true);
    setQueryError(null);
    setQueryResult(null);

    try {
      const query = await getEffectiveQuery(viz.sql, viz.config || {});
      const result = await executeQuery({
        query,
        queryType: 'SQL',
        autoLimitRowCount: 10000,
        defaultSchema: viz.defaultSchema || 'public',
      });
      setQueryResult(result);
    } catch (err) {
      setQueryError(err instanceof Error ? err.message : 'Failed to run query');
    } finally {
      setQueryLoading(false);
    }
  }, [viz?.sql, viz?.config, viz?.defaultSchema]);

  // Auto-run query when visualization loads
  useEffect(() => {
    if (viz?.sql && viz.id !== ranQueryRef.current) {
      ranQueryRef.current = viz.id;
      handleRunQuery();
    }
  }, [viz?.id, viz?.sql, handleRunQuery]);

  const handleDelete = () => {
    if (!viz?.id) return;

    Modal.confirm({
      title: 'Delete Visualization?',
      content: `Are you sure you want to delete "${viz.name}"? This cannot be undone.`,
      okText: 'Delete',
      okButtonProps: { danger: true },
      onOk: async () => {
        try {
          if (viz.id) {
            await deleteVisualization(viz.id);
          }
          message.success('Visualization deleted');
          navigate('/visualizations');
        } catch (err) {
          message.error(err instanceof Error ? err.message : 'Failed to delete visualization');
        }
      },
    });
  };

  if (isLoading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '100vh' }}>
        <Spin size="large" tip="Loading visualization..." />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/visualizations')}>
          Back to Visualizations
        </Button>
        <Alert
          type="error"
          message="Failed to load visualization"
          description={error instanceof Error ? error.message : 'Unknown error'}
          style={{ marginTop: 24 }}
        />
      </div>
    );
  }

  if (!viz) {
    return (
      <div style={{ padding: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/visualizations')}>
          Back to Visualizations
        </Button>
        <Empty description="Visualization not found" style={{ marginTop: 40 }} />
      </div>
    );
  }

  const handleBack = () => {
    if (projectId) {
      navigate(`/projects/${projectId}/visualizations`);
    } else {
      navigate('/visualizations');
    }
  };

  return (
    <div style={{ padding: '24px' }}>
      <Space style={{ marginBottom: '24px' }}>
        <Button icon={<ArrowLeftOutlined />} onClick={handleBack}>
          Back
        </Button>
      </Space>

      <Card
        title={
          <Space>
            <span>{viz.name}</span>
            <Tag color={chartColors[viz.chartType]}>{viz.chartType}</Tag>
          </Space>
        }
        extra={
          <Space>
            <Button
              icon={<PlayCircleOutlined />}
              type="primary"
              onClick={handleRunQuery}
              loading={queryLoading}
              disabled={!viz.sql}
            >
              Run Query
            </Button>
            <Button icon={<EditOutlined />} onClick={() => navigate(`/visualizations/${viz.id}/edit`)}>
              Edit
            </Button>
            <Button danger icon={<DeleteOutlined />} onClick={handleDelete}>
              Delete
            </Button>
          </Space>
        }
      >
        <Descriptions column={2}>
          <Descriptions.Item label="Type">{viz.chartType}</Descriptions.Item>
          <Descriptions.Item label="Owner">{viz.owner || 'Unknown'}</Descriptions.Item>
          <Descriptions.Item label="Status">
            <Tag color={viz.isPublic ? 'green' : 'orange'}>{viz.isPublic ? 'Public' : 'Private'}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="Created">{new Date(viz.createdAt || '').toLocaleString()}</Descriptions.Item>
        </Descriptions>

        {viz.description && (
          <div style={{ marginTop: '16px' }}>
            <strong>Description:</strong>
            <p>{viz.description}</p>
          </div>
        )}
      </Card>

      {viz.sql && (
        <Card title="SQL Query" style={{ marginTop: '24px' }}>
          <Collapse
            items={[
              {
                key: '1',
                label: 'SQL Query',
                children: (
                  <div>
                    <Input.TextArea
                      value={viz.sql}
                      readOnly
                      rows={10}
                      style={{ marginBottom: '12px', fontFamily: 'monospace' }}
                    />
                    <Button
                      icon={<CopyOutlined />}
                      onClick={() => {
                        if (viz.sql) {
                          navigator.clipboard.writeText(viz.sql);
                          message.success('SQL copied to clipboard');
                        }
                      }}
                    >
                      Copy
                    </Button>
                  </div>
                ),
              },
            ]}
          />
        </Card>
      )}

      {queryError && (
        <Alert
          type="error"
          message="Query Failed"
          description={queryError}
          closable
          onClose={() => setQueryError(null)}
          style={{ marginTop: '24px' }}
        />
      )}

      {(queryLoading || queryResult) && (
        <Card title="Visualization" style={{ marginTop: '24px' }}>
          <ChartPreview
            chartType={viz.chartType}
            config={viz.config || {}}
            data={queryResult}
            loading={queryLoading}
            height={600}
          />
        </Card>
      )}
    </div>
  );
}
