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
import { useState, useMemo, useCallback, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Row,
  Col,
  Input,
  Button,
  Space,
  Tag,
  Popconfirm,
  message,
  Typography,
  Tooltip,
  Empty,
  Spin,
  Modal,
  Alert,
  Checkbox,
} from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
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
  EyeOutlined,
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
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getVisualizations, deleteVisualization } from '../api/visualizations';
import { executeQuery } from '../api/queries';
import { getEffectiveQuery } from '../utils/sqlTransformations';
import { ChartPreview, VisualizationEditor } from '../components/visualization';
import { useCurrentUser } from '../hooks/useCurrentUser';
import type { Visualization, ChartType, QueryResult } from '../types';
import AddToProjectModal from '../components/common/AddToProjectModal';
import BulkActionBar from '../components/common/BulkActionBar';
import BulkAddToProjectModal from '../components/common/BulkAddToProjectModal';

const { Title, Text, Paragraph } = Typography;

// Chart type icons mapping
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

// Chart type colors
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

/**
 * Mini chart preview for visualization cards.
 * Fetches the viz SQL and renders a small chart thumbnail.
 */
function MiniVizPreview({ viz }: { viz: Visualization }) {
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);

  // Serialize config to a stable string so the effect re-fires when config changes
  // (e.g. time grain or aggregation settings updated)
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
      .then((query) => {
        if (cancelled) {
          return;
        }
        return executeQuery({
          query,
          queryType: 'SQL',
          autoLimitRowCount: 100,
          defaultSchema: viz.defaultSchema,
        });
      })
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

  // Fall back to icon on failure or no SQL
  if (failed || (!loading && !queryResult)) {
    return (
      <div
        style={{
          height: 140,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          backgroundColor: (chartColors[viz.chartType] || '#5470c6') + '20',
        }}
      >
        <span style={{ fontSize: 48, color: chartColors[viz.chartType] || '#5470c6' }}>
          {chartIcons[viz.chartType] || <BarChartOutlined />}
        </span>
      </div>
    );
  }

  return (
    <div
      style={{
        height: 140,
        backgroundColor: 'var(--color-bg-container)',
        overflow: 'hidden',
      }}
    >
      <ChartPreview
        chartType={viz.chartType as ChartType}
        config={viz.config}
        data={queryResult}
        loading={loading}
        height={140}
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
  const queryClient = useQueryClient();
  const { canEdit, canView, isProjectOwner } = useCurrentUser();
  const [searchText, setSearchText] = useState('');
  const [editBuilderViz, setEditBuilderViz] = useState<Visualization | null>(null);
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewingViz, setViewingViz] = useState<Visualization | null>(null);
  const [viewQueryResult, setViewQueryResult] = useState<QueryResult | null>(null);
  const [viewQueryLoading, setViewQueryLoading] = useState(false);
  const [viewQueryError, setViewQueryError] = useState<string | null>(null);
  const [showSql, setShowSql] = useState(false);
  const [addToProjectId, setAddToProjectId] = useState<string | null>(null);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [bulkProjectModalOpen, setBulkProjectModalOpen] = useState(false);

  // Fetch visualizations
  const { data: visualizations, isLoading, error } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: deleteVisualization,
    onSuccess: () => {
      message.success('Visualization deleted successfully');
      queryClient.invalidateQueries({ queryKey: ['visualizations'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to delete visualization: ${error.message}`);
    },
  });

  // Filter visualizations based on filterIds (project scope) and search text
  const filteredVisualizations = useMemo(() => {
    if (!visualizations) {
      return [];
    }
    let result = visualizations;
    // Visibility: show own visualizations + public visualizations (admins/anonymous see all)
    result = result.filter((v) => canView(v.owner, v.isPublic));
    if (filterIds) {
      const idSet = new Set(filterIds);
      result = result.filter((v) => idSet.has(v.id));
    }
    if (searchText) {
      const lowerSearch = searchText.toLowerCase();
      result = result.filter(
        (v) =>
          v.name.toLowerCase().includes(lowerSearch) ||
          v.chartType.toLowerCase().includes(lowerSearch) ||
          (v.description && v.description.toLowerCase().includes(lowerSearch))
      );
    }
    return result;
  }, [visualizations, searchText, filterIds, canView]);

  // Handle view - execute SQL and render chart
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

  // Format timestamp
  const formatDate = (timestamp: number | string) => {
    const date = typeof timestamp === 'number'
      ? new Date(timestamp)
      : new Date(timestamp);
    return date.toLocaleDateString();
  };

  if (error) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty
            description={
              <Text type="danger">
                Failed to load visualizations: {(error as Error).message}
              </Text>
            }
          />
        </Card>
      </div>
    );
  }

  return (
    <div style={{ padding: 24 }}>
      <Card>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* Header */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Title level={3} style={{ margin: 0 }}>
              Visualizations
            </Title>
            <Space>
              {onAdd && (
                <Button onClick={onAdd}>
                  Add Existing
                </Button>
              )}
              <Button type="primary" onClick={() => navigate(projectId ? `/projects/${projectId}/query` : '/sqllab')}>
                Create from Query
              </Button>
            </Space>
          </div>

          {/* Search */}
          <Input
            placeholder="Search visualizations by name, type, or description..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            allowClear
            style={{ maxWidth: 400 }}
          />

          {/* Bulk Action Bar */}
          <BulkActionBar
            selectedCount={selectedIds.length}
            onAddToProject={!projectId ? () => setBulkProjectModalOpen(true) : undefined}
            onDelete={() => {
              selectedIds.forEach(id => deleteMutation.mutate(id));
              setSelectedIds([]);
            }}
            onClear={() => setSelectedIds([])}
          />

          {/* Visualization Cards */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredVisualizations.length === 0 ? (
            searchText ? (
              <Empty
                image={<BarChartOutlined style={{ fontSize: 64, color: 'var(--color-text-tertiary)' }} />}
                description="No visualizations match your search"
              />
            ) : onAdd ? (
              <Empty
                image={<BarChartOutlined style={{ fontSize: 64, color: 'var(--color-text-tertiary)' }} />}
                description="No visualizations in this project yet"
              >
                <Space>
                  <Button onClick={onAdd}>Add Existing</Button>
                  <Button type="primary" onClick={() => navigate(`/projects/${projectId}/query`)}>
                    Create from Query
                  </Button>
                </Space>
              </Empty>
            ) : (
              <Empty
                image={<BarChartOutlined style={{ fontSize: 64, color: 'var(--color-text-tertiary)' }} />}
                description="No visualizations yet. Run a query and create one!"
              />
            )
          ) : (
            <Row gutter={[16, 16]}>
              {filteredVisualizations.map((viz) => (
                <Col xs={24} sm={12} md={8} lg={6} key={viz.id}>
                  <Card
                    hoverable
                    size="small"
                    style={selectedIds.includes(viz.id) ? { border: '2px solid var(--color-primary)' } : undefined}
                    cover={
                      <div style={{ position: 'relative' }}>
                        <div
                          style={{ position: 'absolute', top: 8, left: 8, zIndex: 1 }}
                          onClick={(e) => e.stopPropagation()}
                        >
                          <Checkbox
                            checked={selectedIds.includes(viz.id)}
                            onChange={(e) => {
                              if (e.target.checked) {
                                setSelectedIds(prev => [...prev, viz.id]);
                              } else {
                                setSelectedIds(prev => prev.filter(id => id !== viz.id));
                              }
                            }}
                          />
                        </div>
                        <MiniVizPreview viz={viz} />
                      </div>
                    }
                    actions={(() => {
                      const editable = canEdit(viz.owner) || (projectOwner ? isProjectOwner(projectOwner) : false);
                      return [
                        <Tooltip title="View" key="view">
                          <EyeOutlined onClick={() => handleView(viz)} />
                        </Tooltip>,
                        ...(editable ? [
                          <Tooltip title="Edit" key="edit">
                            <EditOutlined onClick={() => setEditBuilderViz(viz)} />
                          </Tooltip>,
                        ] : []),
                        ...(!projectId ? [
                          <Tooltip title="Add to Project" key="addToProject">
                            <FolderOutlined onClick={() => setAddToProjectId(viz.id)} />
                          </Tooltip>,
                        ] : []),
                        ...(editable ? [
                          <Popconfirm
                            key="delete"
                            title="Delete this visualization?"
                            description="This action cannot be undone."
                            onConfirm={() => deleteMutation.mutate(viz.id)}
                            okText="Delete"
                            cancelText="Cancel"
                            okButtonProps={{ danger: true }}
                          >
                            <Tooltip title="Delete">
                              <DeleteOutlined style={{ color: '#ff4d4f' }} />
                            </Tooltip>
                          </Popconfirm>,
                        ] : []),
                      ];
                    })()}
                  >
                    <Card.Meta
                      title={
                        <Space>
                          <Text strong ellipsis style={{ maxWidth: 150 }}>
                            {viz.name}
                          </Text>
                          {viz.isPublic ? (
                            <GlobalOutlined style={{ color: '#52c41a', fontSize: 12 }} />
                          ) : (
                            <LockOutlined style={{ color: '#faad14', fontSize: 12 }} />
                          )}
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={0}>
                          <Tag color={chartColors[viz.chartType]} style={{ marginBottom: 4 }}>
                            {viz.chartType}
                          </Tag>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            <UserOutlined /> {viz.owner}
                          </Text>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            {formatDate(viz.updatedAt)}
                          </Text>
                        </Space>
                      }
                    />
                  </Card>
                </Col>
              ))}
            </Row>
          )}
        </Space>
      </Card>

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

      {/* Bulk Add to Project Modal */}
      <BulkAddToProjectModal
        open={bulkProjectModalOpen}
        onClose={() => setBulkProjectModalOpen(false)}
        itemIds={selectedIds}
        itemType="visualization"
      />

      {/* View Modal - renders actual chart */}
      <Modal
        title={
          <Space>
            <span style={{ color: chartColors[viewingViz?.chartType || 'bar'] }}>
              {chartIcons[viewingViz?.chartType || 'bar']}
            </span>
            {viewingViz?.name || 'Visualization'}
          </Space>
        }
        open={viewModalOpen}
        onCancel={() => {
          setViewModalOpen(false);
          setViewingViz(null);
          setViewQueryResult(null);
          setViewQueryError(null);
          setShowSql(false);
        }}
        footer={
          <Space>
            {viewingViz?.sql && (
              <Button
                icon={<CodeOutlined />}
                onClick={() => setShowSql(!showSql)}
              >
                {showSql ? 'Hide SQL' : 'Show SQL'}
              </Button>
            )}
            {viewingViz?.sql && (
              <Button
                icon={<PlayCircleOutlined />}
                onClick={() => {
                  if (viewingViz) {
                    handleView(viewingViz);
                  }
                }}
              >
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
            <Button onClick={() => {
              setViewModalOpen(false);
              setViewingViz(null);
              setViewQueryResult(null);
              setViewQueryError(null);
              setShowSql(false);
            }}>
              Close
            </Button>
          </Space>
        }
        width={800}
        destroyOnClose
      >
        {viewingViz && (
          <div>
            {/* Description */}
            {viewingViz.description && (
              <Paragraph type="secondary" style={{ marginBottom: 12 }}>
                {viewingViz.description}
              </Paragraph>
            )}

            {/* Metadata row */}
            <Space style={{ marginBottom: 16 }} wrap>
              <Tag color={chartColors[viewingViz.chartType]}>{viewingViz.chartType}</Tag>
              <Text type="secondary">
                <UserOutlined /> {viewingViz.owner}
              </Text>
              {viewingViz.isPublic ? (
                <Tag color="green" icon={<GlobalOutlined />}>Public</Tag>
              ) : (
                <Tag color="orange" icon={<LockOutlined />}>Private</Tag>
              )}
              <Text type="secondary">Updated: {formatDate(viewingViz.updatedAt)}</Text>
            </Space>

            {/* SQL display (toggle) */}
            {showSql && viewingViz.sql && (
              <pre
                style={{
                  background: 'var(--color-bg-elevated)',
                  padding: 12,
                  borderRadius: 4,
                  marginBottom: 16,
                  maxHeight: 150,
                  overflow: 'auto',
                  fontFamily: 'monospace',
                  fontSize: 12,
                  border: '1px solid var(--color-border)',
                }}
              >
                {viewingViz.sql}
              </pre>
            )}

            {/* Error display */}
            {viewQueryError && (
              <Alert
                type="warning"
                message={viewQueryError}
                style={{ marginBottom: 16 }}
                showIcon
              />
            )}

            {/* Chart rendering */}
            <div style={{ border: '1px solid var(--color-border)', borderRadius: 8, overflow: 'hidden' }}>
              <ChartPreview
                chartType={viewingViz.chartType as ChartType}
                config={viewingViz.config}
                data={viewQueryResult}
                loading={viewQueryLoading}
                height={400}
                sql={viewingViz.sql}
              />
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
