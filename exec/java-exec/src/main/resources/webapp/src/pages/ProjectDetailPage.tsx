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
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  Tabs,
  Button,
  Space,
  Typography,
  Spin,
  Empty,
  Tag,
  List,
  Popconfirm,
  message,
  Descriptions,
  Tooltip,
} from 'antd';
import {
  ArrowLeftOutlined,
  DatabaseOutlined,
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
  FileTextOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  DeleteOutlined,
  PlusOutlined,
  ShareAltOutlined,
  EditOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getProject,
  removeDataset,
  removeSavedQuery,
  removeVisualization,
  removeDashboard,
  deleteWikiPage,
} from '../api/projects';
import { getSavedQueries } from '../api/savedQueries';
import { getVisualizations } from '../api/visualizations';
import { getDashboards } from '../api/dashboards';
import type { SavedQuery, Visualization, Dashboard } from '../types';
import { AddItemModal, ShareModal, WikiEditor, DatasetPickerModal } from '../components/project';

const { Title, Text, Paragraph } = Typography;

export default function ProjectDetailPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState('overview');
  const [addItemType, setAddItemType] = useState<'savedQuery' | 'visualization' | 'dashboard' | null>(null);
  const [shareModalOpen, setShareModalOpen] = useState(false);
  const [wikiEditorOpen, setWikiEditorOpen] = useState(false);
  const [editingWikiPageId, setEditingWikiPageId] = useState<string | null>(null);
  const [datasetRefModalOpen, setDatasetRefModalOpen] = useState(false);

  const { data: project, isLoading, error } = useQuery({
    queryKey: ['project', id],
    queryFn: () => getProject(id!),
    enabled: !!id,
  });

  const { data: allSavedQueries } = useQuery({
    queryKey: ['saved-queries'],
    queryFn: getSavedQueries,
  });

  const { data: allVisualizations } = useQuery({
    queryKey: ['visualizations'],
    queryFn: getVisualizations,
  });

  const { data: allDashboards } = useQuery({
    queryKey: ['dashboards'],
    queryFn: getDashboards,
  });

  const removeDatasetMutation = useMutation({
    mutationFn: (datasetId: string) => removeDataset(id!, datasetId),
    onSuccess: () => {
      message.success('Dataset removed');
      queryClient.invalidateQueries({ queryKey: ['project', id] });
    },
  });

  const removeSavedQueryMutation = useMutation({
    mutationFn: (queryId: string) => removeSavedQuery(id!, queryId),
    onSuccess: () => {
      message.success('Query removed');
      queryClient.invalidateQueries({ queryKey: ['project', id] });
    },
  });

  const removeVisualizationMutation = useMutation({
    mutationFn: (vizId: string) => removeVisualization(id!, vizId),
    onSuccess: () => {
      message.success('Visualization removed');
      queryClient.invalidateQueries({ queryKey: ['project', id] });
    },
  });

  const removeDashboardMutation = useMutation({
    mutationFn: (dashId: string) => removeDashboard(id!, dashId),
    onSuccess: () => {
      message.success('Dashboard removed');
      queryClient.invalidateQueries({ queryKey: ['project', id] });
    },
  });

  const deleteWikiMutation = useMutation({
    mutationFn: (pageId: string) => deleteWikiPage(id!, pageId),
    onSuccess: () => {
      message.success('Wiki page deleted');
      queryClient.invalidateQueries({ queryKey: ['project', id] });
    },
  });

  const formatDate = (timestamp: number) => {
    return new Date(timestamp).toLocaleString();
  };

  // Resolve saved queries that belong to this project
  const projectSavedQueries: SavedQuery[] = (allSavedQueries || []).filter(
    (q) => project?.savedQueryIds?.includes(q.id)
  );

  const projectVisualizations: Visualization[] = (allVisualizations || []).filter(
    (v) => project?.visualizationIds?.includes(v.id)
  );

  const projectDashboards: Dashboard[] = (allDashboards || []).filter(
    (d) => project?.dashboardIds?.includes(d.id)
  );

  if (isLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (error || !project) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty description={
            <Text type="danger">
              {error ? `Failed to load project: ${(error as Error).message}` : 'Project not found'}
            </Text>
          } />
          <div style={{ textAlign: 'center', marginTop: 16 }}>
            <Button onClick={() => navigate('/projects')}>Back to Projects</Button>
          </div>
        </Card>
      </div>
    );
  }

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Descriptions column={2} bordered size="small">
            <Descriptions.Item label="Owner">
              <UserOutlined /> {project.owner}
            </Descriptions.Item>
            <Descriptions.Item label="Visibility">
              {project.isPublic ? (
                <><GlobalOutlined style={{ color: '#52c41a' }} /> Public</>
              ) : (
                <><LockOutlined style={{ color: '#faad14' }} /> Private</>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="Created">
              {formatDate(project.createdAt)}
            </Descriptions.Item>
            <Descriptions.Item label="Updated">
              {formatDate(project.updatedAt)}
            </Descriptions.Item>
            {project.sharedWith && project.sharedWith.length > 0 && (
              <Descriptions.Item label="Shared With" span={2}>
                {project.sharedWith.map((user) => (
                  <Tag key={user}>{user}</Tag>
                ))}
              </Descriptions.Item>
            )}
          </Descriptions>
          {project.description && (
            <Card size="small" title="Description">
              <Paragraph>{project.description}</Paragraph>
            </Card>
          )}
          <Card size="small" title="Contents Summary">
            <Space size="large">
              <Text><DatabaseOutlined /> {project.datasets?.length || 0} Datasets</Text>
              <Text><CodeOutlined /> {project.savedQueryIds?.length || 0} Queries</Text>
              <Text><BarChartOutlined /> {project.visualizationIds?.length || 0} Visualizations</Text>
              <Text><DashboardOutlined /> {project.dashboardIds?.length || 0} Dashboards</Text>
              <Text><FileTextOutlined /> {project.wikiPages?.length || 0} Wiki Pages</Text>
            </Space>
          </Card>
        </Space>
      ),
    },
    {
      key: 'datasets',
      label: (
        <span><DatabaseOutlined /> Datasets ({project.datasets?.length || 0})</span>
      ),
      children: (
        <Space direction="vertical" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setDatasetRefModalOpen(true)}
            >
              Add Dataset
            </Button>
          </div>
          {(!project.datasets || project.datasets.length === 0) ? (
            <Empty description="No datasets added yet" />
          ) : (
            <List
              dataSource={project.datasets}
              renderItem={(dataset) => (
                <List.Item
                  actions={[
                    dataset.type !== 'saved_query' && dataset.schema ? (
                      <Tooltip title="Data source settings" key="settings">
                        <Button
                          type="text"
                          icon={<SettingOutlined />}
                          size="small"
                          onClick={() => navigate(`/datasources/${encodeURIComponent(dataset.schema!.split('.')[0])}`)}
                        />
                      </Tooltip>
                    ) : null,
                    <Popconfirm
                      key="remove"
                      title="Remove this dataset?"
                      onConfirm={() => removeDatasetMutation.mutate(dataset.id)}
                      okText="Remove"
                      cancelText="Cancel"
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ].filter(Boolean)}
                >
                  <List.Item.Meta
                    avatar={<DatabaseOutlined style={{ fontSize: 20 }} />}
                    title={dataset.label || dataset.schema || '(unnamed)'}
                    description={
                      dataset.type === 'plugin'
                        ? `All schemas in ${dataset.schema}`
                        : dataset.type === 'schema'
                        ? `All tables in ${dataset.schema}`
                        : dataset.type === 'saved_query'
                        ? `Saved Query: ${dataset.savedQueryId}`
                        : `${dataset.schema}.${dataset.table}`
                    }
                  />
                  <Tag>
                    {dataset.type === 'plugin' ? 'Plugin' :
                     dataset.type === 'schema' ? 'Schema' :
                     dataset.type === 'saved_query' ? 'Saved Query' : 'Table'}
                  </Tag>
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
    {
      key: 'queries',
      label: (
        <span><CodeOutlined /> Queries ({project.savedQueryIds?.length || 0})</span>
      ),
      children: (
        <Space direction="vertical" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setAddItemType('savedQuery')}
            >
              Add Query
            </Button>
          </div>
          {projectSavedQueries.length === 0 ? (
            <Empty description="No saved queries added yet" />
          ) : (
            <List
              dataSource={projectSavedQueries}
              renderItem={(query) => (
                <List.Item
                  actions={[
                    <Popconfirm
                      key="remove"
                      title="Remove this query from the project?"
                      onConfirm={() => removeSavedQueryMutation.mutate(query.id)}
                      okText="Remove"
                      cancelText="Cancel"
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ]}
                >
                  <List.Item.Meta
                    avatar={<CodeOutlined style={{ fontSize: 20 }} />}
                    title={query.name}
                    description={query.description || query.sql.substring(0, 100)}
                  />
                  {query.isPublic ? (
                    <Tag color="green">Public</Tag>
                  ) : (
                    <Tag>Private</Tag>
                  )}
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
    {
      key: 'visualizations',
      label: (
        <span><BarChartOutlined /> Visualizations ({project.visualizationIds?.length || 0})</span>
      ),
      children: (
        <Space direction="vertical" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setAddItemType('visualization')}
            >
              Add Visualization
            </Button>
          </div>
          {projectVisualizations.length === 0 ? (
            <Empty description="No visualizations added yet" />
          ) : (
            <List
              dataSource={projectVisualizations}
              renderItem={(viz) => (
                <List.Item
                  actions={[
                    <Popconfirm
                      key="remove"
                      title="Remove this visualization from the project?"
                      onConfirm={() => removeVisualizationMutation.mutate(viz.id)}
                      okText="Remove"
                      cancelText="Cancel"
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ]}
                >
                  <List.Item.Meta
                    avatar={<BarChartOutlined style={{ fontSize: 20 }} />}
                    title={viz.name}
                    description={viz.description || `${viz.chartType} chart`}
                  />
                  <Tag color="blue">{viz.chartType}</Tag>
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
    {
      key: 'dashboards',
      label: (
        <span><DashboardOutlined /> Dashboards ({project.dashboardIds?.length || 0})</span>
      ),
      children: (
        <Space direction="vertical" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setAddItemType('dashboard')}
            >
              Add Dashboard
            </Button>
          </div>
          {projectDashboards.length === 0 ? (
            <Empty description="No dashboards added yet" />
          ) : (
            <List
              dataSource={projectDashboards}
              renderItem={(dash) => (
                <List.Item
                  actions={[
                    <Button
                      key="view"
                      type="link"
                      size="small"
                      onClick={() => navigate(`/dashboards/${dash.id}`)}
                    >
                      View
                    </Button>,
                    <Popconfirm
                      key="remove"
                      title="Remove this dashboard from the project?"
                      onConfirm={() => removeDashboardMutation.mutate(dash.id)}
                      okText="Remove"
                      cancelText="Cancel"
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ]}
                >
                  <List.Item.Meta
                    avatar={<DashboardOutlined style={{ fontSize: 20 }} />}
                    title={dash.name}
                    description={dash.description || `${dash.panels?.length || 0} panels`}
                  />
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
    {
      key: 'wiki',
      label: (
        <span><FileTextOutlined /> Wiki ({project.wikiPages?.length || 0})</span>
      ),
      children: (
        <Space direction="vertical" style={{ width: '100%' }}>
          <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => {
                setEditingWikiPageId(null);
                setWikiEditorOpen(true);
              }}
            >
              New Page
            </Button>
          </div>
          {(!project.wikiPages || project.wikiPages.length === 0) ? (
            <Empty description="No wiki pages yet" />
          ) : (
            <List
              dataSource={[...project.wikiPages].sort((a, b) => a.order - b.order)}
              renderItem={(page) => (
                <List.Item
                  actions={[
                    <Tooltip title="Edit" key="edit">
                      <Button
                        type="text"
                        icon={<EditOutlined />}
                        size="small"
                        onClick={() => {
                          setEditingWikiPageId(page.id);
                          setWikiEditorOpen(true);
                        }}
                      />
                    </Tooltip>,
                    <Popconfirm
                      key="delete"
                      title="Delete this wiki page?"
                      onConfirm={() => deleteWikiMutation.mutate(page.id)}
                      okText="Delete"
                      cancelText="Cancel"
                      okButtonProps={{ danger: true }}
                    >
                      <Button type="text" danger icon={<DeleteOutlined />} size="small" />
                    </Popconfirm>,
                  ]}
                >
                  <List.Item.Meta
                    avatar={<FileTextOutlined style={{ fontSize: 20 }} />}
                    title={
                      <a onClick={() => {
                        setEditingWikiPageId(page.id);
                        setWikiEditorOpen(true);
                      }}>
                        {page.title}
                      </a>
                    }
                    description={`Updated: ${formatDate(page.updatedAt)}`}
                  />
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: 24, overflow: 'auto', flex: 1 }}>
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* Header */}
        <Card>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <Space direction="vertical" size={4}>
              <Space>
                <Button
                  type="text"
                  icon={<ArrowLeftOutlined />}
                  onClick={() => navigate('/projects')}
                />
                <Title level={3} style={{ margin: 0 }}>
                  {project.name}
                </Title>
                {project.isPublic ? (
                  <Tag color="green" icon={<GlobalOutlined />}>Public</Tag>
                ) : (
                  <Tag icon={<LockOutlined />}>Private</Tag>
                )}
              </Space>
              {project.tags && project.tags.length > 0 && (
                <div style={{ marginLeft: 40 }}>
                  {project.tags.map((tag) => (
                    <Tag key={tag} color="blue">{tag}</Tag>
                  ))}
                </div>
              )}
            </Space>
            <Space>
              <Button
                icon={<ShareAltOutlined />}
                onClick={() => setShareModalOpen(true)}
              >
                Share
              </Button>
            </Space>
          </div>
        </Card>

        {/* Tabbed Content */}
        <Card>
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            items={tabItems}
          />
        </Card>
      </Space>

      {/* Modals */}
      <AddItemModal
        open={addItemType !== null}
        type={addItemType || 'savedQuery'}
        projectId={id!}
        existingIds={
          addItemType === 'savedQuery' ? project.savedQueryIds :
          addItemType === 'visualization' ? project.visualizationIds :
          addItemType === 'dashboard' ? project.dashboardIds :
          []
        }
        items={
          addItemType === 'savedQuery'
            ? (allSavedQueries || []).map((q) => ({ id: q.id, name: q.name, description: q.description }))
            : addItemType === 'visualization'
            ? (allVisualizations || []).map((v) => ({ id: v.id, name: v.name, description: v.description }))
            : (allDashboards || []).map((d) => ({ id: d.id, name: d.name, description: d.description }))
        }
        onClose={() => setAddItemType(null)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', id] });
          setAddItemType(null);
        }}
      />

      <ShareModal
        open={shareModalOpen}
        project={project}
        onClose={() => setShareModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', id] });
          setShareModalOpen(false);
        }}
      />

      <WikiEditor
        open={wikiEditorOpen}
        projectId={id!}
        page={editingWikiPageId
          ? project.wikiPages?.find((p) => p.id === editingWikiPageId) || null
          : null}
        onClose={() => {
          setWikiEditorOpen(false);
          setEditingWikiPageId(null);
        }}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', id] });
          setWikiEditorOpen(false);
          setEditingWikiPageId(null);
        }}
      />

      <DatasetPickerModal
        open={datasetRefModalOpen}
        projectId={id!}
        existingDatasets={project.datasets || []}
        onClose={() => setDatasetRefModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', id] });
          setDatasetRefModalOpen(false);
        }}
      />
    </div>
  );
}
