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
import {
  Card,
  Tabs,
  Button,
  Space,
  Typography,
  Tag,
  List,
  Popconfirm,
  message,
  Descriptions,
  Tooltip,
} from 'antd';
import {
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
  DownloadOutlined,
} from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  removeDataset,
  deleteWikiPage,
} from '../api/projects';
import { useProjectContext } from '../contexts/ProjectContext';
import { ShareModal, WikiEditor, DatasetPickerModal, ProjectActivityFeed, ProjectLineage, ExportImportModal } from '../components/project';

const { Title, Text, Paragraph } = Typography;

export default function ProjectDetailPage() {
  const { project, projectId } = useProjectContext();
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState('overview');
  const [shareModalOpen, setShareModalOpen] = useState(false);
  const [wikiEditorOpen, setWikiEditorOpen] = useState(false);
  const [editingWikiPageId, setEditingWikiPageId] = useState<string | null>(null);
  const [datasetRefModalOpen, setDatasetRefModalOpen] = useState(false);
  const [exportModalOpen, setExportModalOpen] = useState(false);

  const removeDatasetMutation = useMutation({
    mutationFn: (datasetId: string) => removeDataset(projectId!, datasetId),
    onSuccess: () => {
      message.success('Dataset removed');
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
    },
  });

  const deleteWikiMutation = useMutation({
    mutationFn: (pageId: string) => deleteWikiPage(projectId!, pageId),
    onSuccess: () => {
      message.success('Wiki page deleted');
      queryClient.invalidateQueries({ queryKey: ['project', projectId] });
    },
  });

  const formatDate = (timestamp: number) => {
    return new Date(timestamp).toLocaleString();
  };

  if (!project) {
    return null;
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
      key: 'activity',
      label: 'Activity',
      children: <ProjectActivityFeed project={project} />,
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
            <span>No datasets added yet</span>
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
            <span>No wiki pages yet</span>
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
                    title={page.title}
                    description={`Updated: ${formatDate(page.updatedAt)}`}
                  />
                </List.Item>
              )}
            />
          )}
        </Space>
      ),
    },
    {
      key: 'lineage',
      label: 'Lineage',
      children: <ProjectLineage project={project} />,
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
                <Title level={3} style={{ margin: 0 }}>
                  Settings
                </Title>
                {project.isPublic ? (
                  <Tag color="green" icon={<GlobalOutlined />}>Public</Tag>
                ) : (
                  <Tag icon={<LockOutlined />}>Private</Tag>
                )}
              </Space>
              {project.tags && project.tags.length > 0 && (
                <div>
                  {project.tags.map((tag) => (
                    <Tag key={tag} color="blue">{tag}</Tag>
                  ))}
                </div>
              )}
            </Space>
            <Space>
              <Button
                icon={<DownloadOutlined />}
                onClick={() => setExportModalOpen(true)}
              >
                Export
              </Button>
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
      <ShareModal
        open={shareModalOpen}
        project={project}
        onClose={() => setShareModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setShareModalOpen(false);
        }}
      />

      <WikiEditor
        open={wikiEditorOpen}
        projectId={projectId!}
        page={editingWikiPageId
          ? project.wikiPages?.find((p) => p.id === editingWikiPageId) || null
          : null}
        onClose={() => {
          setWikiEditorOpen(false);
          setEditingWikiPageId(null);
        }}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setWikiEditorOpen(false);
          setEditingWikiPageId(null);
        }}
      />

      <DatasetPickerModal
        open={datasetRefModalOpen}
        projectId={projectId!}
        existingDatasets={project.datasets || []}
        onClose={() => setDatasetRefModalOpen(false)}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['project', projectId] });
          setDatasetRefModalOpen(false);
        }}
      />

      <ExportImportModal
        open={exportModalOpen}
        mode="export"
        projectId={projectId}
        onClose={() => setExportModalOpen(false)}
      />
    </div>
  );
}
