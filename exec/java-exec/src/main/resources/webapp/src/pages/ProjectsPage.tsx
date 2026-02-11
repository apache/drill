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
import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Row,
  Col,
  Input,
  Button,
  Space,
  Popconfirm,
  message,
  Typography,
  Tooltip,
  Empty,
  Spin,
  Modal,
  Form,
  Switch,
  Tag,
  Select,
} from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  FolderOutlined,
  PlusOutlined,
  StarOutlined,
  StarFilled,
  DatabaseOutlined,
  CodeOutlined,
  BarChartOutlined,
  DashboardOutlined,
  FileTextOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getProjects,
  createProject,
  deleteProject,
  updateProject,
  getFavorites,
  toggleFavorite,
} from '../api/projects';
import type { Project } from '../types';

const { Title, Text } = Typography;
const { TextArea } = Input;

export default function ProjectsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingProject, setEditingProject] = useState<Project | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();

  const { data: projects, isLoading, error } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
  });

  const { data: favorites } = useQuery({
    queryKey: ['project-favorites'],
    queryFn: getFavorites,
  });

  const favoriteMutation = useMutation({
    mutationFn: toggleFavorite,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['project-favorites'] });
    },
  });

  const createMutation = useMutation({
    mutationFn: createProject,
    onSuccess: (newProject) => {
      message.success('Project created');
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      setCreateModalOpen(false);
      createForm.resetFields();
      navigate(`/projects/${newProject.id}`);
    },
    onError: (err: Error) => {
      message.error(`Failed to create project: ${err.message}`);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Project> }) =>
      updateProject(id, data),
    onSuccess: () => {
      message.success('Project updated');
      queryClient.invalidateQueries({ queryKey: ['projects'] });
      setEditModalOpen(false);
      setEditingProject(null);
    },
    onError: (err: Error) => {
      message.error(`Failed to update project: ${err.message}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: deleteProject,
    onSuccess: () => {
      message.success('Project deleted');
      queryClient.invalidateQueries({ queryKey: ['projects'] });
    },
    onError: (err: Error) => {
      message.error(`Failed to delete project: ${err.message}`);
    },
  });

  const filteredProjects = useMemo(() => {
    if (!projects) {
      return [];
    }
    let result = projects;
    if (favoritesOnly && favorites) {
      result = result.filter((p) => favorites.includes(p.id));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (p) =>
          p.name.toLowerCase().includes(lower) ||
          (p.description && p.description.toLowerCase().includes(lower)) ||
          (p.tags && p.tags.some((t) => t.toLowerCase().includes(lower)))
      );
    }
    return result;
  }, [projects, searchText, favoritesOnly, favorites]);

  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      createMutation.mutate({
        name: values.name,
        description: values.description,
        tags: values.tags || [],
        isPublic: values.isPublic || false,
      });
    } catch {
      // Form validation failed
    }
  };

  const handleEdit = (project: Project, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingProject(project);
    editForm.setFieldsValue({
      name: project.name,
      description: project.description,
      tags: project.tags,
      isPublic: project.isPublic,
    });
    setEditModalOpen(true);
  };

  const handleSaveEdit = async () => {
    if (!editingProject) {
      return;
    }
    try {
      const values = await editForm.validateFields();
      updateMutation.mutate({
        id: editingProject.id,
        data: {
          name: values.name,
          description: values.description,
          tags: values.tags || [],
          isPublic: values.isPublic,
        },
      });
    } catch {
      // Form validation failed
    }
  };

  const formatDate = (timestamp: number | string) => {
    const date = typeof timestamp === 'number' ? new Date(timestamp) : new Date(timestamp);
    return date.toLocaleDateString();
  };

  const getItemCounts = (project: Project) => {
    const counts = [];
    if (project.datasets?.length) {
      counts.push({ icon: <DatabaseOutlined />, count: project.datasets.length, label: 'datasets' });
    }
    if (project.savedQueryIds?.length) {
      counts.push({ icon: <CodeOutlined />, count: project.savedQueryIds.length, label: 'queries' });
    }
    if (project.visualizationIds?.length) {
      counts.push({ icon: <BarChartOutlined />, count: project.visualizationIds.length, label: 'vizs' });
    }
    if (project.dashboardIds?.length) {
      counts.push({ icon: <DashboardOutlined />, count: project.dashboardIds.length, label: 'dashboards' });
    }
    if (project.wikiPages?.length) {
      counts.push({ icon: <FileTextOutlined />, count: project.wikiPages.length, label: 'pages' });
    }
    return counts;
  };

  if (error) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty
            description={
              <Text type="danger">
                Failed to load projects: {(error as Error).message}
              </Text>
            }
          />
        </Card>
      </div>
    );
  }

  return (
    <div style={{ padding: 24, overflow: 'auto', flex: 1 }}>
      <Card>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* Header */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Title level={3} style={{ margin: 0 }}>
              Projects
            </Title>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalOpen(true)}
            >
              New Project
            </Button>
          </div>

          {/* Search and Favorites Filter */}
          <Space>
            <Input
              placeholder="Search projects by name, description, or tag..."
              prefix={<SearchOutlined />}
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              allowClear
              style={{ width: 400 }}
            />
            <Tooltip title="Show favorites only">
              <Button
                type={favoritesOnly ? 'primary' : 'default'}
                icon={favoritesOnly ? <StarFilled /> : <StarOutlined />}
                onClick={() => setFavoritesOnly(!favoritesOnly)}
              >
                Favorites
              </Button>
            </Tooltip>
          </Space>

          {/* Project Cards */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredProjects.length === 0 ? (
            <Empty
              image={<FolderOutlined style={{ fontSize: 64, color: '#d9d9d9' }} />}
              description={
                searchText
                  ? 'No projects match your search'
                  : 'No projects yet. Create one to get started!'
              }
            >
              {!searchText && (
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={() => setCreateModalOpen(true)}
                >
                  Create Project
                </Button>
              )}
            </Empty>
          ) : (
            <Row gutter={[16, 16]}>
              {filteredProjects.map((project) => (
                <Col xs={24} sm={12} md={8} lg={6} key={project.id}>
                  <Card
                    hoverable
                    size="small"
                    onClick={() => navigate(`/projects/${project.id}/query`)}
                    cover={
                      <div
                        style={{
                          height: 120,
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          background: 'linear-gradient(135deg, #1890ff 0%, #096dd9 100%)',
                          position: 'relative',
                        }}
                      >
                        {/* Favorite Star */}
                        <div
                          style={{ position: 'absolute', top: 8, right: 8, zIndex: 1 }}
                          onClick={(e) => {
                            e.stopPropagation();
                            favoriteMutation.mutate(project.id);
                          }}
                        >
                          {(favorites || []).includes(project.id) ? (
                            <StarFilled style={{ fontSize: 20, color: '#faad14', cursor: 'pointer' }} />
                          ) : (
                            <StarOutlined style={{ fontSize: 20, color: 'rgba(255,255,255,0.7)', cursor: 'pointer' }} />
                          )}
                        </div>
                        <Space direction="vertical" align="center">
                          <FolderOutlined style={{ fontSize: 36, color: '#fff' }} />
                          <Space size={8}>
                            {getItemCounts(project).map((item, idx) => (
                              <Text key={idx} style={{ color: 'rgba(255,255,255,0.85)', fontSize: 11 }}>
                                {item.icon} {item.count}
                              </Text>
                            ))}
                            {getItemCounts(project).length === 0 && (
                              <Text style={{ color: 'rgba(255,255,255,0.6)', fontSize: 11 }}>
                                Empty project
                              </Text>
                            )}
                          </Space>
                        </Space>
                      </div>
                    }
                    actions={[
                      <Tooltip title="Edit details" key="edit">
                        <EditOutlined onClick={(e) => handleEdit(project, e)} />
                      </Tooltip>,
                      <Tooltip title="Project settings" key="settings">
                        <SettingOutlined onClick={(e) => {
                          e.stopPropagation();
                          navigate(`/projects/${project.id}`);
                        }} />
                      </Tooltip>,
                      <Popconfirm
                        key="delete"
                        title="Delete this project?"
                        description="This action cannot be undone."
                        onConfirm={(e) => {
                          e?.stopPropagation();
                          deleteMutation.mutate(project.id);
                        }}
                        onCancel={(e) => e?.stopPropagation()}
                        okText="Delete"
                        cancelText="Cancel"
                        okButtonProps={{ danger: true }}
                      >
                        <Tooltip title="Delete">
                          <DeleteOutlined
                            style={{ color: '#ff4d4f' }}
                            onClick={(e) => e.stopPropagation()}
                          />
                        </Tooltip>
                      </Popconfirm>,
                    ]}
                  >
                    <Card.Meta
                      title={
                        <Space>
                          <Text strong ellipsis style={{ maxWidth: 150 }}>
                            {project.name}
                          </Text>
                          {project.isPublic ? (
                            <GlobalOutlined style={{ color: '#52c41a', fontSize: 12 }} />
                          ) : (
                            <LockOutlined style={{ color: '#faad14', fontSize: 12 }} />
                          )}
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={0}>
                          {project.description && (
                            <Text type="secondary" ellipsis style={{ fontSize: 12 }}>
                              {project.description}
                            </Text>
                          )}
                          {project.tags && project.tags.length > 0 && (
                            <div>
                              {project.tags.slice(0, 3).map((tag) => (
                                <Tag key={tag} style={{ fontSize: 10, marginRight: 2 }}>
                                  {tag}
                                </Tag>
                              ))}
                              {project.tags.length > 3 && (
                                <Tag style={{ fontSize: 10 }}>+{project.tags.length - 3}</Tag>
                              )}
                            </div>
                          )}
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            <UserOutlined /> {project.owner}
                          </Text>
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            Updated: {formatDate(project.updatedAt)}
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

      {/* Create Project Modal */}
      <Modal
        title="Create New Project"
        open={createModalOpen}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalOpen(false);
          createForm.resetFields();
        }}
        confirmLoading={createMutation.isPending}
        okText="Create"
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a project name' }]}
          >
            <Input placeholder="My Analytics Project" />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} placeholder="Project description..." />
          </Form.Item>
          <Form.Item name="tags" label="Tags">
            <Select
              mode="tags"
              placeholder="Add tags..."
              style={{ width: '100%' }}
            />
          </Form.Item>
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* Edit Project Modal */}
      <Modal
        title="Edit Project"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingProject(null);
        }}
        confirmLoading={updateMutation.isPending}
      >
        <Form form={editForm} layout="vertical">
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a name' }]}
          >
            <Input />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} />
          </Form.Item>
          <Form.Item name="tags" label="Tags">
            <Select
              mode="tags"
              placeholder="Add tags..."
              style={{ width: '100%' }}
            />
          </Form.Item>
          <Form.Item name="isPublic" label="Visibility">
            <Select
              options={[
                { value: false, label: 'Private' },
                { value: true, label: 'Public' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
