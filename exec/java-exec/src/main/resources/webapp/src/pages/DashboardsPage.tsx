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
  Select,
  Switch,
} from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  DashboardOutlined,
  PlusOutlined,
  AppstoreOutlined,
  ClockCircleOutlined,
  LinkOutlined,
  StarOutlined,
  StarFilled,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getDashboards,
  createDashboard,
  deleteDashboard,
  updateDashboard,
  getFavorites,
  toggleFavorite,
} from '../api/dashboards';
import type { Dashboard } from '../types';

const { Title, Text } = Typography;
const { TextArea } = Input;

export default function DashboardsPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [favoritesOnly, setFavoritesOnly] = useState(false);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingDashboard, setEditingDashboard] = useState<Dashboard | null>(null);
  const [createForm] = Form.useForm();
  const [editForm] = Form.useForm();

  // Fetch dashboards
  const { data: dashboards, isLoading, error } = useQuery({
    queryKey: ['dashboards'],
    queryFn: getDashboards,
  });

  // Fetch favorites
  const { data: favorites } = useQuery({
    queryKey: ['dashboard-favorites'],
    queryFn: getFavorites,
  });

  // Toggle favorite mutation
  const favoriteMutation = useMutation({
    mutationFn: toggleFavorite,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dashboard-favorites'] });
    },
  });

  // Create mutation
  const createMutation = useMutation({
    mutationFn: createDashboard,
    onSuccess: (newDashboard) => {
      message.success('Dashboard created');
      queryClient.invalidateQueries({ queryKey: ['dashboards'] });
      setCreateModalOpen(false);
      createForm.resetFields();
      navigate(`/dashboards/${newDashboard.id}`);
    },
    onError: (err: Error) => {
      message.error(`Failed to create dashboard: ${err.message}`);
    },
  });

  // Update mutation
  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<Dashboard> }) =>
      updateDashboard(id, data),
    onSuccess: () => {
      message.success('Dashboard updated');
      queryClient.invalidateQueries({ queryKey: ['dashboards'] });
      setEditModalOpen(false);
      setEditingDashboard(null);
    },
    onError: (err: Error) => {
      message.error(`Failed to update dashboard: ${err.message}`);
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: deleteDashboard,
    onSuccess: () => {
      message.success('Dashboard deleted');
      queryClient.invalidateQueries({ queryKey: ['dashboards'] });
    },
    onError: (err: Error) => {
      message.error(`Failed to delete dashboard: ${err.message}`);
    },
  });

  // Filter dashboards
  const filteredDashboards = useMemo(() => {
    if (!dashboards) {
      return [];
    }
    let result = dashboards;
    if (favoritesOnly && favorites) {
      result = result.filter((d) => favorites.includes(d.id));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (d) =>
          d.name.toLowerCase().includes(lower) ||
          (d.description && d.description.toLowerCase().includes(lower))
      );
    }
    return result;
  }, [dashboards, searchText, favoritesOnly, favorites]);

  // Handle create
  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      createMutation.mutate({
        name: values.name,
        description: values.description,
        isPublic: values.isPublic || false,
        refreshInterval: values.refreshInterval || 0,
      });
    } catch {
      // Form validation failed
    }
  };

  // Handle edit
  const handleEdit = (dashboard: Dashboard, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingDashboard(dashboard);
    editForm.setFieldsValue({
      name: dashboard.name,
      description: dashboard.description,
      isPublic: dashboard.isPublic,
    });
    setEditModalOpen(true);
  };

  // Handle save edit
  const handleSaveEdit = async () => {
    if (!editingDashboard) {
      return;
    }
    try {
      const values = await editForm.validateFields();
      updateMutation.mutate({
        id: editingDashboard.id,
        data: {
          name: values.name,
          description: values.description,
          isPublic: values.isPublic,
        },
      });
    } catch {
      // Form validation failed
    }
  };

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
                Failed to load dashboards: {(error as Error).message}
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
              Dashboards
            </Title>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalOpen(true)}
            >
              New Dashboard
            </Button>
          </div>

          {/* Search and Favorites Filter */}
          <Space>
            <Input
              placeholder="Search dashboards by name or description..."
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

          {/* Dashboard Cards */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredDashboards.length === 0 ? (
            <Empty
              image={<DashboardOutlined style={{ fontSize: 64, color: '#d9d9d9' }} />}
              description={
                searchText
                  ? 'No dashboards match your search'
                  : 'No dashboards yet. Create one to get started!'
              }
            >
              {!searchText && (
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={() => setCreateModalOpen(true)}
                >
                  Create Dashboard
                </Button>
              )}
            </Empty>
          ) : (
            <Row gutter={[16, 16]}>
              {filteredDashboards.map((dashboard) => (
                <Col xs={24} sm={12} md={8} lg={6} key={dashboard.id}>
                  <Card
                    hoverable
                    size="small"
                    onClick={() => navigate(`/dashboards/${dashboard.id}`)}
                    cover={
                      <div
                        style={{
                          height: 120,
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          background: 'linear-gradient(135deg, #722ed1 0%, #2f54eb 100%)',
                          position: 'relative',
                        }}
                      >
                        {/* Favorite Star */}
                        <div
                          style={{ position: 'absolute', top: 8, right: 8, zIndex: 1 }}
                          onClick={(e) => {
                            e.stopPropagation();
                            favoriteMutation.mutate(dashboard.id);
                          }}
                        >
                          {(favorites || []).includes(dashboard.id) ? (
                            <StarFilled style={{ fontSize: 20, color: '#faad14', cursor: 'pointer' }} />
                          ) : (
                            <StarOutlined style={{ fontSize: 20, color: 'rgba(255,255,255,0.7)', cursor: 'pointer' }} />
                          )}
                        </div>
                        <Space direction="vertical" align="center">
                          <DashboardOutlined style={{ fontSize: 36, color: '#fff' }} />
                          <Text style={{ color: '#fff', fontSize: 12 }}>
                            <AppstoreOutlined /> {dashboard.panels?.length || 0} panels
                          </Text>
                        </Space>
                      </div>
                    }
                    actions={[
                      <Tooltip title="Edit details" key="edit">
                        <EditOutlined onClick={(e) => handleEdit(dashboard, e)} />
                      </Tooltip>,
                      <Popconfirm
                        key="delete"
                        title="Delete this dashboard?"
                        description="This action cannot be undone."
                        onConfirm={(e) => {
                          e?.stopPropagation();
                          deleteMutation.mutate(dashboard.id);
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
                            {dashboard.name}
                          </Text>
                          {dashboard.isPublic ? (
                            <GlobalOutlined style={{ color: '#52c41a', fontSize: 12 }} />
                          ) : (
                            <LockOutlined style={{ color: '#faad14', fontSize: 12 }} />
                          )}
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size={0}>
                          {dashboard.description && (
                            <Text type="secondary" ellipsis style={{ fontSize: 12 }}>
                              {dashboard.description}
                            </Text>
                          )}
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            <UserOutlined /> {dashboard.owner}
                          </Text>
                          {dashboard.refreshInterval > 0 && (
                            <Text type="secondary" style={{ fontSize: 11 }}>
                              <ClockCircleOutlined /> Auto-refresh: {dashboard.refreshInterval}s
                            </Text>
                          )}
                          <Text type="secondary" style={{ fontSize: 11 }}>
                            Updated: {formatDate(dashboard.updatedAt)}
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

      {/* Create Dashboard Modal */}
      <Modal
        title="Create New Dashboard"
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
            rules={[{ required: true, message: 'Please enter a dashboard name' }]}
          >
            <Input placeholder="My Dashboard" />
          </Form.Item>
          <Form.Item name="description" label="Description">
            <TextArea rows={3} placeholder="Dashboard description..." />
          </Form.Item>
          <Form.Item name="refreshInterval" label="Auto-refresh interval">
            <Select
              placeholder="Select refresh interval"
              options={[
                { value: 0, label: 'Off' },
                { value: 10, label: '10 seconds' },
                { value: 30, label: '30 seconds' },
                { value: 60, label: '1 minute' },
                { value: 300, label: '5 minutes' },
              ]}
            />
          </Form.Item>
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* Edit Dashboard Modal */}
      <Modal
        title="Edit Dashboard"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingDashboard(null);
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
          <Form.Item name="isPublic" label="Visibility">
            <Select
              options={[
                { value: false, label: 'Private' },
                { value: true, label: 'Public' },
              ]}
            />
          </Form.Item>
        </Form>
        {editingDashboard?.isPublic && (
          <div style={{ padding: '8px 0', borderTop: '1px solid #f0f0f0' }}>
            <Text type="secondary" style={{ display: 'block', marginBottom: 4 }}>
              <LinkOutlined /> Shareable Link
            </Text>
            <Input.Group compact>
              <Input
                value={`${window.location.origin}/sqllab/dashboards/${editingDashboard.id}`}
                readOnly
                style={{ width: 'calc(100% - 80px)' }}
              />
              <Button
                icon={<LinkOutlined />}
                onClick={() => {
                  navigator.clipboard.writeText(
                    `${window.location.origin}/sqllab/dashboards/${editingDashboard.id}`
                  ).then(
                    () => message.success('Link copied!'),
                    () => message.error('Failed to copy')
                  );
                }}
              >
                Copy
              </Button>
            </Input.Group>
          </div>
        )}
      </Modal>
    </div>
  );
}
