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
  Tag,
  Badge,
} from 'antd';
import {
  SearchOutlined,
  EditOutlined,
  DeleteOutlined,
  PlusOutlined,
  CheckCircleOutlined,
  StopOutlined,
  DatabaseOutlined,
  PoweroffOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getPlugins, deletePlugin, enablePlugin } from '../api/storage';
import { cleanupPluginDatasets } from '../api/projects';
import { getPluginLogoUrl, getPluginGradient, knownPluginTypes, getTemplate } from '../components/datasource';
import type { StoragePluginDetail } from '../types';

const { Title, Text } = Typography;

type FilterMode = 'all' | 'enabled' | 'disabled';

function getPluginType(plugin: StoragePluginDetail): string {
  return (plugin.config?.type as string) || 'unknown';
}

function isEnabled(plugin: StoragePluginDetail): boolean {
  return (plugin.config?.enabled as boolean) || false;
}

function getConnectionSnippet(config: Record<string, unknown>): string {
  const conn =
    (config.connection as string) ||
    (config.url as string) ||
    '';
  if (conn.length > 60) {
    return conn.substring(0, 57) + '...';
  }
  return conn;
}

export default function DataSourcesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [filterMode, setFilterMode] = useState<FilterMode>('all');
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [createForm] = Form.useForm();

  const { data: plugins, isLoading, error } = useQuery({
    queryKey: ['storage-plugins'],
    queryFn: getPlugins,
  });

  const deleteMutation = useMutation({
    mutationFn: deletePlugin,
    onSuccess: (_data, pluginName) => {
      message.success('Plugin deleted');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
      // Remove dataset references for this plugin from all projects
      cleanupPluginDatasets(pluginName).then(() => {
        queryClient.invalidateQueries({ queryKey: ['projects'] });
      }).catch(() => {
        // Best-effort cleanup — ignore failures
      });
    },
    onError: (err: Error) => {
      message.error(`Failed to delete plugin: ${err.message}`);
    },
  });

  const enableMutation = useMutation({
    mutationFn: ({ name, enable }: { name: string; enable: boolean }) =>
      enablePlugin(name, enable),
    onSuccess: (_, { enable }) => {
      message.success(enable ? 'Plugin enabled' : 'Plugin disabled');
      queryClient.invalidateQueries({ queryKey: ['storage-plugins'] });
    },
    onError: (err: Error) => {
      message.error(`Failed to toggle plugin: ${err.message}`);
    },
  });

  const filteredPlugins = useMemo(() => {
    if (!plugins) {
      return [];
    }
    let result = [...plugins];
    if (filterMode === 'enabled') {
      result = result.filter((p) => isEnabled(p));
    } else if (filterMode === 'disabled') {
      result = result.filter((p) => !isEnabled(p));
    }
    if (searchText) {
      const lower = searchText.toLowerCase();
      result = result.filter(
        (p) =>
          p.name.toLowerCase().includes(lower) ||
          getPluginType(p).toLowerCase().includes(lower) ||
          ((p.config?.description as string) || '').toLowerCase().includes(lower)
      );
    }
    return result.sort((a, b) => {
      const aEnabled = isEnabled(a);
      const bEnabled = isEnabled(b);
      if (aEnabled !== bEnabled) {
        return aEnabled ? -1 : 1;
      }
      return a.name.localeCompare(b.name);
    });
  }, [plugins, searchText, filterMode]);

  const handleCreate = async () => {
    try {
      const values = await createForm.validateFields();
      const template = getTemplate(values.type);
      if (values.description) {
        template.description = values.description;
      }
      // Navigate to edit page with the template config as URL state
      navigate(`/datasources/${encodeURIComponent(values.name)}`, {
        state: { config: template, isNew: true },
      });
      setCreateModalOpen(false);
      createForm.resetFields();
    } catch {
      // Form validation failed
    }
  };

  if (error) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty
            description={
              <Text type="danger">
                Failed to load storage plugins: {(error as Error).message}
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
              Data Sources
            </Title>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setCreateModalOpen(true)}
            >
              New Data Source
            </Button>
          </div>

          {/* Search and Filter */}
          <Space>
            <Input
              placeholder="Search plugins by name or type..."
              prefix={<SearchOutlined />}
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              allowClear
              style={{ width: 400 }}
            />
            <Button
              type={filterMode === 'all' ? 'primary' : 'default'}
              onClick={() => setFilterMode('all')}
            >
              All
            </Button>
            <Button
              type={filterMode === 'enabled' ? 'primary' : 'default'}
              icon={<CheckCircleOutlined />}
              onClick={() => setFilterMode('enabled')}
            >
              Enabled
            </Button>
            <Button
              type={filterMode === 'disabled' ? 'primary' : 'default'}
              icon={<StopOutlined />}
              onClick={() => setFilterMode('disabled')}
            >
              Disabled
            </Button>
          </Space>

          {/* Plugin Cards */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredPlugins.length === 0 ? (
            <Empty
              image={<DatabaseOutlined style={{ fontSize: 64, color: '#d9d9d9' }} />}
              description={
                searchText
                  ? 'No plugins match your search'
                  : 'No storage plugins found.'
              }
            >
              {!searchText && (
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={() => setCreateModalOpen(true)}
                >
                  Create Data Source
                </Button>
              )}
            </Empty>
          ) : (
            <Row gutter={[16, 16]}>
              {filteredPlugins.map((plugin) => {
                const type = getPluginType(plugin);
                const enabled = isEnabled(plugin);
                const logoUrl = getPluginLogoUrl(type, plugin.config?.connection as string);

                return (
                  <Col xs={24} sm={12} md={8} lg={6} key={plugin.name}>
                    <Badge.Ribbon
                      text={enabled ? 'Enabled' : 'Disabled'}
                      color={enabled ? 'green' : 'default'}
                    >
                      <Card
                        hoverable
                        size="small"
                        style={enabled ? undefined : { opacity: 0.55 }}
                        onClick={() =>
                          navigate(`/datasources/${encodeURIComponent(plugin.name)}`)
                        }
                        cover={
                          <div
                            style={{
                              height: 120,
                              display: 'flex',
                              alignItems: 'center',
                              justifyContent: 'center',
                              background: getPluginGradient(type),
                              filter: enabled ? undefined : 'grayscale(80%)',
                              position: 'relative',
                            }}
                          >
                            {logoUrl ? (
                              <img
                                src={logoUrl}
                                alt={type}
                                style={{
                                  maxHeight: 56,
                                  maxWidth: 120,
                                  objectFit: 'contain',
                                  mixBlendMode: 'multiply',
                                }}
                                onError={(e) => {
                                  // Fallback to icon if image fails to load
                                  (e.target as HTMLImageElement).style.display = 'none';
                                }}
                              />
                            ) : (
                              <SettingOutlined style={{ fontSize: 40, color: '#fff' }} />
                            )}
                          </div>
                        }
                        actions={[
                          <Tooltip title="Edit" key="edit">
                            <EditOutlined
                              onClick={(e) => {
                                e.stopPropagation();
                                navigate(
                                  `/datasources/${encodeURIComponent(plugin.name)}`
                                );
                              }}
                            />
                          </Tooltip>,
                          <Tooltip title={enabled ? 'Disable' : 'Enable'} key="toggle">
                            <PoweroffOutlined
                              style={{ color: enabled ? '#52c41a' : '#d9d9d9' }}
                              onClick={(e) => {
                                e.stopPropagation();
                                enableMutation.mutate({
                                  name: plugin.name,
                                  enable: !enabled,
                                });
                              }}
                            />
                          </Tooltip>,
                          <Popconfirm
                            key="delete"
                            title="Delete this plugin?"
                            description="This action cannot be undone."
                            onConfirm={(e) => {
                              e?.stopPropagation();
                              deleteMutation.mutate(plugin.name);
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
                                {plugin.name}
                              </Text>
                            </Space>
                          }
                          description={
                            <Space direction="vertical" size={0}>
                              <Tag color="blue">{type}</Tag>
                              {(plugin.config?.description as string) && (
                                <Text
                                  type="secondary"
                                  ellipsis
                                  style={{ fontSize: 12, marginTop: 4 }}
                                >
                                  {plugin.config.description as string}
                                </Text>
                              )}
                              {getConnectionSnippet(plugin.config) && (
                                <Text
                                  type="secondary"
                                  ellipsis
                                  style={{ fontSize: 11, marginTop: 4 }}
                                >
                                  {getConnectionSnippet(plugin.config)}
                                </Text>
                              )}
                            </Space>
                          }
                        />
                      </Card>
                    </Badge.Ribbon>
                  </Col>
                );
              })}
            </Row>
          )}
        </Space>
      </Card>

      {/* Create Plugin Modal */}
      <Modal
        title="New Data Source"
        open={createModalOpen}
        onOk={handleCreate}
        onCancel={() => {
          setCreateModalOpen(false);
          createForm.resetFields();
        }}
        okText="Configure"
      >
        <Form form={createForm} layout="vertical">
          <Form.Item
            name="name"
            label="Plugin Name"
            rules={[
              { required: true, message: 'Please enter a plugin name' },
              {
                pattern: /^[a-zA-Z0-9_-]+$/,
                message: 'Only letters, numbers, hyphens, and underscores',
              },
            ]}
          >
            <Input placeholder="my_plugin" />
          </Form.Item>
          <Form.Item
            name="description"
            label="Description (optional)"
          >
            <Input.TextArea
              placeholder="Brief description of this data source"
              autoSize={{ minRows: 1, maxRows: 3 }}
            />
          </Form.Item>
          <Form.Item
            name="type"
            label="Plugin Type"
            rules={[{ required: true, message: 'Please select a type' }]}
          >
            <Select
              placeholder="Select plugin type"
              options={[
                ...knownPluginTypes,
                { value: '__other', label: 'Other (JSON editor)' },
              ]}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
