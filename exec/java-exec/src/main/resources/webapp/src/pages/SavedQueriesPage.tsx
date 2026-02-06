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
  Table,
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
  Form,
  Switch,
} from 'antd';
import {
  SearchOutlined,
  PlayCircleOutlined,
  EditOutlined,
  DeleteOutlined,
  GlobalOutlined,
  LockOutlined,
  UserOutlined,
  CodeOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSavedQueries, deleteSavedQuery, updateSavedQuery } from '../api/savedQueries';
import type { SavedQuery } from '../types';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

export default function SavedQueriesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingQuery, setEditingQuery] = useState<SavedQuery | null>(null);
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewingQuery, setViewingQuery] = useState<SavedQuery | null>(null);
  const [form] = Form.useForm();

  // Fetch saved queries
  const { data: queries, isLoading, error } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: deleteSavedQuery,
    onSuccess: () => {
      message.success('Query deleted successfully');
      queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to delete query: ${error.message}`);
    },
  });

  // Update mutation
  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<SavedQuery> }) =>
      updateSavedQuery(id, data),
    onSuccess: () => {
      message.success('Query updated successfully');
      queryClient.invalidateQueries({ queryKey: ['savedQueries'] });
      setEditModalOpen(false);
      setEditingQuery(null);
    },
    onError: (error: Error) => {
      message.error(`Failed to update query: ${error.message}`);
    },
  });

  // Filter queries based on search text
  const filteredQueries = useMemo(() => {
    if (!queries) {
      return [];
    }
    if (!searchText) {
      return queries;
    }
    const lowerSearch = searchText.toLowerCase();
    return queries.filter(
      (q) =>
        q.name.toLowerCase().includes(lowerSearch) ||
        q.sql.toLowerCase().includes(lowerSearch) ||
        (q.description && q.description.toLowerCase().includes(lowerSearch))
    );
  }, [queries, searchText]);

  // Handle loading query into SQL Lab
  const handleLoadQuery = (query: SavedQuery) => {
    // Navigate to SQL Lab - the query will be loaded into the active tab via state
    navigate('/', { state: { loadQuery: query } });
  };

  // Handle edit
  const handleEdit = (query: SavedQuery) => {
    setEditingQuery(query);
    form.setFieldsValue({
      name: query.name,
      description: query.description,
      isPublic: query.isPublic,
    });
    setEditModalOpen(true);
  };

  // Handle view SQL
  const handleViewSql = (query: SavedQuery) => {
    setViewingQuery(query);
    setViewModalOpen(true);
  };

  // Handle save edit
  const handleSaveEdit = async () => {
    if (!editingQuery) {
      return;
    }
    try {
      const values = await form.validateFields();
      updateMutation.mutate({
        id: editingQuery.id,
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
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
  };

  // Table columns
  const columns: ColumnsType<SavedQuery> = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name: string, record: SavedQuery) => (
        <Space direction="vertical" size={0}>
          <Space>
            <Text strong>{name}</Text>
            {record.isPublic ? (
              <Tooltip title="Public - visible to all users">
                <GlobalOutlined style={{ color: '#52c41a' }} />
              </Tooltip>
            ) : (
              <Tooltip title="Private - only visible to you">
                <LockOutlined style={{ color: '#faad14' }} />
              </Tooltip>
            )}
          </Space>
          {record.description && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {record.description}
            </Text>
          )}
        </Space>
      ),
    },
    {
      title: 'Owner',
      dataIndex: 'owner',
      key: 'owner',
      width: 150,
      render: (owner: string) => (
        <Space>
          <UserOutlined />
          <Text>{owner}</Text>
        </Space>
      ),
    },
    {
      title: 'Schema',
      dataIndex: 'defaultSchema',
      key: 'defaultSchema',
      width: 150,
      render: (schema: string | undefined) =>
        schema ? <Tag>{schema}</Tag> : <Text type="secondary">None</Text>,
    },
    {
      title: 'Updated',
      dataIndex: 'updatedAt',
      key: 'updatedAt',
      width: 180,
      sorter: (a, b) => {
        const aTime = typeof a.updatedAt === 'number' ? a.updatedAt : new Date(a.updatedAt).getTime();
        const bTime = typeof b.updatedAt === 'number' ? b.updatedAt : new Date(b.updatedAt).getTime();
        return aTime - bTime;
      },
      render: (timestamp: number | string) => (
        <Text type="secondary">{formatDate(timestamp)}</Text>
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 200,
      render: (_: unknown, record: SavedQuery) => (
        <Space>
          <Tooltip title="Run in SQL Lab">
            <Button
              type="primary"
              size="small"
              icon={<PlayCircleOutlined />}
              onClick={() => handleLoadQuery(record)}
            />
          </Tooltip>
          <Tooltip title="View SQL">
            <Button
              size="small"
              icon={<CodeOutlined />}
              onClick={() => handleViewSql(record)}
            />
          </Tooltip>
          <Tooltip title="Edit">
            <Button
              size="small"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          <Popconfirm
            title="Delete this query?"
            description="This action cannot be undone."
            onConfirm={() => deleteMutation.mutate(record.id)}
            okText="Delete"
            cancelText="Cancel"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="Delete">
              <Button size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (error) {
    return (
      <div style={{ padding: 24 }}>
        <Card>
          <Empty
            description={
              <Text type="danger">
                Failed to load saved queries: {(error as Error).message}
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
              Saved Queries
            </Title>
            <Button type="primary" onClick={() => navigate('/sqllab')}>
              New Query
            </Button>
          </div>

          {/* Search */}
          <Input
            placeholder="Search queries by name, SQL, or description..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            allowClear
            style={{ maxWidth: 400 }}
          />

          {/* Table */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredQueries.length === 0 ? (
            <Empty
              description={
                searchText
                  ? 'No queries match your search'
                  : 'No saved queries yet. Create one in SQL Lab!'
              }
            />
          ) : (
            <Table
              dataSource={filteredQueries}
              columns={columns}
              rowKey="id"
              pagination={{
                pageSize: 10,
                showSizeChanger: true,
                showTotal: (total) => `${total} queries`,
              }}
            />
          )}
        </Space>
      </Card>

      {/* Edit Modal */}
      <Modal
        title="Edit Query"
        open={editModalOpen}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalOpen(false);
          setEditingQuery(null);
        }}
        confirmLoading={updateMutation.isPending}
      >
        <Form form={form} layout="vertical">
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
          <Form.Item name="isPublic" label="Public" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* View SQL Modal */}
      <Modal
        title={viewingQuery?.name || 'SQL'}
        open={viewModalOpen}
        onCancel={() => {
          setViewModalOpen(false);
          setViewingQuery(null);
        }}
        footer={[
          <Button key="close" onClick={() => setViewModalOpen(false)}>
            Close
          </Button>,
          <Button
            key="run"
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={() => {
              if (viewingQuery) {
                handleLoadQuery(viewingQuery);
              }
            }}
          >
            Run in SQL Lab
          </Button>,
        ]}
        width={700}
      >
        {viewingQuery && (
          <div>
            {viewingQuery.description && (
              <Paragraph type="secondary">{viewingQuery.description}</Paragraph>
            )}
            <pre
              style={{
                background: '#f5f5f5',
                padding: 16,
                borderRadius: 4,
                maxHeight: 400,
                overflow: 'auto',
                fontFamily: 'monospace',
                fontSize: 13,
              }}
            >
              {viewingQuery.sql}
            </pre>
          </div>
        )}
      </Modal>
    </div>
  );
}
