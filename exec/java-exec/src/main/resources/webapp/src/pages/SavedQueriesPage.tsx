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
  Select,
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
  FolderOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSavedQueries, deleteSavedQuery, updateSavedQuery } from '../api/savedQueries';
import { getProjects } from '../api/projects';
import { getSchedules } from '../api/schedules';
import { useCurrentUser } from '../hooks/useCurrentUser';
import type { SavedQuery } from '../types';
import type { ColumnsType } from 'antd/es/table';
import AddToProjectModal from '../components/common/AddToProjectModal';
import BulkActionBar from '../components/common/BulkActionBar';
import BulkAddToProjectModal from '../components/common/BulkAddToProjectModal';
import ScheduleModal from '../components/query-editor/ScheduleModal';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

interface SavedQueriesPageProps {
  filterIds?: string[];
  projectId?: string;
  projectOwner?: string;
  onAdd?: () => void;
}

export default function SavedQueriesPage({ filterIds, projectId, projectOwner, onAdd }: SavedQueriesPageProps = {}) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { canEdit, canView, isProjectOwner } = useCurrentUser();
  const [searchText, setSearchText] = useState('');
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editingQuery, setEditingQuery] = useState<SavedQuery | null>(null);
  const [viewModalOpen, setViewModalOpen] = useState(false);
  const [viewingQuery, setViewingQuery] = useState<SavedQuery | null>(null);
  const [addToProjectId, setAddToProjectId] = useState<string | null>(null);
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [bulkProjectModalOpen, setBulkProjectModalOpen] = useState(false);
  const [scheduleQueryId, setScheduleQueryId] = useState<string | null>(null);
  const [scheduleQueryName, setScheduleQueryName] = useState('');
  const [scheduleQuerySql, setScheduleQuerySql] = useState<string | undefined>(undefined);
  const [projectFilter, setProjectFilter] = useState<string | null>(null);
  const [form] = Form.useForm();

  // Fetch saved queries
  const { data: queries, isLoading, error } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  // Fetch projects for filter dropdown (only on global page)
  const { data: projects } = useQuery({
    queryKey: ['projects'],
    queryFn: getProjects,
    enabled: !projectId,
  });

  // Fetch schedules
  const { data: schedules } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
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

  // Filter queries based on visibility, filterIds (project scope), project filter, and search text
  const filteredQueries = useMemo(() => {
    if (!queries) {
      return [];
    }
    let result = queries;
    // Visibility: show own queries + public queries (admins/anonymous see all)
    result = result.filter((q) => canView(q.owner, q.isPublic));
    if (filterIds) {
      const idSet = new Set(filterIds);
      result = result.filter((q) => idSet.has(q.id));
    }
    if (projectFilter && projects) {
      const proj = projects.find((p) => p.id === projectFilter);
      if (proj) {
        const idSet = new Set(proj.savedQueryIds || []);
        result = result.filter((q) => idSet.has(q.id));
      }
    }
    if (searchText) {
      const lowerSearch = searchText.toLowerCase();
      result = result.filter(
        (q) =>
          q.name.toLowerCase().includes(lowerSearch) ||
          q.sql.toLowerCase().includes(lowerSearch) ||
          (q.description && q.description.toLowerCase().includes(lowerSearch))
      );
    }
    return result;
  }, [queries, searchText, filterIds, projectFilter, projects, canView]);

  // Handle loading query into SQL Lab
  const handleLoadQuery = (query: SavedQuery) => {
    const target = projectId ? `/projects/${projectId}/query` : '/';
    navigate(target, { state: { loadQuery: query } });
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
      render: (name: string, record: SavedQuery) => {
        const hasActiveSchedule = schedules?.some((s) => s.savedQueryId === record.id && s.enabled);
        return (
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
            {hasActiveSchedule && (
              <Tag color="green" icon={<ClockCircleOutlined />}>Scheduled</Tag>
            )}
          </Space>
          {record.description && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {record.description}
            </Text>
          )}
        </Space>
        );
      },
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
      width: 240,
      render: (_: unknown, record: SavedQuery) => {
        const hasSchedule = schedules?.some((s) => s.savedQueryId === record.id && s.enabled);
        const editable = canEdit(record.owner) || (projectOwner ? isProjectOwner(projectOwner) : false);
        return (
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
          {editable && (
            <Tooltip title="Edit">
              <Button
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEdit(record)}
              />
            </Tooltip>
          )}
          {editable && (
            <Tooltip title="Schedule">
              <Button
                size="small"
                icon={<ClockCircleOutlined />}
                style={hasSchedule ? { color: '#52c41a' } : undefined}
                onClick={() => {
                  setScheduleQueryId(record.id);
                  setScheduleQueryName(record.name);
                  setScheduleQuerySql(record.sql);
                }}
              />
            </Tooltip>
          )}
          {!projectId && (
            <Tooltip title="Add to Project">
              <Button size="small" icon={<FolderOutlined />} onClick={() => setAddToProjectId(record.id)} />
            </Tooltip>
          )}
          {editable && (
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
          )}
        </Space>
        );
      },
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
            <Space>
              {onAdd && (
                <Button onClick={onAdd}>
                  Add Existing
                </Button>
              )}
              <Button type="primary" onClick={() => navigate(projectId ? `/projects/${projectId}/query` : '/sqllab')}>
                New Query
              </Button>
            </Space>
          </div>

          {/* Search and Project Filter */}
          <Space wrap>
            <Input
              placeholder="Search queries by name, SQL, or description..."
              prefix={<SearchOutlined />}
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              allowClear
              style={{ width: 400 }}
            />
            {!projectId && projects && projects.length > 0 && (
              <Select
                placeholder="Filter by project"
                value={projectFilter}
                onChange={setProjectFilter}
                allowClear
                style={{ width: 200 }}
                options={projects.map((p) => ({ value: p.id, label: p.name }))}
                suffixIcon={<FolderOutlined />}
              />
            )}
          </Space>

          {/* Bulk Action Bar */}
          <BulkActionBar
            selectedCount={selectedRowKeys.length}
            onAddToProject={!projectId ? () => setBulkProjectModalOpen(true) : undefined}
            onDelete={() => {
              selectedRowKeys.forEach(id => deleteMutation.mutate(id));
              setSelectedRowKeys([]);
            }}
            onClear={() => setSelectedRowKeys([])}
          />

          {/* Table */}
          {isLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Spin size="large" />
            </div>
          ) : filteredQueries.length === 0 ? (
            searchText ? (
              <Empty description="No queries match your search" />
            ) : onAdd ? (
              <Empty description="No saved queries in this project yet">
                <Space>
                  <Button onClick={onAdd}>Add Existing</Button>
                  <Button type="primary" onClick={() => navigate(`/projects/${projectId}/query`)}>
                    New Query
                  </Button>
                </Space>
              </Empty>
            ) : (
              <Empty description="No saved queries yet. Create one in SQL Lab!" />
            )
          ) : (
            <Table
              dataSource={filteredQueries}
              columns={columns}
              rowKey="id"
              rowSelection={{
                selectedRowKeys,
                onChange: (keys) => setSelectedRowKeys(keys as string[]),
              }}
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
                background: 'var(--color-bg-elevated)',
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

      {/* Add to Project Modal */}
      <AddToProjectModal
        open={!!addToProjectId}
        onClose={() => setAddToProjectId(null)}
        itemId={addToProjectId || ''}
        itemType="savedQuery"
      />

      {/* Bulk Add to Project Modal */}
      <BulkAddToProjectModal
        open={bulkProjectModalOpen}
        onClose={() => setBulkProjectModalOpen(false)}
        itemIds={selectedRowKeys}
        itemType="savedQuery"
      />

      {/* Schedule Modal */}
      <ScheduleModal
        open={!!scheduleQueryId}
        onClose={() => {
          setScheduleQueryId(null);
          setScheduleQueryName('');
          setScheduleQuerySql(undefined);
        }}
        savedQueryId={scheduleQueryId || ''}
        savedQueryName={scheduleQueryName}
        savedQuerySql={scheduleQuerySql}
        onSuccess={() => {
          queryClient.invalidateQueries({ queryKey: ['schedules'] });
        }}
      />
    </div>
  );
}
