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
import { useState, useMemo, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Table,
  Input,
  Button,
  Space,
  message,
  Typography,
  Tooltip,
  Empty,
  Spin,
  Modal,
  Select,
} from 'antd';
import {
  SearchOutlined,
  ExclamationCircleOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  LoadingOutlined,
  StopOutlined,
  SelectOutlined,
  EditOutlined,
  DeleteOutlined,
  CopyOutlined,
  FormOutlined,
  UnorderedListOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueryProfiles, cancelQuery } from '../api/queries';
import type { QueryProfile } from '../api/queries';
import type { ColumnsType } from 'antd/es/table';

const { Text } = Typography;

const stateConfig: Record<string, { bgColor: string; textColor: string; icon: JSX.Element | null }> = {
  Succeeded: { bgColor: '#f6ffed', textColor: '#52c41a', icon: <CheckCircleOutlined /> },
  Failed: { bgColor: '#fff1f0', textColor: '#ff4d4f', icon: <CloseCircleOutlined /> },
  Running: { bgColor: '#e6f7ff', textColor: '#1890ff', icon: <LoadingOutlined /> },
  Cancellation_Requested: { bgColor: '#fffbe6', textColor: '#faad14', icon: <StopOutlined /> },
  Cancelled: { bgColor: '#fffbe6', textColor: '#faad14', icon: <StopOutlined /> },
};

const queryTypeConfig: Record<string, { bgColor: string; textColor: string; icon: JSX.Element | null }> = {
  SELECT: { bgColor: '#e6f7ff', textColor: '#1890ff', icon: <SelectOutlined /> },
  INSERT: { bgColor: '#f9f0ff', textColor: '#722ed1', icon: <CopyOutlined /> },
  UPDATE: { bgColor: '#fffbe6', textColor: '#faad14', icon: <EditOutlined /> },
  DELETE: { bgColor: '#fff1f0', textColor: '#ff4d4f', icon: <DeleteOutlined /> },
  CREATE: { bgColor: '#f6ffed', textColor: '#52c41a', icon: <CopyOutlined /> },
  DROP: { bgColor: '#fff1f0', textColor: '#ff4d4f', icon: <DeleteOutlined /> },
  ALTER: { bgColor: '#fffbe6', textColor: '#faad14', icon: <FormOutlined /> },
  'SHOW/DESC': { bgColor: '#f5f5f5', textColor: '#000000', icon: <UnorderedListOutlined /> },
  EXPLAIN: { bgColor: '#e6f7ff', textColor: '#1890ff', icon: <UnorderedListOutlined /> },
  USE: { bgColor: '#f5f5f5', textColor: '#000000', icon: <UnorderedListOutlined /> },
  SET: { bgColor: '#f5f5f5', textColor: '#000000', icon: <FormOutlined /> },
  Other: { bgColor: '#f5f5f5', textColor: '#000000', icon: null },
};

const renderQueryType = (queryType: string) => {
  const config = queryTypeConfig[queryType] || queryTypeConfig['Other'];
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        padding: '4px 12px',
        borderRadius: '4px',
        backgroundColor: config.bgColor,
        color: config.textColor,
        fontWeight: 500,
        fontSize: '12px',
      }}
    >
      {config.icon}
      {queryType}
    </span>
  );
};

const renderStatusBadge = (state: string) => {
  const config = stateConfig[state] || { bgColor: '#f5f5f5', textColor: '#000000', icon: null };
  return (
    <span
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        padding: '4px 12px',
        borderRadius: '4px',
        backgroundColor: config.bgColor,
        color: config.textColor,
        fontWeight: 500,
        fontSize: '12px',
      }}
    >
      {config.icon}
      {state}
    </span>
  );
};

// Helper to get unique values for filters
const getUniqueValues = (data: QueryProfile[], key: keyof QueryProfile): string[] => {
  const values = new Set<string>();
  data.forEach((item) => {
    const value = item[key];
    if (value && typeof value === 'string') {
      values.add(value);
    }
  });
  return Array.from(values).sort();
};

// Detect query type from SQL text
const detectQueryType = (sql: string): string => {
  if (!sql) return 'Unknown';

  // Normalize: trim whitespace and convert to uppercase
  const normalized = sql.trim().toUpperCase();

  // Skip comments and get first meaningful word
  const lines = normalized.split('\n');
  let firstWord = '';

  for (const line of lines) {
    const trimmed = line.trim();
    // Skip comment lines
    if (trimmed.startsWith('--') || trimmed.startsWith('/*')) {
      continue;
    }
    // Get first word from non-comment line
    const match = trimmed.match(/^([A-Z]+)/);
    if (match) {
      firstWord = match[1];
      break;
    }
  }

  // Categorize by SQL command
  if (firstWord.startsWith('SELECT') || firstWord.startsWith('WITH')) {
    return 'SELECT';
  }
  if (firstWord.startsWith('INSERT')) {
    return 'INSERT';
  }
  if (firstWord.startsWith('UPDATE')) {
    return 'UPDATE';
  }
  if (firstWord.startsWith('DELETE')) {
    return 'DELETE';
  }
  if (firstWord.startsWith('CREATE') || firstWord.startsWith('CTAS')) {
    return 'CREATE';
  }
  if (firstWord.startsWith('DROP') || firstWord.startsWith('TRUNCATE')) {
    return 'DROP';
  }
  if (firstWord.startsWith('ALTER')) {
    return 'ALTER';
  }
  if (firstWord.startsWith('SHOW') || firstWord.startsWith('DESCRIBE') || firstWord.startsWith('DESC')) {
    return 'SHOW/DESC';
  }
  if (firstWord.startsWith('EXPLAIN')) {
    return 'EXPLAIN';
  }
  if (firstWord.startsWith('USE')) {
    return 'USE';
  }
  if (firstWord.startsWith('SET')) {
    return 'SET';
  }

  return 'Other';
};

export default function ProfilesPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [selectedRunningIds, setSelectedRunningIds] = useState<React.Key[]>([]);
  const [isCancelling, setIsCancelling] = useState(false);
  const [profilesLimit, setProfilesLimit] = useState(100);

  // Fetch profiles (running + completed)
  const { data: profilesData, isLoading, error } = useQuery({
    queryKey: ['profiles'],
    queryFn: getQueryProfiles,
    refetchInterval: 5000, // Auto-refresh every 5 seconds
  });

  // Cancel mutation
  const cancelMutation = useMutation({
    mutationFn: cancelQuery,
  });

  const runningQueries = profilesData?.runningQueries || [];

  // Filter completed queries based on search text
  const filteredFinished = useMemo(() => {
    const queries = profilesData?.finishedQueries || [];
    if (!searchText) {
      return queries;
    }
    const lowerSearch = searchText.toLowerCase();
    return queries.filter(
      (q) =>
        q.user.toLowerCase().includes(lowerSearch) ||
        q.query.toLowerCase().includes(lowerSearch)
    );
  }, [profilesData?.finishedQueries, searchText]);

  // Format timestamp
  const formatDate = (timestamp: number) => {
    const date = new Date(timestamp);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
  };

  // Truncate query text for display
  const truncateQuery = (query: string, maxLength: number = 100) => {
    return query.length > maxLength ? query.substring(0, maxLength) + '...' : query;
  };

  // Handle running a query in SQL Lab
  const handleRunQuery = useCallback(
    (query: string) => {
      navigate('/query', {
        state: {
          loadQuery: {
            sql: query,
          },
        },
      });
    },
    [navigate]
  );

  // Handle cancel selected queries with sequential cancellation and error tracking
  const handleCancelSelected = useCallback(async () => {
    if (selectedRunningIds.length === 0) {
      message.warning('No queries selected');
      return;
    }

    const queryCount = selectedRunningIds.length;
    const showConfirm = queryCount > 1;

    const performCancel = async () => {
      setIsCancelling(true);
      const failures: Array<{ id: string; error: string }> = [];
      let successCount = 0;

      // Cancel queries sequentially to avoid race conditions
      for (const id of selectedRunningIds) {
        try {
          await cancelMutation.mutateAsync(id as string);
          successCount++;
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : 'Unknown error';
          failures.push({ id: id as string, error: errorMsg });
        }
      }

      setIsCancelling(false);

      // Show comprehensive feedback
      if (failures.length === 0) {
        message.success(`Successfully cancelled ${successCount} ${successCount === 1 ? 'query' : 'queries'}`);
        setSelectedRunningIds([]);
        queryClient.invalidateQueries({ queryKey: ['profiles'] });
      } else if (successCount === 0) {
        // All failed
        const errorDetails = failures.map((f) => `${f.id}: ${f.error}`).join('\n');
        message.error({
          content: (
            <div>
              <div>Failed to cancel all {queryCount} queries:</div>
              <div style={{ marginTop: 8, maxHeight: 200, overflow: 'auto', fontSize: 12, whiteSpace: 'pre-wrap' }}>
                {errorDetails}
              </div>
            </div>
          ),
          duration: 5,
        });
      } else {
        // Partial success
        const errorDetails = failures.map((f) => `${f.id}: ${f.error}`).join('\n');
        message.warning({
          content: (
            <div>
              <div>Cancelled {successCount}/{queryCount} queries</div>
              <div style={{ marginTop: 8, maxHeight: 200, overflow: 'auto', fontSize: 12, whiteSpace: 'pre-wrap' }}>
                Failed:\n{errorDetails}
              </div>
            </div>
          ),
          duration: 5,
        });
        setSelectedRunningIds([]);
        queryClient.invalidateQueries({ queryKey: ['profiles'] });
      }
    };

    if (showConfirm) {
      Modal.confirm({
        title: 'Cancel Multiple Queries',
        icon: <ExclamationCircleOutlined />,
        content: `Are you sure you want to cancel ${queryCount} selected ${queryCount === 1 ? 'query' : 'queries'}?`,
        okText: 'Cancel Queries',
        okType: 'danger',
        cancelText: 'Keep Queries',
        onOk() {
          performCancel();
        },
      });
    } else {
      performCancel();
    }
  }, [selectedRunningIds, cancelMutation, queryClient]);

  // Get unique query types for running queries
  const runningQueryTypes = Array.from(new Set(runningQueries.map((q) => detectQueryType(q.query)))).sort();
  const runningQueryTypeOptions = runningQueryTypes.map((t) => ({ text: t, value: t }));

  // Get unique values for running queries filters
  const runningUserOptions = getUniqueValues(runningQueries, 'user').map((u) => ({ text: u, value: u }));
  const runningForemanOptions = getUniqueValues(runningQueries, 'foreman').map((f) => ({ text: f, value: f }));

  // Table columns for running queries
  const runningColumns: ColumnsType<QueryProfile> = [
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      fixed: 'left',
      render: (_: unknown, record: QueryProfile) => (
        <Tooltip title="Run in SQL Lab">
          <Button
            type="text"
            size="small"
            icon={<PlayCircleOutlined />}
            onClick={() => handleRunQuery(record.query)}
            style={{ color: '#1890ff' }}
          />
        </Tooltip>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'state',
      key: 'state',
      width: 130,
      sorter: (a, b) => a.state.localeCompare(b.state),
      filters: [
        { text: 'Running', value: 'Running' },
        { text: 'Cancellation Requested', value: 'Cancellation_Requested' },
      ],
      onFilter: (value, record) => record.state === value,
      render: (state: string) => renderStatusBadge(state),
    },
    {
      title: 'Start Time',
      dataIndex: 'startTime',
      key: 'startTime',
      width: 180,
      sorter: (a, b) => a.startTime - b.startTime,
      render: (timestamp: number) => (
        <Text type="secondary">{formatDate(timestamp)}</Text>
      ),
    },
    {
      title: 'User',
      dataIndex: 'user',
      key: 'user',
      width: 120,
      sorter: (a, b) => a.user.localeCompare(b.user),
      filters: runningUserOptions,
      onFilter: (value, record) => record.user === value,
    },
    {
      title: 'Query',
      dataIndex: 'query',
      key: 'query',
      sorter: (a, b) => a.query.localeCompare(b.query),
      render: (query: string, record: QueryProfile) => (
        <Tooltip title={query}>
          <a href={`/profiles/${record.queryId}`} target="_blank" rel="noreferrer">
            {truncateQuery(query)}
          </a>
        </Tooltip>
      ),
    },
    {
      title: 'Query Type',
      key: 'queryType',
      width: 120,
      sorter: (a, b) => detectQueryType(a.query).localeCompare(detectQueryType(b.query)),
      filters: runningQueryTypeOptions,
      onFilter: (value, record) => detectQueryType(record.query) === value,
      render: (_: unknown, record: QueryProfile) => renderQueryType(detectQueryType(record.query)),
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      key: 'duration',
      width: 100,
      sorter: (a, b) => {
        const aDur = a.duration ? parseFloat(a.duration) : 0;
        const bDur = b.duration ? parseFloat(b.duration) : 0;
        return aDur - bDur;
      },
      render: (duration: string | undefined) => (
        <Text type="secondary">{duration || '-'}</Text>
      ),
    },
    {
      title: 'Foreman',
      dataIndex: 'foreman',
      key: 'foreman',
      width: 150,
      sorter: (a, b) => a.foreman.localeCompare(b.foreman),
      filters: runningForemanOptions,
      onFilter: (value, record) => record.foreman === value,
    },
  ];

  // Get unique query types for finished queries
  const finishedQueryTypes = Array.from(new Set(filteredFinished.map((q) => detectQueryType(q.query)))).sort();
  const finishedQueryTypeOptions = finishedQueryTypes.map((t) => ({ text: t, value: t }));

  // Get unique values for finished queries filters
  const finishedUserOptions = getUniqueValues(filteredFinished, 'user').map((u) => ({ text: u, value: u }));
  const finishedForemanOptions = getUniqueValues(filteredFinished, 'foreman').map((f) => ({ text: f, value: f }));
  const queueOptions = getUniqueValues(filteredFinished, 'queueName').map((q) => ({ text: q, value: q }));

  // Table columns for finished queries
  const finishedColumns: ColumnsType<QueryProfile> = [
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      fixed: 'left',
      render: (_: unknown, record: QueryProfile) => (
        <Tooltip title="Run in SQL Lab">
          <Button
            type="text"
            size="small"
            icon={<PlayCircleOutlined />}
            onClick={() => handleRunQuery(record.query)}
            style={{ color: '#1890ff' }}
          />
        </Tooltip>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'state',
      key: 'state',
      width: 130,
      sorter: (a, b) => a.state.localeCompare(b.state),
      filters: [
        { text: 'Succeeded', value: 'Succeeded' },
        { text: 'Failed', value: 'Failed' },
        { text: 'Cancelled', value: 'Cancelled' },
        { text: 'Cancellation Requested', value: 'Cancellation_Requested' },
      ],
      onFilter: (value, record) => record.state === value,
      render: (state: string) => renderStatusBadge(state),
    },
    {
      title: 'Start Time',
      dataIndex: 'startTime',
      key: 'startTime',
      width: 180,
      sorter: (a, b) => a.startTime - b.startTime,
      render: (timestamp: number) => (
        <Text type="secondary">{formatDate(timestamp)}</Text>
      ),
    },
    {
      title: 'User',
      dataIndex: 'user',
      key: 'user',
      width: 120,
      sorter: (a, b) => a.user.localeCompare(b.user),
      filters: finishedUserOptions,
      onFilter: (value, record) => record.user === value,
    },
    {
      title: 'Query',
      dataIndex: 'query',
      key: 'query',
      sorter: (a, b) => a.query.localeCompare(b.query),
      render: (query: string, record: QueryProfile) => (
        <Tooltip title={query}>
          <a href={`/profiles/${record.queryId}`} target="_blank" rel="noreferrer">
            {truncateQuery(query)}
          </a>
        </Tooltip>
      ),
    },
    {
      title: 'Query Type',
      key: 'queryType',
      width: 120,
      sorter: (a, b) => detectQueryType(a.query).localeCompare(detectQueryType(b.query)),
      filters: finishedQueryTypeOptions,
      onFilter: (value, record) => detectQueryType(record.query) === value,
      render: (_: unknown, record: QueryProfile) => renderQueryType(detectQueryType(record.query)),
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      key: 'duration',
      width: 100,
      sorter: (a, b) => {
        const aDur = a.duration ? parseFloat(a.duration) : 0;
        const bDur = b.duration ? parseFloat(b.duration) : 0;
        return aDur - bDur;
      },
      render: (duration: string | undefined) => (
        <Text type="secondary">{duration || '-'}</Text>
      ),
    },
    {
      title: 'Foreman',
      dataIndex: 'foreman',
      key: 'foreman',
      width: 150,
      sorter: (a, b) => a.foreman.localeCompare(b.foreman),
      filters: finishedForemanOptions,
      onFilter: (value, record) => record.foreman === value,
    },
    {
      title: 'Queue',
      dataIndex: 'queueName',
      key: 'queueName',
      width: 120,
      sorter: (a, b) => {
        const aQueue = a.queueName || '';
        const bQueue = b.queueName || '';
        return aQueue.localeCompare(bQueue);
      },
      filters: queueOptions,
      onFilter: (value, record) => record.queueName === value,
      render: (queueName: string | undefined) => {
        if (!queueName || queueName === '-') {
          return <Text type="secondary">-</Text>;
        }
        return <Text>{queueName}</Text>;
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
                Failed to load profiles: {(error as Error).message}
              </Text>
            }
          />
        </Card>
      </div>
    );
  }

  return (
    <div style={{ padding: 24 }}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* Controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          <span>Load profiles limit:</span>
          <Select
            style={{ width: 150 }}
            value={profilesLimit}
            onChange={setProfilesLimit}
            options={[
              { label: '50', value: 50 },
              { label: '100', value: 100 },
              { label: '250', value: 250 },
              { label: '500', value: 500 },
              { label: '1000', value: 1000 },
            ]}
          />
        </div>

        {/* Running Queries Section */}
        {runningQueries.length > 0 && (
          <Card
            title="Running Queries"
            extra={
              <Button
                danger
                onClick={handleCancelSelected}
                disabled={selectedRunningIds.length === 0 || isCancelling}
                loading={isCancelling}
              >
                Cancel Selected
              </Button>
            }
          >
            {isLoading ? (
              <div style={{ textAlign: 'center', padding: 40 }}>
                <Spin size="large" />
              </div>
            ) : (
              <Table
                dataSource={runningQueries}
                columns={runningColumns}
                rowKey="queryId"
                rowSelection={{
                  selectedRowKeys: selectedRunningIds,
                  onChange: (selectedKeys) => setSelectedRunningIds(selectedKeys),
                }}
                pagination={{
                  pageSize: 10,
                  showSizeChanger: true,
                  showTotal: (total) => `${total} queries`,
                }}
              />
            )}
          </Card>
        )}

        {/* Completed Queries Section */}
        <Card title="Completed Queries">
          <Space direction="vertical" style={{ width: '100%' }} size="middle">
            {/* Search */}
            <Input
              placeholder="Search queries by user or SQL text..."
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
            ) : filteredFinished.length === 0 ? (
              <Empty
                description={
                  searchText ? 'No queries match your search' : 'No completed queries'
                }
              />
            ) : (
              <Table
                dataSource={filteredFinished}
                columns={finishedColumns}
                rowKey="queryId"
                pagination={{
                  pageSize: 20,
                  showSizeChanger: true,
                  showTotal: (total) => `${total} queries`,
                }}
              />
            )}
          </Space>
        </Card>
      </Space>
    </div>
  );
}
