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
import { useMemo, useState } from 'react';
import { Modal, Table, Input, Tag, Tooltip, Typography, Button, Popconfirm, Space } from 'antd';
import { CheckCircleOutlined, CloseCircleOutlined, DeleteOutlined, SearchOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { QueryHistoryEntry } from '../../types';

const { Text } = Typography;

interface QueryHistoryModalProps {
  open: boolean;
  onClose: () => void;
  history: QueryHistoryEntry[];
  onSelectQuery: (sql: string) => void;
  onClearHistory: () => void;
}

function formatDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  return `${(ms / 1000).toFixed(2)}s`;
}

export default function QueryHistoryModal({
  open,
  onClose,
  history,
  onSelectQuery,
  onClearHistory,
}: QueryHistoryModalProps) {
  const [searchText, setSearchText] = useState('');

  const filteredHistory = useMemo(() => {
    if (!searchText.trim()) {
      return history;
    }
    const lower = searchText.toLowerCase();
    return history.filter((entry) => entry.sql.toLowerCase().includes(lower));
  }, [history, searchText]);

  const columns: ColumnsType<QueryHistoryEntry> = [
    {
      title: 'SQL',
      dataIndex: 'sql',
      width: '40%',
      render: (sql: string) => (
        <Tooltip title={sql} placement="topLeft" overlayStyle={{ maxWidth: 500 }}>
          <Text code style={{ fontSize: 12 }}>
            {sql.length > 80 ? sql.slice(0, 80) + '...' : sql}
          </Text>
        </Tooltip>
      ),
    },
    {
      title: 'Status',
      dataIndex: 'status',
      width: 100,
      filters: [
        { text: 'Success', value: 'success' },
        { text: 'Error', value: 'error' },
      ],
      onFilter: (value, record) => record.status === value,
      render: (status: string) =>
        status === 'success' ? (
          <Tag icon={<CheckCircleOutlined />} color="success">
            Success
          </Tag>
        ) : (
          <Tag icon={<CloseCircleOutlined />} color="error">
            Error
          </Tag>
        ),
    },
    {
      title: 'Rows',
      dataIndex: 'rowCount',
      width: 80,
      align: 'right',
      render: (count: number) => count.toLocaleString(),
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      width: 100,
      sorter: (a, b) => a.duration - b.duration,
      render: (ms: number) => formatDuration(ms),
    },
    {
      title: 'Run At',
      dataIndex: 'timestamp',
      width: 170,
      defaultSortOrder: 'descend',
      sorter: (a, b) => a.timestamp - b.timestamp,
      render: (ts: number) => new Date(ts).toLocaleString(),
    },
  ];

  return (
    <Modal
      title="Query History"
      open={open}
      onCancel={onClose}
      footer={null}
      width={900}
      destroyOnClose
    >
      <Space style={{ width: '100%', marginBottom: 16 }} size="middle">
        <Input
          placeholder="Search SQL..."
          prefix={<SearchOutlined />}
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          allowClear
          style={{ width: 300 }}
        />
        <Popconfirm
          title="Clear all query history?"
          description="This action cannot be undone."
          onConfirm={onClearHistory}
          okText="Clear"
          okButtonProps={{ danger: true }}
        >
          <Button icon={<DeleteOutlined />} danger disabled={history.length === 0}>
            Clear History
          </Button>
        </Popconfirm>
      </Space>
      <Table
        dataSource={filteredHistory}
        columns={columns}
        rowKey="id"
        size="small"
        pagination={{ pageSize: 15 }}
        onRow={(record) => ({
          onClick: () => {
            onSelectQuery(record.sql);
            onClose();
          },
          style: { cursor: 'pointer' },
        })}
        locale={{ emptyText: searchText ? 'No matching queries' : 'No query history yet' }}
      />
    </Modal>
  );
}
