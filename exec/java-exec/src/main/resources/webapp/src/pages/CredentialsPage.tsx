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
import { useMemo, useRef, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Empty,
  Form,
  Input,
  Modal,
  Space,
  Spin,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import {
  ApiOutlined,
  KeyOutlined,
  LockOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { ColumnsType } from 'antd/es/table';
import {
  getCredentialPlugins,
  updateCredentials,
  type CredentialPlugin,
} from '../api/credentials';
import { getErrorMessage } from '../api/client';
import { usePageChrome } from '../contexts/AppChromeContext';

const { Text, Title, Paragraph } = Typography;

interface EditingState {
  plugin: CredentialPlugin;
  username: string;
  password: string;
}

function CredentialsPage() {
  const queryClient = useQueryClient();
  const [editing, setEditing] = useState<EditingState | null>(null);
  // Track the open OAuth popup per plugin so we can poll for close.
  const pollersRef = useRef<Map<string, number>>(new Map());

  const {
    data: plugins,
    isLoading,
    error,
    refetch,
    isRefetching,
  } = useQuery({
    queryKey: ['credentials'],
    queryFn: getCredentialPlugins,
  });

  const updateMutation = useMutation({
    mutationFn: ({ name, username, password }: { name: string; username: string; password: string }) =>
      updateCredentials(name, username, password),
    onSuccess: () => {
      message.success('Credentials updated');
      queryClient.invalidateQueries({ queryKey: ['credentials'] });
      setEditing(null);
    },
    onError: (err) => {
      message.error(`Couldn't update credentials: ${getErrorMessage(err)}`);
    },
  });

  // ── OAuth popup flow ──
  const startAuthorize = (plugin: CredentialPlugin) => {
    if (!plugin.authorizationUrl) {
      message.error(`No authorization URL is configured for ${plugin.name}.`);
      return;
    }
    // Match the legacy popup geometry (450 x 600) so providers' login screens render correctly.
    const popup = window.open(
      plugin.authorizationUrl,
      `authorize-${plugin.name}`,
      'toolbar=no,menubar=no,scrollbars=yes,resizable=yes,top=200,left=200,width=450,height=600',
    );
    if (!popup) {
      message.error('Popup was blocked. Please allow popups for this site and try again.');
      return;
    }
    // Poll until the popup closes, then refetch so the row reflects the new auth state.
    const intervalId = window.setInterval(() => {
      if (popup.closed) {
        window.clearInterval(intervalId);
        pollersRef.current.delete(plugin.name);
        queryClient.invalidateQueries({ queryKey: ['credentials'] });
      }
    }, 1000);
    pollersRef.current.set(plugin.name, intervalId);
  };

  // ── Toolbar ──
  const toolbarActions = useMemo(
    () => (
      <Button
        size="small"
        icon={<ReloadOutlined />}
        loading={isRefetching}
        onClick={() => refetch()}
      >
        Refresh
      </Button>
    ),
    [isRefetching, refetch],
  );
  usePageChrome({
    breadcrumb: [
      { key: 'administration', label: 'Administration' },
      { key: 'credentials', label: 'Credentials' },
    ],
    toolbarActions,
  });

  // ── Table ──
  const columns: ColumnsType<CredentialPlugin> = useMemo(
    () => [
      {
        title: 'Plugin',
        dataIndex: 'name',
        key: 'name',
        render: (name: string, row) => (
          <Space size={8}>
            {row.isOauth ? <SafetyCertificateOutlined /> : <KeyOutlined />}
            <Text strong>{name}</Text>
          </Space>
        ),
      },
      {
        title: 'Auth type',
        key: 'authType',
        width: 140,
        render: (_value: unknown, row) =>
          row.isOauth ? (
            <Tag color="blue" icon={<SafetyCertificateOutlined />}>OAuth 2.0</Tag>
          ) : (
            <Tag icon={<LockOutlined />}>User/password</Tag>
          ),
      },
      {
        title: 'Status',
        key: 'status',
        width: 110,
        render: (_value: unknown, row) =>
          row.enabled ? (
            <Tag color="green">Enabled</Tag>
          ) : (
            <Tag>Disabled</Tag>
          ),
      },
      {
        title: 'Action',
        key: 'action',
        width: 220,
        render: (_value: unknown, row) =>
          row.isOauth ? (
            <Tooltip
              title={
                row.authorizationUrl
                  ? 'Open the provider authorize page in a popup.'
                  : 'Authorize URL not configured for this plugin.'
              }
            >
              <Button
                icon={<ApiOutlined />}
                onClick={() => startAuthorize(row)}
                disabled={!row.authorizationUrl}
              >
                Authorize
              </Button>
            </Tooltip>
          ) : (
            <Button
              icon={<KeyOutlined />}
              onClick={() => setEditing({ plugin: row, username: '', password: '' })}
            >
              Update credentials
            </Button>
          ),
      },
    ],
    // startAuthorize is recreated each render; queryClient is stable. We intentionally don't
    // re-derive columns when startAuthorize changes because its only deps are stable too.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  if (isLoading) {
    return (
      <div style={{ padding: 32, textAlign: 'center' }}>
        <Spin />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          type="error"
          showIcon
          message="Couldn't load credentials"
          description={getErrorMessage(error)}
          action={
            <Button size="small" onClick={() => refetch()}>
              Retry
            </Button>
          }
        />
      </div>
    );
  }

  const rows = plugins ?? [];

  return (
    <div className="page-credentials" style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0, marginBottom: 6 }}>
        <KeyOutlined /> Credentials
      </Title>
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        Manage your personal credentials for storage plugins that use per-user authentication
        (USER_TRANSLATION). OAuth-enabled plugins use a popup to complete the provider's
        authorization flow.
      </Paragraph>

      <Card size="small" bodyStyle={{ padding: 0 }}>
        {rows.length === 0 ? (
          <div style={{ padding: 32 }}>
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="No plugins are configured with per-user authentication."
            />
          </div>
        ) : (
          <Table
            rowKey="name"
            dataSource={rows}
            columns={columns}
            pagination={false}
            size="small"
          />
        )}
      </Card>

      <Modal
        open={editing !== null}
        title={editing ? `Update credentials — ${editing.plugin.name}` : 'Update credentials'}
        onCancel={() => (updateMutation.isPending ? undefined : setEditing(null))}
        onOk={() => {
          if (!editing) {
            return;
          }
          if (!editing.username) {
            message.warning('Username is required.');
            return;
          }
          updateMutation.mutate({
            name: editing.plugin.name,
            username: editing.username,
            password: editing.password,
          });
        }}
        okText="Save"
        okButtonProps={{ loading: updateMutation.isPending }}
        cancelButtonProps={{ disabled: updateMutation.isPending }}
        destroyOnClose
      >
        {editing && (
          <Form layout="vertical">
            <Form.Item label="Username" required>
              <Input
                value={editing.username}
                onChange={(e) => setEditing({ ...editing, username: e.target.value })}
                autoFocus
                placeholder="Username"
              />
            </Form.Item>
            <Form.Item label="Password">
              <Input.Password
                value={editing.password}
                onChange={(e) => setEditing({ ...editing, password: e.target.value })}
                placeholder="Password"
              />
            </Form.Item>
            <Text type="secondary" style={{ fontSize: 12 }}>
              These credentials are stored per user and are used only for queries you run
              against <Text code>{editing.plugin.name}</Text>.
            </Text>
          </Form>
        )}
      </Modal>
    </div>
  );
}

export default CredentialsPage;
