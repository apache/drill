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
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Empty,
  Modal,
  Row,
  Space,
  Spin,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import {
  CheckCircleOutlined,
  ClusterOutlined,
  CrownOutlined,
  ExclamationCircleOutlined,
  LockOutlined,
  PoweroffOutlined,
  ReloadOutlined,
  SafetyCertificateOutlined,
  ThunderboltOutlined,
  UnlockOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import type { ColumnsType } from 'antd/es/table';
import {
  forcefulShutdown,
  getClusterInfo,
  getGracePeriod,
  gracefulShutdown,
  gracefulShutdownHost,
  quiescentMode,
  type DrillbitInfo,
} from '../api/cluster';
import { getErrorMessage } from '../api/client';
import { usePageChrome } from '../contexts/AppChromeContext';
import { useCurrentUser } from '../hooks/useCurrentUser';

const { Text, Title } = Typography;

type ShutdownKind = 'graceful' | 'quiescent' | 'forceful' | 'host';

interface PendingShutdown {
  kind: ShutdownKind;
  hostLabel?: string;
  drillbit?: DrillbitInfo;
}

const SHUTDOWN_COPY: Record<ShutdownKind, { title: string; verb: string; body: string; warn: boolean }> = {
  graceful: {
    title: 'Graceful shutdown',
    verb: 'Shut down gracefully',
    body: 'The local Drillbit will stop accepting new queries and exit once in-flight work drains.',
    warn: false,
  },
  quiescent: {
    title: 'Quiescent mode',
    verb: 'Enter quiescent mode',
    body: 'The local Drillbit will stop accepting new queries but keep serving existing ones. The process stays running.',
    warn: false,
  },
  forceful: {
    title: 'Forceful shutdown',
    verb: 'Shut down forcefully',
    body: 'The local Drillbit will terminate immediately. Running queries will fail.',
    warn: true,
  },
  host: {
    title: 'Graceful shutdown',
    verb: 'Shut down gracefully',
    body: 'The selected Drillbit will stop accepting new queries and exit once in-flight work drains.',
    warn: false,
  },
};

function StatePill({ state }: { state: string }) {
  const normalized = (state ?? 'UNKNOWN').toUpperCase();
  let color: string | undefined;
  let icon = <CheckCircleOutlined />;
  if (normalized === 'ONLINE' || normalized === 'STARTUP') {
    color = 'green';
  } else if (normalized === 'QUIESCENT' || normalized === 'GRACE') {
    color = 'orange';
    icon = <WarningOutlined />;
  } else if (normalized === 'OFFLINE' || normalized === 'SHUTDOWN') {
    color = 'red';
    icon = <ExclamationCircleOutlined />;
  }
  return (
    <Tag color={color} icon={icon}>
      {normalized}
    </Tag>
  );
}

function EncryptionRow({
  label,
  enabled,
  icon,
}: {
  label: string;
  enabled: boolean;
  icon: React.ReactNode;
}) {
  return (
    <Space size={6}>
      <span style={{ color: 'var(--color-text-tertiary)' }}>{icon}</span>
      <Text>{label}</Text>
      {enabled ? (
        <Tag color="green" icon={<LockOutlined />}>
          Enabled
        </Tag>
      ) : (
        <Tag icon={<UnlockOutlined />}>Disabled</Tag>
      )}
    </Space>
  );
}

function ClusterPage() {
  const queryClient = useQueryClient();
  const { user } = useCurrentUser();
  const [pendingShutdown, setPendingShutdown] = useState<PendingShutdown | null>(null);

  const {
    data: cluster,
    isLoading,
    error,
    refetch,
    isRefetching,
  } = useQuery({
    queryKey: ['cluster'],
    queryFn: getClusterInfo,
    refetchInterval: 15_000,
  });

  const { data: gracePeriodSecs } = useQuery({
    queryKey: ['cluster', 'gracePeriod'],
    queryFn: getGracePeriod,
    staleTime: 60_000,
  });

  const shutdownMutation = useMutation({
    mutationFn: async (req: PendingShutdown) => {
      if (req.kind === 'graceful') {
        return gracefulShutdown();
      }
      if (req.kind === 'quiescent') {
        return quiescentMode();
      }
      if (req.kind === 'forceful') {
        return forcefulShutdown();
      }
      if (req.kind === 'host' && req.hostLabel) {
        return { response: await gracefulShutdownHost(req.hostLabel) };
      }
      throw new Error('Unsupported shutdown kind');
    },
    onSuccess: (data) => {
      const text = typeof data === 'string' ? data : data?.response;
      message.success(text || 'Request submitted');
      queryClient.invalidateQueries({ queryKey: ['cluster'] });
    },
    onError: (err) => {
      message.error(`Shutdown request failed: ${getErrorMessage(err)}`);
    },
  });

  // ── Admin gating ──
  // user?.authEnabled === false ⇒ anonymous mode, everyone can act as admin.
  // user?.isAdmin ⇒ true admin in auth-enabled mode.
  // shouldShowAdminInfo from the cluster payload corroborates the same thing
  // for the snapshot we just loaded; combine both so we don't reveal/enable
  // controls when either signal disagrees.
  const localIsAdmin = user ? !user.authEnabled || user.isAdmin : false;
  const serverShowsAdminInfo = cluster?.shouldShowAdminInfo ?? false;
  const canShutdown = localIsAdmin && (cluster?.authEnabled === false || serverShowsAdminInfo);

  // ── Toolbar ──
  const toolbarActions = useMemo(
    () => (
      <Space size={8}>
        <Button
          size="small"
          icon={<ReloadOutlined />}
          loading={isRefetching}
          onClick={() => refetch()}
        >
          Refresh
        </Button>
        {canShutdown && (
          <>
            <Button
              size="small"
              icon={<ThunderboltOutlined />}
              onClick={() => setPendingShutdown({ kind: 'quiescent' })}
            >
              Quiescent mode
            </Button>
            <Button
              size="small"
              icon={<PoweroffOutlined />}
              onClick={() => setPendingShutdown({ kind: 'graceful' })}
            >
              Graceful shutdown
            </Button>
            <Button
              size="small"
              danger
              icon={<WarningOutlined />}
              onClick={() => setPendingShutdown({ kind: 'forceful' })}
            >
              Forceful shutdown
            </Button>
          </>
        )}
      </Space>
    ),
    [canShutdown, isRefetching, refetch],
  );
  usePageChrome({
    breadcrumb: [
      { key: 'administration', label: 'Administration' },
      { key: 'cluster', label: 'Cluster' },
    ],
    toolbarActions,
  });

  // ── Drillbit table ──
  const columns: ColumnsType<DrillbitInfo> = useMemo(
    () => [
      {
        title: 'Address',
        dataIndex: 'address',
        key: 'address',
        render: (address: string, row) => (
          <Space size={6}>
            {row.current && (
              <Tooltip title="This is the Drillbit serving the current request.">
                <CrownOutlined style={{ color: 'var(--color-warning)' }} />
              </Tooltip>
            )}
            <Text strong={row.current}>{address}</Text>
          </Space>
        ),
      },
      {
        title: 'State',
        dataIndex: 'state',
        key: 'state',
        width: 140,
        render: (state: string) => <StatePill state={state} />,
      },
      {
        title: 'Version',
        dataIndex: 'version',
        key: 'version',
        width: 200,
        render: (version: string, row) => (
          <Space size={6}>
            <Text>{version}</Text>
            {!row.versionMatch && (
              <Tag color="orange" icon={<WarningOutlined />}>
                Mismatch
              </Tag>
            )}
          </Space>
        ),
      },
      {
        title: 'HTTP port',
        dataIndex: 'httpPort',
        key: 'httpPort',
        width: 100,
      },
      {
        title: 'User port',
        dataIndex: 'userPort',
        key: 'userPort',
        width: 100,
      },
      {
        title: 'Control port',
        dataIndex: 'controlPort',
        key: 'controlPort',
        width: 110,
      },
      {
        title: 'Data port',
        dataIndex: 'dataPort',
        key: 'dataPort',
        width: 100,
      },
      ...(canShutdown
        ? [
            {
              title: 'Actions',
              key: 'actions',
              width: 200,
              render: (_value: unknown, row: DrillbitInfo) => (
                <Button
                  size="small"
                  icon={<PoweroffOutlined />}
                  onClick={() =>
                    setPendingShutdown({
                      kind: 'host',
                      hostLabel: row.address,
                      drillbit: row,
                    })
                  }
                >
                  Graceful shutdown
                </Button>
              ),
            },
          ]
        : []),
    ],
    [canShutdown],
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
          message="Couldn't load cluster info"
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

  if (!cluster) {
    return <Empty description="No cluster data" />;
  }

  const drillbits = cluster.drillbits ?? [];
  const drillbitRowKey = (b: DrillbitInfo) => `${b.address}:${b.httpPort}:${b.controlPort}`;
  const onlineCount = drillbits.filter((b) =>
    ['ONLINE', 'STARTUP'].includes((b.state ?? '').toUpperCase()),
  ).length;

  return (
    <div className="page-cluster" style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0, marginBottom: 16 }}>
        <ClusterOutlined /> Cluster
      </Title>

      {cluster.mismatchedVersions.length > 0 && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="Version mismatch detected"
          description={
            <span>
              Current Drillbit is at <Text code>{cluster.currentVersion}</Text>. Other versions
              present in the cluster: {cluster.mismatchedVersions.map((v) => (
                <Text code key={v} style={{ marginRight: 6 }}>
                  {v}
                </Text>
              ))}
            </span>
          }
        />
      )}

      {/* ── Summary cards ── */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Drillbits"
              value={drillbits.length}
              prefix={<ClusterOutlined />}
              suffix={<Text type="secondary">{`/ ${onlineCount} online`}</Text>}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Cluster version"
              value={cluster.currentVersion}
              prefix={<SafetyCertificateOutlined />}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Authentication"
              value={cluster.authEnabled ? 'Enabled' : 'Disabled'}
              valueStyle={{
                color: cluster.authEnabled ? 'var(--color-success)' : 'var(--color-text-secondary)',
              }}
              prefix={cluster.authEnabled ? <LockOutlined /> : <UnlockOutlined />}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card>
            <Statistic
              title="Grace period"
              value={gracePeriodSecs ?? '—'}
              suffix={gracePeriodSecs !== undefined ? 'sec' : undefined}
            />
          </Card>
        </Col>
      </Row>

      {/* ── Drillbits ── */}
      <Card
        title="Drillbits"
        size="small"
        style={{ marginBottom: 16 }}
        bodyStyle={{ padding: 0 }}
      >
        <Table
          rowKey={drillbitRowKey}
          dataSource={drillbits}
          columns={columns}
          pagination={false}
          size="small"
        />
      </Card>

      {/* ── Encryption ── */}
      <Card title="Encryption" size="small" style={{ marginBottom: 16 }}>
        <Space direction="vertical" size={6}>
          <EncryptionRow
            label="User connections (SASL / SSL)"
            enabled={cluster.userEncryptionEnabled}
            icon={<SafetyCertificateOutlined />}
          />
          <EncryptionRow
            label="Bit-to-bit (SASL)"
            enabled={cluster.bitEncryptionEnabled}
            icon={<SafetyCertificateOutlined />}
          />
        </Space>
      </Card>

      {/* ── Queue ── */}
      <Card title="Distributed query queue" size="small" style={{ marginBottom: 16 }}>
        {cluster.queueInfo?.enabled ? (
          <Descriptions size="small" column={{ xs: 1, sm: 2, md: 3 }}>
            <Descriptions.Item label="Small queue size">{cluster.queueInfo.smallQueueSize}</Descriptions.Item>
            <Descriptions.Item label="Large queue size">{cluster.queueInfo.largeQueueSize}</Descriptions.Item>
            <Descriptions.Item label="Threshold">{cluster.queueInfo.threshold}</Descriptions.Item>
            <Descriptions.Item label="Small-query memory">{cluster.queueInfo.smallQueueMemory}</Descriptions.Item>
            <Descriptions.Item label="Large-query memory">{cluster.queueInfo.largeQueueMemory}</Descriptions.Item>
            <Descriptions.Item label="Total memory">{cluster.queueInfo.totalMemory}</Descriptions.Item>
          </Descriptions>
        ) : (
          <Text type="secondary">Distributed query queue is not enabled.</Text>
        )}
      </Card>

      {/* ── Admin info ── */}
      {serverShowsAdminInfo && (
        <Card title="Administration" size="small">
          <Descriptions size="small" column={{ xs: 1, sm: 2 }}>
            {cluster.processUser && (
              <Descriptions.Item label="Process user">{cluster.processUser}</Descriptions.Item>
            )}
            {cluster.processUserGroups && (
              <Descriptions.Item label="Process user groups">{cluster.processUserGroups}</Descriptions.Item>
            )}
            {cluster.adminUsers && (
              <Descriptions.Item label="Admin users">{cluster.adminUsers}</Descriptions.Item>
            )}
            {cluster.adminUserGroups && (
              <Descriptions.Item label="Admin user groups">{cluster.adminUserGroups}</Descriptions.Item>
            )}
          </Descriptions>
        </Card>
      )}

      {/* ── Shutdown confirmation ── */}
      <Modal
        open={pendingShutdown !== null}
        title={pendingShutdown ? SHUTDOWN_COPY[pendingShutdown.kind].title : ''}
        onCancel={() => setPendingShutdown(null)}
        onOk={() => {
          if (pendingShutdown) {
            shutdownMutation.mutate(pendingShutdown, {
              onSettled: () => setPendingShutdown(null),
            });
          }
        }}
        okText={pendingShutdown ? SHUTDOWN_COPY[pendingShutdown.kind].verb : 'Confirm'}
        okButtonProps={{
          danger: pendingShutdown ? SHUTDOWN_COPY[pendingShutdown.kind].warn : false,
          loading: shutdownMutation.isPending,
        }}
        cancelButtonProps={{ disabled: shutdownMutation.isPending }}
      >
        {pendingShutdown && (
          <Space direction="vertical" size={8}>
            <Text>{SHUTDOWN_COPY[pendingShutdown.kind].body}</Text>
            {pendingShutdown.kind === 'host' && pendingShutdown.drillbit && (
              <Text>
                Target: <Text code>{pendingShutdown.drillbit.address}</Text>
                {' on HTTP port '}
                <Text code>{pendingShutdown.drillbit.httpPort}</Text>
              </Text>
            )}
            {SHUTDOWN_COPY[pendingShutdown.kind].warn && (
              <Alert type="warning" showIcon message="This action is not reversible." />
            )}
          </Space>
        )}
      </Modal>
    </div>
  );
}

export default ClusterPage;
