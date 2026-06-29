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
  Collapse,
  Empty,
  Input,
  Row,
  Space,
  Spin,
  Statistic,
  Switch,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import {
  CopyOutlined,
  PartitionOutlined,
  ReloadOutlined,
  WarningOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { getThreadDump, type ThreadEntry } from '../api/threads';
import { getErrorMessage } from '../api/client';
import { usePageChrome } from '../contexts/AppChromeContext';
import { useCurrentUser } from '../hooks/useCurrentUser';

const { Paragraph, Text, Title } = Typography;

const STATE_COLOR: Record<string, string> = {
  RUNNABLE: 'green',
  BLOCKED: 'red',
  WAITING: 'orange',
  TIMED_WAITING: 'gold',
  NEW: 'blue',
  TERMINATED: 'default',
};

function stateColor(state: string): string | undefined {
  return STATE_COLOR[state] ?? undefined;
}

/** Render the dump as the JDK's stack-trace-style text — for copy-to-clipboard. */
function renderDumpAsText(entries: ThreadEntry[]): string {
  return entries
    .map((t) => {
      const header = `"${t.name}" #${t.id}${t.daemon ? ' daemon' : ''} prio=${t.priority} ${t.state}`;
      const lock = t.lockName ? `\n  - waiting on <${t.lockName}>${t.lockOwnerName ? ` (owned by "${t.lockOwnerName}" #${t.lockOwnerId})` : ''}` : '';
      const stack = t.stackTrace.map((f) => `\tat ${f}`).join('\n');
      return `${header}${lock}\n${stack}`;
    })
    .join('\n\n');
}

function ThreadsPage() {
  const { user } = useCurrentUser();
  const [search, setSearch] = useState('');
  const [stateFilter, setStateFilter] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const {
    data: dump,
    isLoading,
    error,
    refetch,
    isRefetching,
  } = useQuery({
    queryKey: ['threads'],
    queryFn: getThreadDump,
    refetchInterval: autoRefresh ? 3_000 : false,
  });

  const handleCopy = async () => {
    if (!dump) {
      return;
    }
    try {
      await navigator.clipboard.writeText(renderDumpAsText(dump.threads));
      message.success('Thread dump copied to clipboard');
    } catch (err) {
      message.error(`Couldn't copy: ${(err as Error).message}`);
    }
  };

  const toolbarActions = useMemo(
    () => (
      <Space size={8}>
        <Tooltip title="Refresh every 3 seconds">
          <Space size={4}>
            <Switch
              size="small"
              checked={autoRefresh}
              onChange={setAutoRefresh}
            />
            <Text type="secondary" style={{ fontSize: 12 }}>Auto-refresh</Text>
          </Space>
        </Tooltip>
        <Button
          size="small"
          icon={<ReloadOutlined />}
          loading={isRefetching}
          onClick={() => refetch()}
        >
          Refresh
        </Button>
        <Button
          size="small"
          icon={<CopyOutlined />}
          onClick={handleCopy}
          disabled={!dump}
        >
          Copy dump
        </Button>
      </Space>
    ),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [autoRefresh, dump, isRefetching, refetch],
  );

  usePageChrome({
    breadcrumb: [
      { key: 'administration', label: 'Administration' },
      { key: 'threads', label: 'Threads' },
    ],
    toolbarActions,
  });

  // ── Admin gating ──
  // /api/v1/threads is @RolesAllowed(ADMIN_ROLE) on the server, so a non-admin
  // request will be rejected. We mirror the gate in the UI so non-admins see a
  // clear message instead of a network error.
  const isAdmin = user ? !user.authEnabled || user.isAdmin : false;
  if (user && !isAdmin) {
    return (
      <div style={{ padding: 24 }}>
        <Alert
          type="warning"
          showIcon
          message="Admin access required"
          description="Thread dumps are restricted to admin users because stack traces can leak in-flight query SQL and other sensitive context."
        />
      </div>
    );
  }

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
          message="Couldn't load thread dump"
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

  if (!dump) {
    return <Empty description="No data" />;
  }

  // ── Derived ──
  const stateCounts = dump.threads.reduce<Record<string, number>>((acc, t) => {
    acc[t.state] = (acc[t.state] ?? 0) + 1;
    return acc;
  }, {});
  const allStates = Object.keys(stateCounts).sort();

  const needle = search.trim().toLowerCase();
  const visible = dump.threads.filter((t) => {
    if (stateFilter && t.state !== stateFilter) {
      return false;
    }
    if (!needle) {
      return true;
    }
    if (t.name.toLowerCase().includes(needle)) {
      return true;
    }
    return t.stackTrace.some((frame) => frame.toLowerCase().includes(needle));
  });

  const deadlocked = new Set(dump.deadlockedThreadIds);

  return (
    <div className="page-threads" style={{ padding: 24 }}>
      <Title level={3} style={{ marginTop: 0, marginBottom: 6 }}>
        <PartitionOutlined /> Threads
      </Title>
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        Live JVM thread snapshot for this Drillbit. Stack traces are captured server-side; toggle
        auto-refresh to poll every three seconds.
      </Paragraph>

      {deadlocked.size > 0 && (
        <Alert
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          icon={<WarningOutlined />}
          message={`Deadlock detected — ${deadlocked.size} thread${deadlocked.size === 1 ? '' : 's'} affected`}
          description={
            <span>
              Thread IDs: {Array.from(deadlocked).map((id) => (
                <Text code key={id} style={{ marginRight: 6 }}>{id}</Text>
              ))}
            </span>
          }
        />
      )}

      {/* ── Summary cards ── */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Live threads" value={dump.count} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Daemon" value={dump.daemonCount} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Peak" value={dump.peakCount} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="Total started" value={dump.totalStartedCount} />
          </Card>
        </Col>
      </Row>

      {/* ── Filter ── */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap size={8}>
          <Input.Search
            allowClear
            placeholder="Filter by name or stack frame"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ width: 320 }}
          />
          <Space size={4}>
            <Tag
              color={stateFilter === null ? 'blue' : undefined}
              style={{ cursor: 'pointer' }}
              onClick={() => setStateFilter(null)}
            >
              All ({dump.threads.length})
            </Tag>
            {allStates.map((state) => (
              <Tag
                key={state}
                color={stateFilter === state ? 'blue' : stateColor(state)}
                style={{ cursor: 'pointer' }}
                onClick={() => setStateFilter(stateFilter === state ? null : state)}
              >
                {state} ({stateCounts[state]})
              </Tag>
            ))}
          </Space>
        </Space>
      </Card>

      {/* ── Thread list ── */}
      {visible.length === 0 ? (
        <Card>
          <Empty description="No threads match the current filter" />
        </Card>
      ) : (
        <Collapse
          accordion={false}
          size="small"
          items={visible.map((t) => ({
            key: String(t.id),
            label: (
              <Space size={8} style={{ width: '100%' }}>
                <Tag color={deadlocked.has(t.id) ? 'red' : stateColor(t.state)}>
                  {t.state}
                </Tag>
                <Text strong style={{ flexGrow: 1 }}>{t.name}</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>#{t.id}</Text>
                {t.daemon && <Tag>daemon</Tag>}
                {deadlocked.has(t.id) && (
                  <Tag color="red" icon={<WarningOutlined />}>deadlocked</Tag>
                )}
              </Space>
            ),
            children: (
              <div>
                {t.lockName && (
                  <Paragraph style={{ marginBottom: 8 }}>
                    <Text type="secondary">Waiting on </Text>
                    <Text code>{t.lockName}</Text>
                    {t.lockOwnerName && (
                      <>
                        {' '}<Text type="secondary">held by</Text>{' '}
                        <Text code>{t.lockOwnerName} #{t.lockOwnerId}</Text>
                      </>
                    )}
                  </Paragraph>
                )}
                <pre
                  style={{
                    margin: 0,
                    padding: 12,
                    background: 'var(--color-bg-window)',
                    border: '1px solid var(--color-separator)',
                    borderRadius: 'var(--radius-sm)',
                    fontSize: 12,
                    overflowX: 'auto',
                  }}
                >
                  {t.stackTrace.length === 0
                    ? '(no stack frames)'
                    : t.stackTrace.map((f) => `\tat ${f}`).join('\n')}
                </pre>
              </div>
            ),
          }))}
        />
      )}
    </div>
  );
}

export default ThreadsPage;
