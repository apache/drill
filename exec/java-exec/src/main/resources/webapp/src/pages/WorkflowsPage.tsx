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
import {
  Card,
  Table,
  Typography,
  Space,
  Tag,
  Button,
  Tooltip,
  Empty,
  Spin,
  Input,
  InputNumber,
  Switch,
  Popconfirm,
  message,
  Alert,
  Descriptions,
  Badge,
} from 'antd';
import {
  SearchOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  MinusCircleFilled,
  ClockCircleOutlined,
  DeleteOutlined,
  EditOutlined,
  ReloadOutlined,
  SettingOutlined,
  WarningOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
} from '@ant-design/icons';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  getSchedules,
  deleteSchedule,
  renewSchedule,
  runScheduleNow,
  updateSchedule,
  getSnapshots,
  getScheduleConfig,
  updateScheduleConfig,
} from '../api/schedules';
import { getSavedQueries } from '../api/savedQueries';
import type { QuerySchedule, QuerySnapshot } from '../types';
import type { ColumnsType } from 'antd/es/table';
import ScheduleModal from '../components/query-editor/ScheduleModal';
import Markdown from 'react-markdown';
import rehypeRaw from 'rehype-raw';

const { Title, Text } = Typography;

const FREQUENCY_LABELS: Record<string, string> = {
  hourly: 'Hourly',
  daily: 'Daily',
  weekly: 'Weekly',
  monthly: 'Monthly',
};

const DAY_LABELS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

function formatSchedule(s: QuerySchedule): string {
  const time = s.timeOfDay || '00:00';
  switch (s.frequency) {
    case 'hourly':
      return `Every hour at :${time.split(':')[1] || '00'}`;
    case 'daily':
      return `Daily at ${time}`;
    case 'weekly':
      return `${DAY_LABELS[s.dayOfWeek ?? 0]}s at ${time}`;
    case 'monthly':
      return `${s.dayOfMonth ?? 1}${ordinalSuffix(s.dayOfMonth ?? 1)} of month at ${time}`;
    default:
      return FREQUENCY_LABELS[s.frequency] || s.frequency;
  }
}

function ordinalSuffix(n: number): string {
  if (n >= 11 && n <= 13) {
    return 'th';
  }
  switch (n % 10) {
    case 1: return 'st';
    case 2: return 'nd';
    case 3: return 'rd';
    default: return 'th';
  }
}

function daysUntilExpiry(expiresAt?: string): number | null {
  if (!expiresAt) {
    return null;
  }
  const diff = new Date(expiresAt).getTime() - Date.now();
  return Math.ceil(diff / 86400000);
}

function RunStatusDots({ snapshots }: { snapshots: QuerySnapshot[] }) {
  const recent = snapshots.slice(0, 5);
  if (recent.length === 0) {
    return <Text type="secondary">No runs</Text>;
  }
  return (
    <Space size={2}>
      {recent.map((snap) => (
        <Tooltip key={snap.id} title={`${new Date(snap.executedAt).toLocaleString()} - ${snap.status}`}>
          {snap.status === 'success'
            ? <CheckCircleFilled style={{ color: '#52c41a', fontSize: 14 }} />
            : <CloseCircleFilled style={{ color: '#ff4d4f', fontSize: 14 }} />
          }
        </Tooltip>
      ))}
    </Space>
  );
}

export default function WorkflowsPage() {
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [editingSchedule, setEditingSchedule] = useState<{
    id: string;
    name: string;
    sql?: string;
  } | null>(null);
  const [runningIds, setRunningIds] = useState<Set<string>>(new Set());
  const [showConfig, setShowConfig] = useState(false);
  const [configDraft, setConfigDraft] = useState<{
    expirationEnabled: boolean;
    expirationDays: number;
    warningDaysBeforeExpiry: number;
  } | null>(null);

  const { data: serverConfig } = useQuery({
    queryKey: ['workflow-config'],
    queryFn: getScheduleConfig,
  });

  const config = useMemo(() => configDraft || serverConfig || {
    expirationEnabled: true,
    expirationDays: 90,
    warningDaysBeforeExpiry: 14,
  }, [configDraft, serverConfig]);

  const { data: schedules, isLoading } = useQuery({
    queryKey: ['schedules'],
    queryFn: getSchedules,
  });

  const { data: savedQueries } = useQuery({
    queryKey: ['savedQueries'],
    queryFn: getSavedQueries,
  });

  // Build a map of schedule ID -> snapshots
  const { data: allSnapshots } = useQuery({
    queryKey: ['all-snapshots', schedules?.map((s) => s.id).join(',')],
    queryFn: async () => {
      if (!schedules) {
        return {};
      }
      const map: Record<string, QuerySnapshot[]> = {};
      for (const s of schedules) {
        map[s.id] = await getSnapshots(s.id);
      }
      return map;
    },
    enabled: !!schedules && schedules.length > 0,
  });

  const queryMap = useMemo(() => {
    const m: Record<string, { name: string; sql: string }> = {};
    for (const q of savedQueries || []) {
      m[q.id] = { name: q.name, sql: q.sql };
    }
    return m;
  }, [savedQueries]);

  const filteredSchedules = useMemo(() => {
    if (!schedules) {
      return [];
    }
    if (!searchText) {
      return schedules;
    }
    const lower = searchText.toLowerCase();
    return schedules.filter((s) => {
      const qName = queryMap[s.savedQueryId]?.name || '';
      return (
        qName.toLowerCase().includes(lower) ||
        (s.description && s.description.toLowerCase().includes(lower)) ||
        s.frequency.toLowerCase().includes(lower)
      );
    });
  }, [schedules, searchText, queryMap]);

  // Schedules expiring soon
  const expiringSchedules = useMemo(() => {
    if (!config.expirationEnabled || !schedules) {
      return [];
    }
    return schedules.filter((s) => {
      const days = daysUntilExpiry(s.expiresAt);
      return days !== null && days > 0 && days <= config.warningDaysBeforeExpiry && s.enabled;
    });
  }, [schedules, config]);

  const handleDelete = async (id: string) => {
    try {
      await deleteSchedule(id);
      message.success('Schedule deleted');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to delete schedule');
    }
  };

  const handleRenew = async (id: string) => {
    try {
      await renewSchedule(id);
      message.success(`Schedule renewed for ${config.expirationDays} days`);
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to renew schedule');
    }
  };

  const handleRunNow = async (id: string) => {
    try {
      setRunningIds((prev) => new Set(prev).add(id));
      await runScheduleNow(id);
      message.success('Schedule executed successfully');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
      queryClient.invalidateQueries({ queryKey: ['all-snapshots'] });
    } catch {
      message.error('Failed to run schedule');
    } finally {
      setRunningIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  };

  const handleTogglePause = async (record: QuerySchedule) => {
    try {
      await updateSchedule(record.id, { paused: !record.paused });
      message.success(record.paused ? 'Schedule resumed' : 'Schedule paused');
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to update schedule');
    }
  };

  const handleSaveConfig = async () => {
    try {
      await updateScheduleConfig(config);
      message.success('Workflow settings saved');
      setConfigDraft(null);
      setShowConfig(false);
      queryClient.invalidateQueries({ queryKey: ['workflow-config'] });
      queryClient.invalidateQueries({ queryKey: ['schedules'] });
    } catch {
      message.error('Failed to save settings. Admin privileges may be required.');
    }
  };

  // Schedules with triggered alerts in their latest snapshot
  const schedulesWithAlerts = useMemo(() => {
    if (!schedules || !allSnapshots) {
      return [];
    }
    return schedules.filter((s) => {
      const snaps = allSnapshots[s.id] || [];
      const latest = snaps[0];
      return latest?.triggeredAlerts && latest.triggeredAlerts.length > 0;
    });
  }, [schedules, allSnapshots]);

  const columns: ColumnsType<QuerySchedule> = [
    {
      title: 'Query Name',
      key: 'name',
      sorter: (a, b) => {
        const aName = queryMap[a.savedQueryId]?.name || '';
        const bName = queryMap[b.savedQueryId]?.name || '';
        return aName.localeCompare(bName);
      },
      render: (_, record) => {
        const q = queryMap[record.savedQueryId];
        return (
          <Space direction="vertical" size={0}>
            <Text strong>{q?.name || record.savedQueryId}</Text>
            {record.description && (
              <Text type="secondary" style={{ fontSize: 12 }}>{record.description}</Text>
            )}
          </Space>
        );
      },
    },
    {
      title: 'Status',
      key: 'status',
      width: 100,
      render: (_, record) => {
        if (!record.enabled) {
          return <Tag icon={<MinusCircleFilled />}>Disabled</Tag>;
        }
        if (record.status === 'expired') {
          return <Tag color="error">Expired</Tag>;
        }
        if (record.status === 'paused' || record.paused) {
          return <Tag color="warning" icon={<PauseCircleOutlined />}>Paused</Tag>;
        }
        const days = daysUntilExpiry(record.expiresAt);
        if (days !== null && days > 0 && days <= config.warningDaysBeforeExpiry) {
          return (
            <Tooltip title={`Expires in ${days} day${days !== 1 ? 's' : ''}`}>
              <Tag color="orange" icon={<WarningOutlined />}>Expiring</Tag>
            </Tooltip>
          );
        }
        if (record.status === 'active') {
          return <Tag color="success" icon={<CheckCircleFilled />}>Active</Tag>;
        }
        return <Tag color="success" icon={<CheckCircleFilled />}>Active</Tag>;
      },
    },
    {
      title: 'Recent Runs',
      key: 'runs',
      width: 120,
      render: (_, record) => (
        <RunStatusDots snapshots={allSnapshots?.[record.id] || []} />
      ),
    },
    {
      title: 'Alerts',
      key: 'alerts',
      width: 80,
      render: (_, record) => {
        const latestSnap = (allSnapshots?.[record.id] || [])[0];
        const alertCount = latestSnap?.triggeredAlerts?.length || 0;
        return alertCount > 0
          ? <Tag color="red">{alertCount} alert{alertCount !== 1 ? 's' : ''}</Tag>
          : <Text type="secondary">None</Text>;
      },
    },
    {
      title: 'Results',
      key: 'results',
      width: 80,
      render: (_, record) => {
        return record.persistResults
          ? <Tag color="blue">{record.resultFormat || 'parquet'}</Tag>
          : <Text type="secondary">-</Text>;
      },
    },
    {
      title: 'Schedule',
      key: 'schedule',
      width: 200,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Text>{formatSchedule(record)}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>{FREQUENCY_LABELS[record.frequency]}</Text>
        </Space>
      ),
    },
    {
      title: 'Created',
      dataIndex: 'createdAt',
      key: 'createdAt',
      width: 140,
      sorter: (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
      render: (val: string) => (
        <Text type="secondary">{new Date(val).toLocaleDateString()}</Text>
      ),
    },
    {
      title: 'Last Run',
      dataIndex: 'lastRunAt',
      key: 'lastRunAt',
      width: 160,
      sorter: (a, b) => {
        const aTime = a.lastRunAt ? new Date(a.lastRunAt).getTime() : 0;
        const bTime = b.lastRunAt ? new Date(b.lastRunAt).getTime() : 0;
        return aTime - bTime;
      },
      render: (val?: string) => val
        ? <Text type="secondary">{new Date(val).toLocaleString()}</Text>
        : <Text type="secondary">Never</Text>,
    },
    {
      title: 'Expires',
      key: 'expires',
      width: 120,
      render: (_, record) => {
        if (!record.expiresAt) {
          return <Text type="secondary">Never</Text>;
        }
        const days = daysUntilExpiry(record.expiresAt);
        if (days !== null && days <= 0) {
          return <Tag color="error">Expired</Tag>;
        }
        if (days !== null && days <= config.warningDaysBeforeExpiry) {
          return (
            <Tooltip title={`Renew to extend ${config.expirationDays} more days`}>
              <Tag color="warning">{days}d left</Tag>
            </Tooltip>
          );
        }
        return <Text type="secondary">{days}d left</Text>;
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 220,
      render: (_, record) => {
        const q = queryMap[record.savedQueryId];
        const days = daysUntilExpiry(record.expiresAt);
        const needsRenewal = days !== null && days <= config.warningDaysBeforeExpiry;
        const isRunning = record.isRunning || runningIds.has(record.id);
        return (
          <Space>
            {!isRunning && (
              <Tooltip title="Run now">
                <Button
                  size="small"
                  icon={<PlayCircleOutlined />}
                  loading={runningIds.has(record.id)}
                  onClick={() => handleRunNow(record.id)}
                />
              </Tooltip>
            )}
            <Tooltip title={record.paused ? 'Resume' : 'Pause'}>
              <Button
                size="small"
                icon={record.paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />}
                onClick={() => handleTogglePause(record)}
              />
            </Tooltip>
            {needsRenewal && (
              <Tooltip title="Renew schedule">
                <Button
                  size="small"
                  type="primary"
                  icon={<ReloadOutlined />}
                  onClick={() => handleRenew(record.id)}
                >
                  Renew
                </Button>
              </Tooltip>
            )}
            <Tooltip title="Edit">
              <Button
                size="small"
                icon={<EditOutlined />}
                onClick={() => setEditingSchedule({
                  id: record.savedQueryId,
                  name: q?.name || 'Query',
                  sql: q?.sql,
                })}
              />
            </Tooltip>
            <Popconfirm
              title="Delete this schedule?"
              onConfirm={() => handleDelete(record.id)}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Tooltip title="Delete">
                <Button size="small" danger icon={<DeleteOutlined />} />
              </Tooltip>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];

  return (
    <div style={{ padding: 24 }}>
      {/* Expiration warnings */}
      {expiringSchedules.length > 0 && (
        <Alert
          type="warning"
          showIcon
          icon={<WarningOutlined />}
          style={{ marginBottom: 16 }}
          message={`${expiringSchedules.length} scheduled ${expiringSchedules.length === 1 ? 'query is' : 'queries are'} expiring soon`}
          description={
            <Space direction="vertical" size={4}>
              {expiringSchedules.map((s) => {
                const q = queryMap[s.savedQueryId];
                const days = daysUntilExpiry(s.expiresAt);
                return (
                  <Space key={s.id}>
                    <Text>{q?.name || s.savedQueryId}</Text>
                    <Tag color="warning">{days}d left</Tag>
                    <Button size="small" type="link" onClick={() => handleRenew(s.id)}>
                      Renew now
                    </Button>
                  </Space>
                );
              })}
            </Space>
          }
        />
      )}

      {/* Alert warnings */}
      {schedulesWithAlerts.length > 0 && (
        <Alert
          type="info"
          showIcon
          icon={<WarningOutlined />}
          style={{ marginBottom: 16 }}
          message={`${schedulesWithAlerts.length} scheduled ${schedulesWithAlerts.length === 1 ? 'query has' : 'queries have'} triggered alerts`}
          description={
            <Space direction="vertical" size={4}>
              {schedulesWithAlerts.map((s) => {
                const q = queryMap[s.savedQueryId];
                const latestSnap = (allSnapshots?.[s.id] || [])[0];
                const alertCount = latestSnap?.triggeredAlerts?.length || 0;
                return (
                  <Space key={s.id}>
                    <Text>{q?.name || s.savedQueryId}</Text>
                    <Tag color="red">{alertCount} alert{alertCount !== 1 ? 's' : ''}</Tag>
                  </Space>
                );
              })}
            </Space>
          }
        />
      )}

      <Card>
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* Header */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Title level={3} style={{ margin: 0 }}>
              <ClockCircleOutlined style={{ marginRight: 8 }} />
              Workflows
            </Title>
            <Space>
              <Tooltip title="Workflow settings">
                <Button
                  icon={<SettingOutlined />}
                  onClick={() => setShowConfig(!showConfig)}
                >
                  Settings
                </Button>
              </Tooltip>
            </Space>
          </div>

          {/* Settings panel */}
          {showConfig && (
            <Card size="small" title="Workflow Expiration Settings" extra={<Tag>Admin</Tag>}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="Auto-expire scheduled queries">
                    <Switch
                      checked={config.expirationEnabled}
                      onChange={(checked) => setConfigDraft({ ...config, expirationEnabled: checked })}
                    />
                  </Descriptions.Item>
                  {config.expirationEnabled && (
                    <>
                      <Descriptions.Item label="Expiration period (days)">
                        <InputNumber
                          min={7}
                          max={365}
                          value={config.expirationDays}
                          onChange={(val) => setConfigDraft({ ...config, expirationDays: val || 90 })}
                          style={{ width: 100 }}
                        />
                      </Descriptions.Item>
                      <Descriptions.Item label="Warning before expiry (days)">
                        <InputNumber
                          min={1}
                          max={30}
                          value={config.warningDaysBeforeExpiry}
                          onChange={(val) => setConfigDraft({ ...config, warningDaysBeforeExpiry: val || 14 })}
                          style={{ width: 100 }}
                        />
                      </Descriptions.Item>
                    </>
                  )}
                </Descriptions>
                <Button type="primary" size="small" onClick={handleSaveConfig}>
                  Save Settings
                </Button>
              </Space>
            </Card>
          )}

          {/* Search */}
          <Input
            placeholder="Search workflows by query name, description, or frequency..."
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
          ) : filteredSchedules.length === 0 ? (
            <Empty
              description={
                searchText
                  ? 'No workflows match your search'
                  : 'No scheduled queries yet. Schedule a saved query to create a workflow.'
              }
            />
          ) : (
            <Table
              dataSource={filteredSchedules}
              columns={columns}
              rowKey="id"
              pagination={{
                pageSize: 15,
                showSizeChanger: true,
                showTotal: (total) => `${total} workflows`,
              }}
              expandable={{
                expandedRowRender: (record) => {
                  const snaps = allSnapshots?.[record.id] || [];
                  const latest = snaps[0];
                  if (!latest) {
                    return <Empty description="No runs yet" />;
                  }
                  return (
                    <Space direction="vertical" style={{ width: '100%' }} size="small">
                      {latest.aiSummary && (
                        <Card size="small" title="AI Summary">
                          <Markdown rehypePlugins={[rehypeRaw]}>{latest.aiSummary}</Markdown>
                        </Card>
                      )}
                      {latest.triggeredAlerts && latest.triggeredAlerts.length > 0 && (
                        <div>
                          <Text strong>Alerts: </Text>
                          <Space size={4} wrap>
                            {latest.triggeredAlerts.map((alert, idx) => (
                              <Tag color="red" key={idx}>{alert.message}</Tag>
                            ))}
                          </Space>
                        </div>
                      )}
                      {latest.resultPath && (
                        <div>
                          <Text strong>Results: </Text>
                          <Text copyable code>SELECT * FROM dfs.`{latest.resultPath}`</Text>
                        </div>
                      )}
                      {latest.rowCountDelta != null && (
                        <div>
                          <Text strong>Row count change: </Text>
                          <Badge
                            count={latest.rowCountDelta > 0 ? `+${latest.rowCountDelta}` : String(latest.rowCountDelta)}
                            style={{ backgroundColor: latest.rowCountDelta > 0 ? '#52c41a' : latest.rowCountDelta < 0 ? '#ff4d4f' : '#d9d9d9' }}
                          />
                          {latest.previousRowCount != null && (
                            <Text type="secondary" style={{ marginLeft: 8 }}>
                              (prev: {latest.previousRowCount}, now: {latest.rowCount ?? '?'})
                            </Text>
                          )}
                        </div>
                      )}
                      {latest.previewRows && latest.previewColumns && latest.previewRows.length > 0 && (
                        <Card size="small" title="Preview">
                          <Table
                            size="small"
                            dataSource={latest.previewRows.map((row, idx) => ({ ...row, _key: idx }))}
                            columns={latest.previewColumns.map((col) => ({
                              title: col,
                              dataIndex: col,
                              key: col,
                              ellipsis: true,
                            }))}
                            rowKey="_key"
                            pagination={false}
                            scroll={{ x: true }}
                          />
                        </Card>
                      )}
                    </Space>
                  );
                },
              }}
            />
          )}
        </Space>
      </Card>

      {/* Schedule Edit Modal */}
      {editingSchedule && (
        <ScheduleModal
          open={!!editingSchedule}
          onClose={() => setEditingSchedule(null)}
          savedQueryId={editingSchedule.id}
          savedQueryName={editingSchedule.name}
          savedQuerySql={editingSchedule.sql}
          onSuccess={() => {
            queryClient.invalidateQueries({ queryKey: ['schedules'] });
            setEditingSchedule(null);
          }}
        />
      )}
    </div>
  );
}
