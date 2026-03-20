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
import { useState, useEffect, useMemo } from 'react';
import {
  Modal,
  Form,
  Select,
  Switch,
  Button,
  Space,
  Typography,
  message,
  Popconfirm,
  Tabs,
  InputNumber,
  Input,
  Table,
  Tag,
  Empty,
  TimePicker,
  Descriptions,
  Collapse,
} from 'antd';
import {
  ClockCircleOutlined,
  DeleteOutlined,
  MailOutlined,
  CodeOutlined,
  HistoryOutlined,
  SettingOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  MinusCircleOutlined,
  DatabaseOutlined,
  RobotOutlined,
  AlertOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import {
  getScheduleForQuery,
  getSnapshots,
  createSchedule,
  updateSchedule,
  deleteSchedule,
  runScheduleNow,
} from '../../api/schedules';
import type {
  ScheduleFrequency,
  DayOfWeek,
  QuerySchedule,
  QuerySnapshot,
  AlertRule,
  TriggeredAlert,
} from '../../types';

const { Text } = Typography;

interface ScheduleModalProps {
  open: boolean;
  onClose: () => void;
  savedQueryId: string;
  savedQueryName: string;
  savedQuerySql?: string;
  onSuccess?: () => void;
}

const FREQUENCY_OPTIONS: { value: ScheduleFrequency; label: string; description: string }[] = [
  { value: 'hourly', label: 'Hourly', description: 'Runs every hour at the specified minute' },
  { value: 'daily', label: 'Daily', description: 'Runs once per day at the specified time' },
  { value: 'weekly', label: 'Weekly', description: 'Runs once per week on the selected day' },
  { value: 'monthly', label: 'Monthly', description: 'Runs once per month on the selected day' },
];

const DAY_OPTIONS: { value: DayOfWeek; label: string }[] = [
  { value: 0, label: 'Sunday' },
  { value: 1, label: 'Monday' },
  { value: 2, label: 'Tuesday' },
  { value: 3, label: 'Wednesday' },
  { value: 4, label: 'Thursday' },
  { value: 5, label: 'Friday' },
  { value: 6, label: 'Saturday' },
];

const ALERT_TYPE_OPTIONS: { value: AlertRule['type']; label: string; needsThreshold: boolean }[] = [
  { value: 'rowCountAbove', label: 'Row Count Above', needsThreshold: true },
  { value: 'rowCountBelow', label: 'Row Count Below', needsThreshold: true },
  { value: 'queryFailed', label: 'Query Failed', needsThreshold: false },
  { value: 'durationAbove', label: 'Duration Above (seconds)', needsThreshold: true },
  { value: 'durationExceedsInterval', label: 'Duration Exceeds Interval', needsThreshold: false },
];

const DEFAULT_AI_PROMPT =
  'Provide a brief executive summary of these query results. Highlight any anomalies, key metrics, and trends. Keep it concise — no more than 3-5 bullet points.';

function formatNextRun(isoString?: string): string {
  if (!isoString) {
    return 'Not scheduled';
  }
  const d = new Date(isoString);
  const now = new Date();
  const diff = d.getTime() - now.getTime();
  const hours = Math.floor(diff / 3600000);
  const mins = Math.floor((diff % 3600000) / 60000);
  const relative = hours > 24
    ? `in ${Math.floor(hours / 24)} day${Math.floor(hours / 24) > 1 ? 's' : ''}`
    : hours > 0
      ? `in ${hours}h ${mins}m`
      : `in ${mins}m`;
  return `${d.toLocaleString()} (${relative})`;
}

function alertTypeNeedsThreshold(type: AlertRule['type']): boolean {
  return type !== 'queryFailed' && type !== 'durationExceedsInterval';
}

export default function ScheduleModal({
  open,
  onClose,
  savedQueryId,
  savedQueryName,
  savedQuerySql,
  onSuccess,
}: ScheduleModalProps) {
  const [form] = Form.useForm();
  const [existing, setExisting] = useState<QuerySchedule | null>(null);
  const [snapshots, setSnapshots] = useState<QuerySnapshot[]>([]);
  const [loading, setLoading] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [fetching, setFetching] = useState(false);
  const [runningNow, setRunningNow] = useState(false);
  const [activeTab, setActiveTab] = useState('schedule');
  const [alertRules, setAlertRules] = useState<AlertRule[]>([]);

  const frequency: ScheduleFrequency = Form.useWatch('frequency', form) || 'daily';
  const enabled: boolean = Form.useWatch('enabled', form) ?? false;
  const persistResults: boolean = Form.useWatch('persistResults', form) ?? false;
  const aiSummaryEnabled: boolean = Form.useWatch('aiSummaryEnabled', form) ?? false;
  const refreshMode: string = Form.useWatch('refreshMode', form) || 'query';

  const defaultFormValues = {
    description: '',
    frequency: 'daily' as ScheduleFrequency,
    enabled: false,
    paused: false,
    timeOfDay: dayjs('08:00', 'HH:mm'),
    dayOfWeek: 1 as DayOfWeek,
    dayOfMonth: 1,
    notifyOnSuccess: false,
    notifyOnFailure: true,
    notifyEmails: '',
    retentionCount: 30,
    persistResults: false,
    resultLocation: 'dfs.tmp',
    resultFormat: 'parquet',
    resultMode: 'overwrite',
    aiSummaryEnabled: false,
    aiSummaryPrompt: DEFAULT_AI_PROMPT,
    aiSummaryMaxRows: 100,
    refreshMode: 'query',
    materializedViewName: '',
    timeoutSeconds: 300,
  };

  // Load existing schedule and snapshots when modal opens
  useEffect(() => {
    if (open && savedQueryId) {
      setFetching(true);
      setActiveTab('schedule');
      Promise.all([
        getScheduleForQuery(savedQueryId),
        getScheduleForQuery(savedQueryId).then((s) =>
          s ? getSnapshots(s.id) : []
        ),
      ])
        .then(([schedule, snaps]) => {
          setExisting(schedule);
          setSnapshots(snaps);
          if (schedule) {
            setAlertRules(schedule.alertRules || []);
            form.setFieldsValue({
              description: schedule.description || '',
              frequency: schedule.frequency,
              enabled: schedule.enabled,
              paused: schedule.paused ?? false,
              timeOfDay: dayjs(schedule.timeOfDay, 'HH:mm'),
              dayOfWeek: schedule.dayOfWeek,
              dayOfMonth: schedule.dayOfMonth,
              notifyOnSuccess: schedule.notifyOnSuccess,
              notifyOnFailure: schedule.notifyOnFailure,
              notifyEmails: schedule.notifyEmails?.join(', ') || '',
              retentionCount: schedule.retentionCount,
              persistResults: schedule.persistResults ?? false,
              resultLocation: schedule.resultLocation || 'dfs.tmp',
              resultFormat: schedule.resultFormat || 'parquet',
              resultMode: schedule.resultMode || 'overwrite',
              aiSummaryEnabled: schedule.aiSummaryEnabled ?? false,
              aiSummaryPrompt: schedule.aiSummaryPrompt || DEFAULT_AI_PROMPT,
              aiSummaryMaxRows: schedule.aiSummaryMaxRows ?? 100,
              refreshMode: schedule.refreshMode || 'query',
              materializedViewName: schedule.materializedViewName || '',
              timeoutSeconds: schedule.timeoutSeconds ?? 300,
            });
          } else {
            setAlertRules([]);
            form.setFieldsValue(defaultFormValues);
          }
        })
        .catch(() => {
          message.error('Failed to load schedule');
        })
        .finally(() => {
          setFetching(false);
        });
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, savedQueryId, form]);

  const cronExpression = useMemo(() => {
    const time = form.getFieldValue('timeOfDay');
    const h = time ? dayjs(time).hour() : 8;
    const m = time ? dayjs(time).minute() : 0;
    switch (frequency) {
      case 'hourly':
        return `${m} * * * *`;
      case 'daily':
        return `${m} ${h} * * *`;
      case 'weekly': {
        const dow = form.getFieldValue('dayOfWeek') ?? 1;
        return `${m} ${h} * * ${dow}`;
      }
      case 'monthly': {
        const dom = form.getFieldValue('dayOfMonth') ?? 1;
        return `${m} ${h} ${dom} * *`;
      }
      default:
        return '';
    }
  }, [frequency, form]);

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);
      const timeOfDay = values.timeOfDay ? dayjs(values.timeOfDay).format('HH:mm') : '08:00';
      const emails = values.notifyEmails
        ? values.notifyEmails.split(',').map((e: string) => e.trim()).filter(Boolean)
        : [];
      const data = {
        savedQueryId,
        description: values.description || undefined,
        frequency: values.frequency,
        enabled: values.enabled,
        paused: values.paused,
        timeOfDay,
        dayOfWeek: values.frequency === 'weekly' ? values.dayOfWeek : undefined,
        dayOfMonth: values.frequency === 'monthly' ? values.dayOfMonth : undefined,
        notifyOnSuccess: values.notifyOnSuccess,
        notifyOnFailure: values.notifyOnFailure,
        notifyEmails: emails,
        retentionCount: values.retentionCount,
        persistResults: values.persistResults,
        resultLocation: values.persistResults ? values.resultLocation : undefined,
        resultFormat: values.persistResults ? values.resultFormat : undefined,
        resultMode: values.persistResults ? values.resultMode : undefined,
        aiSummaryEnabled: values.aiSummaryEnabled,
        aiSummaryPrompt: values.aiSummaryEnabled ? values.aiSummaryPrompt : undefined,
        aiSummaryMaxRows: values.aiSummaryEnabled ? values.aiSummaryMaxRows : undefined,
        alertRules,
        refreshMode: values.refreshMode,
        materializedViewName: values.refreshMode === 'materializedView'
          ? values.materializedViewName
          : undefined,
        timeoutSeconds: values.timeoutSeconds,
      };
      if (existing) {
        await updateSchedule(existing.id, data);
        message.success('Schedule updated');
      } else {
        await createSchedule(data);
        message.success('Schedule created');
      }
      onSuccess?.();
      onClose();
    } catch {
      // validation or API error
    } finally {
      setLoading(false);
    }
  };

  const handleDelete = async () => {
    if (!existing) {
      return;
    }
    setDeleting(true);
    try {
      await deleteSchedule(existing.id);
      message.success('Schedule deleted');
      setExisting(null);
      onSuccess?.();
      onClose();
    } catch {
      message.error('Failed to delete schedule');
    } finally {
      setDeleting(false);
    }
  };

  const handleRunNow = async () => {
    if (!existing) {
      return;
    }
    setRunningNow(true);
    try {
      await runScheduleNow(existing.id);
      message.success('Schedule run triggered');
      // Refresh snapshots
      const snaps = await getSnapshots(existing.id);
      setSnapshots(snaps);
    } catch {
      message.error('Failed to trigger run');
    } finally {
      setRunningNow(false);
    }
  };

  // Alert rules management
  const addAlertRule = () => {
    const newRule: AlertRule = {
      id: `rule-${Date.now()}`,
      type: 'rowCountAbove',
      threshold: 1000,
      enabled: true,
    };
    setAlertRules([...alertRules, newRule]);
  };

  const removeAlertRule = (ruleId: string) => {
    setAlertRules(alertRules.filter((r) => r.id !== ruleId));
  };

  const updateAlertRule = (ruleId: string, updates: Partial<AlertRule>) => {
    setAlertRules(
      alertRules.map((r) => (r.id === ruleId ? { ...r, ...updates } : r))
    );
  };

  // Snapshot columns with expanded features
  const snapshotColumns = [
    {
      title: 'Executed',
      dataIndex: 'executedAt',
      key: 'executedAt',
      render: (val: string) => new Date(val).toLocaleString(),
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      render: (val: string) => (
        <Tag color={val === 'success' ? 'green' : val === 'skipped' ? 'default' : 'red'}>
          {val}
        </Tag>
      ),
    },
    {
      title: 'Rows',
      dataIndex: 'rowCount',
      key: 'rowCount',
      render: (val?: number) => val !== undefined ? val.toLocaleString() : '-',
    },
    {
      title: 'Duration',
      dataIndex: 'duration',
      key: 'duration',
      render: (val?: number) => val !== undefined ? `${(val / 1000).toFixed(1)}s` : '-',
    },
    {
      title: 'Result Path',
      dataIndex: 'resultPath',
      key: 'resultPath',
      render: (val?: string) => val ? (
        <a href={`#${val}`} title={val}>
          <Text ellipsis style={{ maxWidth: 120 }}>{val}</Text>
        </a>
      ) : '-',
    },
    {
      title: 'Alerts',
      dataIndex: 'triggeredAlerts',
      key: 'triggeredAlerts',
      render: (alerts?: TriggeredAlert[]) =>
        alerts && alerts.length > 0
          ? alerts.map((a, i) => (
            <Tag color="orange" key={i}>{a.ruleType}</Tag>
          ))
          : '-',
    },
    {
      title: 'Error',
      dataIndex: 'errorMessage',
      key: 'errorMessage',
      render: (val?: string) =>
        val ? <Text type="danger" ellipsis style={{ maxWidth: 150 }}>{val}</Text> : '-',
    },
  ];

  const expandedRowRender = (record: QuerySnapshot) => {
    const items = [];

    if (record.aiSummary) {
      items.push({
        key: 'aiSummary',
        label: 'AI Summary',
        children: (
          <div style={{ whiteSpace: 'pre-wrap', fontSize: 13 }}>{record.aiSummary}</div>
        ),
      });
    }

    if (record.previewRows && record.previewRows.length > 0 && record.previewColumns) {
      const previewCols = record.previewColumns.map((col) => ({
        title: col,
        dataIndex: col,
        key: col,
        ellipsis: true,
      }));
      items.push({
        key: 'previewRows',
        label: `Preview (${record.previewRows.length} rows)`,
        children: (
          <Table
            dataSource={record.previewRows.map((row, idx) => ({ ...row, _key: idx }))}
            columns={previewCols}
            rowKey="_key"
            size="small"
            pagination={false}
            scroll={{ x: 'max-content' }}
            style={{ maxHeight: 200, overflow: 'auto' }}
          />
        ),
      });
    }

    if (items.length === 0) {
      return <Text type="secondary">No additional details available.</Text>;
    }

    return <Collapse items={items} size="small" />;
  };

  const tabItems = [
    {
      key: 'schedule',
      label: <span><SettingOutlined /> Schedule</span>,
      children: (
        <Form form={form} layout="vertical" disabled={fetching}>
          <Form.Item name="description" label="Description">
            <Input.TextArea rows={2} placeholder="What does this scheduled query do?" />
          </Form.Item>

          <Form.Item
            name="frequency"
            label="Frequency"
            rules={[{ required: true, message: 'Please select a frequency' }]}
          >
            <Select
              options={FREQUENCY_OPTIONS.map((o) => ({
                value: o.value,
                label: (
                  <span>
                    {o.label}
                    <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>{o.description}</Text>
                  </span>
                ),
              }))}
            />
          </Form.Item>

          <Form.Item
            name="timeOfDay"
            label={frequency === 'hourly' ? 'Minute of Hour' : 'Time of Day'}
          >
            <TimePicker
              format={frequency === 'hourly' ? 'mm' : 'HH:mm'}
              minuteStep={5}
              showNow={false}
              style={{ width: '100%' }}
            />
          </Form.Item>

          {frequency === 'weekly' && (
            <Form.Item name="dayOfWeek" label="Day of Week">
              <Select options={DAY_OPTIONS} />
            </Form.Item>
          )}

          {frequency === 'monthly' && (
            <Form.Item name="dayOfMonth" label="Day of Month">
              <InputNumber min={1} max={28} style={{ width: '100%' }} />
            </Form.Item>
          )}

          <div style={{ display: 'flex', gap: 24 }}>
            <Form.Item name="enabled" label="Enabled" valuePropName="checked">
              <Switch />
            </Form.Item>
            <Form.Item
              name="paused"
              label="Paused"
              valuePropName="checked"
              tooltip="Temporarily pause the schedule without disabling it"
            >
              <Switch />
            </Form.Item>
          </div>

          <Form.Item
            name="timeoutSeconds"
            label="Query Timeout (seconds)"
            extra="Maximum time allowed for the scheduled query to run"
          >
            <InputNumber min={10} max={86400} style={{ width: '100%' }} />
          </Form.Item>

          {/* Summary */}
          <Descriptions size="small" column={1} bordered style={{ marginTop: 8 }}>
            <Descriptions.Item label="Cron Expression">
              <code>{cronExpression}</code>
            </Descriptions.Item>
            {enabled && existing?.nextRunAt && (
              <Descriptions.Item label="Next Run">
                {formatNextRun(existing.nextRunAt)}
              </Descriptions.Item>
            )}
            {existing?.lastRunAt && (
              <Descriptions.Item label="Last Run">
                {new Date(existing.lastRunAt).toLocaleString()}
              </Descriptions.Item>
            )}
          </Descriptions>
        </Form>
      ),
    },
    {
      key: 'results',
      label: <span><DatabaseOutlined /> Results</span>,
      children: (
        <Form form={form} layout="vertical" disabled={fetching}>
          <Form.Item
            name="persistResults"
            label="Persist Results"
            valuePropName="checked"
            extra="Save query results to a storage location after each run"
          >
            <Switch />
          </Form.Item>

          {persistResults && (
            <>
              <Form.Item
                name="resultLocation"
                label="Result Location"
                rules={[{ required: persistResults, message: 'Please specify a result location' }]}
              >
                <Input placeholder="e.g. dfs.tmp, dfs.data" />
              </Form.Item>

              <Form.Item name="resultFormat" label="Result Format">
                <Select
                  options={[
                    { value: 'parquet', label: 'Parquet' },
                    { value: 'csv', label: 'CSV' },
                    { value: 'json', label: 'JSON' },
                  ]}
                />
              </Form.Item>

              <Form.Item name="resultMode" label="Result Mode">
                <Select
                  options={[
                    { value: 'overwrite', label: 'Overwrite' },
                    { value: 'append', label: 'Append' },
                    { value: 'newTablePerRun', label: 'New Table Per Run' },
                  ]}
                />
              </Form.Item>
            </>
          )}

          <Form.Item
            name="refreshMode"
            label="Refresh Mode"
            extra="Choose how the query executes: normal query or as a materialized view"
          >
            <Select
              options={[
                { value: 'query', label: 'Normal Query' },
                { value: 'materializedView', label: 'Materialized View' },
              ]}
            />
          </Form.Item>

          {refreshMode === 'materializedView' && (
            <Form.Item
              name="materializedViewName"
              label="Materialized View Name"
              rules={[{ required: true, message: 'Please specify a view name' }]}
            >
              <Input placeholder="e.g. my_materialized_view" />
            </Form.Item>
          )}
        </Form>
      ),
    },
    {
      key: 'aiSummary',
      label: <span><RobotOutlined /> AI Summary</span>,
      children: (
        <Form form={form} layout="vertical" disabled={fetching}>
          <Form.Item
            name="aiSummaryEnabled"
            label="Enable AI Summary"
            valuePropName="checked"
          >
            <Switch />
          </Form.Item>

          <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
            When enabled, results are sent to Prospector AI for summarization after each run.
            The summary is stored and optionally emailed.
          </Text>

          {aiSummaryEnabled && (
            <>
              <Form.Item
                name="aiSummaryPrompt"
                label="Summary Prompt"
                extra="Customize the prompt sent to the AI for summarization"
              >
                <Input.TextArea rows={4} />
              </Form.Item>

              <Form.Item
                name="aiSummaryMaxRows"
                label="Max Rows to Summarize"
                extra="Limit the number of result rows sent to the AI (1 - 10,000)"
              >
                <InputNumber min={1} max={10000} style={{ width: '100%' }} />
              </Form.Item>
            </>
          )}
        </Form>
      ),
    },
    {
      key: 'alerts',
      label: <span><AlertOutlined /> Alerts ({alertRules.length})</span>,
      children: (
        <div>
          <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
            Configure rules that trigger alerts based on query results. Alerts appear in the
            run history and are included in notification emails.
          </Text>

          {alertRules.map((rule) => (
            <div
              key={rule.id}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                marginBottom: 12,
                padding: 12,
                border: '1px solid var(--color-border)',
                borderRadius: 6,
                background: 'var(--color-bg-elevated)',
              }}
            >
              <Select
                value={rule.type}
                onChange={(val) => {
                  const updates: Partial<AlertRule> = { type: val };
                  if (!alertTypeNeedsThreshold(val)) {
                    updates.threshold = undefined;
                  } else if (rule.threshold === undefined) {
                    updates.threshold = 1000;
                  }
                  updateAlertRule(rule.id, updates);
                }}
                style={{ width: 220 }}
                options={ALERT_TYPE_OPTIONS.map((o) => ({
                  value: o.value,
                  label: o.label,
                }))}
              />
              {alertTypeNeedsThreshold(rule.type) && (
                <InputNumber
                  value={rule.threshold}
                  onChange={(val) => updateAlertRule(rule.id, { threshold: val ?? undefined })}
                  placeholder="Threshold"
                  min={0}
                  style={{ width: 120 }}
                />
              )}
              <Switch
                checked={rule.enabled}
                onChange={(val) => updateAlertRule(rule.id, { enabled: val })}
                checkedChildren="On"
                unCheckedChildren="Off"
                size="small"
              />
              <Button
                type="text"
                danger
                icon={<MinusCircleOutlined />}
                onClick={() => removeAlertRule(rule.id)}
                size="small"
              />
            </div>
          ))}

          <Button
            type="dashed"
            onClick={addAlertRule}
            icon={<PlusOutlined />}
            style={{ width: '100%', marginTop: 8 }}
          >
            Add Alert Rule
          </Button>
        </div>
      ),
    },
    {
      key: 'notifications',
      label: <span><MailOutlined /> Notifications</span>,
      children: (
        <Form form={form} layout="vertical" disabled={fetching}>
          <Form.Item name="notifyOnFailure" label="Notify on Failure" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item name="notifyOnSuccess" label="Notify on Success" valuePropName="checked">
            <Switch />
          </Form.Item>
          <Form.Item
            name="notifyEmails"
            label="Email Recipients"
            extra="Comma-separated email addresses"
          >
            <Input placeholder="user@example.com, team@example.com" />
          </Form.Item>
          <Form.Item
            name="retentionCount"
            label="Snapshot Retention"
            extra="Number of past run results to keep"
          >
            <InputNumber min={1} max={365} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      ),
    },
    {
      key: 'history',
      label: <span><HistoryOutlined /> Run History ({snapshots.length})</span>,
      children: snapshots.length > 0 ? (
        <Table
          dataSource={snapshots}
          columns={snapshotColumns}
          rowKey="id"
          size="small"
          pagination={{ pageSize: 5 }}
          expandable={{
            expandedRowRender,
            rowExpandable: (record) =>
              !!(record.aiSummary || (record.previewRows && record.previewRows.length > 0)),
          }}
          scroll={{ x: 'max-content' }}
        />
      ) : (
        <Empty description="No run history yet. Scheduled runs will appear here." />
      ),
    },
    {
      key: 'sql',
      label: <span><CodeOutlined /> SQL Preview</span>,
      children: savedQuerySql ? (
        <pre style={{
          background: 'var(--color-bg-elevated)',
          padding: 16,
          borderRadius: 6,
          maxHeight: 300,
          overflow: 'auto',
          fontFamily: 'monospace',
          fontSize: 13,
          border: '1px solid var(--color-border)',
        }}>
          {savedQuerySql}
        </pre>
      ) : (
        <Empty description="SQL not available" />
      ),
    },
  ];

  return (
    <Modal
      title={
        <Space>
          <ClockCircleOutlined />
          <span>Schedule: {savedQueryName}</span>
          {existing?.enabled && <Tag color="green">Active</Tag>}
          {existing?.paused && <Tag color="orange">Paused</Tag>}
        </Space>
      }
      open={open}
      onCancel={onClose}
      width={720}
      footer={
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <div>
            {existing && (
              <Space>
                <Popconfirm
                  title="Delete this schedule?"
                  description="The query will no longer run on a schedule."
                  onConfirm={handleDelete}
                  okText="Delete"
                  cancelText="Cancel"
                  okButtonProps={{ danger: true }}
                >
                  <Button danger icon={<DeleteOutlined />} loading={deleting}>
                    Delete
                  </Button>
                </Popconfirm>
                <Button
                  icon={<PlayCircleOutlined />}
                  onClick={handleRunNow}
                  loading={runningNow}
                >
                  Run Now
                </Button>
              </Space>
            )}
          </div>
          <Space>
            <Button onClick={onClose}>Cancel</Button>
            <Button type="primary" onClick={handleSave} loading={loading}>
              {existing ? 'Update' : 'Create'} Schedule
            </Button>
          </Space>
        </div>
      }
    >
      <Tabs activeKey={activeTab} onChange={setActiveTab} items={tabItems} />
    </Modal>
  );
}
