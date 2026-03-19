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
} from 'antd';
import {
  ClockCircleOutlined,
  DeleteOutlined,
  MailOutlined,
  CodeOutlined,
  HistoryOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import {
  getScheduleForQuery,
  getSnapshots,
  createSchedule,
  updateSchedule,
  deleteSchedule,
} from '../../api/schedules';
import type { ScheduleFrequency, DayOfWeek, QuerySchedule, QuerySnapshot } from '../../types';

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
      <Tag color={val === 'success' ? 'green' : 'red'}>{val}</Tag>
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
    title: 'Error',
    dataIndex: 'errorMessage',
    key: 'errorMessage',
    render: (val?: string) => val ? <Text type="danger" ellipsis style={{ maxWidth: 200 }}>{val}</Text> : '-',
  },
];

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
  const [activeTab, setActiveTab] = useState('schedule');

  const frequency: ScheduleFrequency = Form.useWatch('frequency', form) || 'daily';
  const enabled: boolean = Form.useWatch('enabled', form) ?? false;

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
            form.setFieldsValue({
              description: schedule.description || '',
              frequency: schedule.frequency,
              enabled: schedule.enabled,
              timeOfDay: dayjs(schedule.timeOfDay, 'HH:mm'),
              dayOfWeek: schedule.dayOfWeek,
              dayOfMonth: schedule.dayOfMonth,
              notifyOnSuccess: schedule.notifyOnSuccess,
              notifyOnFailure: schedule.notifyOnFailure,
              notifyEmails: schedule.notifyEmails?.join(', ') || '',
              retentionCount: schedule.retentionCount,
            });
          } else {
            form.setFieldsValue({
              description: '',
              frequency: 'daily',
              enabled: false,
              timeOfDay: dayjs('08:00', 'HH:mm'),
              dayOfWeek: 1,
              dayOfMonth: 1,
              notifyOnSuccess: false,
              notifyOnFailure: true,
              notifyEmails: '',
              retentionCount: 30,
            });
          }
        })
        .catch(() => {
          message.error('Failed to load schedule');
        })
        .finally(() => {
          setFetching(false);
        });
    }
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
        timeOfDay,
        dayOfWeek: values.frequency === 'weekly' ? values.dayOfWeek : undefined,
        dayOfMonth: values.frequency === 'monthly' ? values.dayOfMonth : undefined,
        notifyOnSuccess: values.notifyOnSuccess,
        notifyOnFailure: values.notifyOnFailure,
        notifyEmails: emails,
        retentionCount: values.retentionCount,
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

          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
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
        </Space>
      }
      open={open}
      onCancel={onClose}
      width={640}
      footer={
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <div>
            {existing && (
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
