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
import { Link, useNavigate } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert, Button, Card, Col, InputNumber, Modal, Row, Select, Space, Statistic, Switch, Table, Tag, Typography, message,
} from 'antd';
import { PlusOutlined, SettingOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { Workflow, WorkflowConfig, WorkflowSchedule } from '../types/workflow';
import {
  assignProject, getObservabilityStatus, getObservabilitySummary, getRunningWorkflows, getWorkflowConfig,
  listWorkflows, renewWorkflow, setupObservability, updateWorkflowConfig,
} from '../api/workflows';
import { getErrorMessage } from '../api/client';
import { getProjects } from '../api/projects';
import { useCurrentUser } from '../hooks/useCurrentUser';

const DAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

export function describeSchedule(s: WorkflowSchedule): string {
  switch (s.frequency) {
    case 'hourly': return `Hourly at :${s.timeOfDay.slice(3)}`;
    case 'weekly': return `${DAYS[s.dayOfWeek ?? 1]} ${s.timeOfDay}`;
    case 'monthly': return `Day ${s.dayOfMonth ?? 1} ${s.timeOfDay}`;
    default: return `Daily ${s.timeOfDay}`;
  }
}

export default function WorkflowsPage({ projectId }: { projectId?: string }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { user } = useCurrentUser();
  const isAdmin = user ? !user.authEnabled || user.isAdmin : false;
  const adminView = !projectId;
  const [settingsOpen, setSettingsOpen] = useState(false);

  const { data: workflows = [], isLoading } = useQuery({
    queryKey: ['workflows', projectId ?? 'all'], queryFn: () => listWorkflows(projectId), enabled: !adminView || isAdmin,
  });
  const { data: summary } = useQuery({
    queryKey: ['workflow-summary', projectId ?? 'all'], queryFn: () => getObservabilitySummary(projectId),
    enabled: !adminView || isAdmin,
  });
  const { data: config } = useQuery({ queryKey: ['workflow-config'], queryFn: getWorkflowConfig });
  const { data: projects = [] } = useQuery({ queryKey: ['projects'], queryFn: getProjects, enabled: adminView && isAdmin });
  const { data: running = [] } = useQuery({
    queryKey: ['workflow-running'], queryFn: getRunningWorkflows, enabled: adminView && isAdmin, refetchInterval: 15_000,
  });
  const { data: obsStatus } = useQuery({
    queryKey: ['workflow-obs-status'], queryFn: getObservabilityStatus, enabled: adminView && isAdmin,
  });

  const refresh = () => queryClient.invalidateQueries({ queryKey: ['workflows'] });
  const renew = useMutation({
    mutationFn: renewWorkflow, onSuccess: refresh, onError: (e) => message.error(getErrorMessage(e)),
  });
  const assign = useMutation({
    mutationFn: ({ id, pid }: { id: string; pid: string }) => assignProject(id, pid),
    onSuccess: refresh, onError: (e) => message.error(getErrorMessage(e)),
  });
  const setup = useMutation({
    mutationFn: setupObservability,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['workflow-obs-status'] }),
  });

  const stats = useMemo(() => {
    const byId = new Map((summary?.rows ?? []).map((r) => [r.workflowId, r]));
    const warnBefore = dayjs().add(config?.warningDaysBeforeExpiry ?? 14, 'day');
    const expiring = (w: Workflow) => !!w.schedule.expiresAt && dayjs(w.schedule.expiresAt).isBefore(warnBefore);
    const failing = (w: Workflow) => (w.consecutiveFailures ?? 0) >= 2;
    const rate = (w: Workflow) => {
      const r = byId.get(w.id!);
      return r && Number(r.runs) > 0 ? Math.round((100 * Number(r.successes)) / Number(r.runs)) : null;
    };
    const sorted = [...workflows].sort((a, b) =>
      Number(failing(b)) - Number(failing(a)) || Number(expiring(b)) - Number(expiring(a)) || a.name.localeCompare(b.name));
    return {
      sorted, expiring, failing, rate,
      active: workflows.filter((w) => w.schedule.enabled).length,
      failingCount: workflows.filter(failing).length,
      expiringCount: workflows.filter(expiring).length,
    };
  }, [workflows, summary, config]);

  if (adminView && user && !isAdmin) {
    return <Alert type="warning" showIcon message="Admin access required" style={{ margin: 16 }} />;
  }

  const projectName = (id: string | null) => projects.find((p) => p.id === id)?.name ?? '';
  const editPath = (w: Workflow) => `/projects/${w.projectId}/workflows/${w.id}`;
  const orphans = workflows.filter((w) => !w.projectId);

  return (
    <div style={{ padding: 16 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }} wrap>
        <Typography.Title level={4} style={{ margin: 0 }}>{adminView ? 'All workflows' : 'Workflows'}</Typography.Title>
        <Space>
          {adminView && <Button icon={<SettingOutlined />} onClick={() => setSettingsOpen(true)}>Settings</Button>}
          {!adminView && (
            <Button type="primary" icon={<PlusOutlined />} onClick={() => navigate(`/projects/${projectId}/workflows/new`)}>
              New workflow
            </Button>
          )}
        </Space>
      </Space>

      <Row gutter={12} style={{ marginBottom: 12 }}>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Workflows" value={workflows.length} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Active" value={stats.active} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Failing" value={stats.failingCount}
          valueStyle={stats.failingCount ? { color: '#cf1322' } : undefined} /></Card></Col>
        <Col xs={12} md={6}><Card size="small"><Statistic title="Expiring soon" value={stats.expiringCount} /></Card></Col>
      </Row>

      {adminView && obsStatus && !obsStatus.ready && (
        <Alert type="info" showIcon style={{ marginBottom: 12 }} message="Run history analytics are not set up"
          description={obsStatus.logDir ? 'Creates the dfs.workflow_logs workspace over the Drill log directory.' : 'DRILL_LOG_DIR is not set on this Drillbit.'}
          action={obsStatus.logDir && <Button size="small" loading={setup.isPending} onClick={() => setup.mutate()}>Set up</Button>} />
      )}

      {adminView && running.length > 0 && (
        <Alert type="info" style={{ marginBottom: 12 }} message={`Running now: ${running.map((w) => w.name).join(', ')}`} />
      )}

      <Table<Workflow> rowKey="id" loading={isLoading} dataSource={stats.sorted.filter((w) => w.projectId)} size="middle"
        columns={[
          { title: 'Name', render: (_, w) => <Link to={editPath(w)}>{w.name}</Link> },
          ...(adminView ? [
            { title: 'Project', render: (_: unknown, w: Workflow) => projectName(w.projectId) },
            { title: 'Owner', dataIndex: 'owner' },
          ] : []),
          { title: 'Schedule', render: (_, w) => describeSchedule(w.schedule) },
          { title: 'State', render: (_, w) => (
            <Space size={4} wrap>
              <Tag color={w.schedule.enabled ? 'green' : 'default'}>{w.schedule.enabled ? 'on' : (w.status ?? 'paused').replace(/_/g, ' ')}</Tag>
              {stats.failing(w) && <Tag color="red">{w.consecutiveFailures} failures</Tag>}
              {stats.expiring(w) && <Tag color="orange">expires {dayjs(w.schedule.expiresAt).format('MMM D')}
                <Button type="link" size="small" onClick={() => renew.mutate(w.id!)}>Renew</Button></Tag>}
            </Space>
          ) },
          { title: 'Last run', render: (_, w) => (w.lastRunAt
            ? <Tag color={w.lastStatus === 'error' ? 'red' : 'green'}>{dayjs(w.lastRunAt).format('MMM D HH:mm')}</Tag> : '—') },
          { title: 'Success (30 d)', render: (_, w) => { const r = stats.rate(w); return r == null ? '—' : `${r}%`; } },
          { title: 'Next run', render: (_, w) => (w.schedule.enabled && w.nextRunAt ? dayjs(w.nextRunAt).format('MMM D HH:mm') : '—') },
          { title: 'Actions', render: (_, w) => w.actions.map((a) => <Tag key={a.id}>{a.type}</Tag>) },
        ]} />

      {adminView && orphans.length > 0 && (
        <Card size="small" title="Workflows without a project" style={{ marginTop: 16 }}>
          <Typography.Paragraph type="secondary">
            Migrated schedules whose query is not in any project. They stay disabled until assigned.
          </Typography.Paragraph>
          <Table<Workflow> rowKey="id" size="small" pagination={false} dataSource={orphans} columns={[
            { title: 'Name', dataIndex: 'name' },
            { title: 'Owner', dataIndex: 'owner' },
            { title: 'Assign to', render: (_, w) => (
              <Select style={{ width: 240 }} placeholder="Choose project"
                options={projects.filter((p) => p.savedQueryIds.includes(w.savedQueryId)).map((p) => ({ value: p.id, label: p.name }))}
                notFoundContent="Add the saved query to a project first"
                onChange={(pid: string) => assign.mutate({ id: w.id!, pid })} />
            ) },
          ]} />
        </Card>
      )}

      {config && <SettingsModal open={settingsOpen} config={config} onClose={() => setSettingsOpen(false)} />}
    </div>
  );
}

function SettingsModal({ open, config, onClose }: { open: boolean; config: WorkflowConfig; onClose: () => void }) {
  const queryClient = useQueryClient();
  const [cfg, setCfg] = useState(config);
  const save = useMutation({
    mutationFn: () => updateWorkflowConfig(cfg),
    onSuccess: () => { queryClient.invalidateQueries({ queryKey: ['workflow-config'] }); onClose(); },
  });
  return (
    <Modal open={open} title="Workflow settings" onCancel={onClose} onOk={() => save.mutate()} confirmLoading={save.isPending}>
      <Space direction="vertical">
        <Space><Switch checked={cfg.expirationEnabled} onChange={(v) => setCfg({ ...cfg, expirationEnabled: v })} /> Workflows expire</Space>
        <Space>Expire after <InputNumber min={1} value={cfg.expirationDays} onChange={(v) => setCfg({ ...cfg, expirationDays: v ?? 90 })} /> days</Space>
        <Space>Warn <InputNumber min={0} value={cfg.warningDaysBeforeExpiry} onChange={(v) => setCfg({ ...cfg, warningDaysBeforeExpiry: v ?? 14 })} /> days before</Space>
        <Space>Action timeout <InputNumber min={1} value={cfg.actionTimeoutSeconds} onChange={(v) => setCfg({ ...cfg, actionTimeoutSeconds: v ?? 60 })} /> seconds</Space>
      </Space>
    </Modal>
  );
}
