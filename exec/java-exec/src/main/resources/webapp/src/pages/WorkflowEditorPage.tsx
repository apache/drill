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
import { useEffect, useMemo, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Alert, Button, Drawer, Dropdown, Form, Input, InputNumber, Select, Space, Spin, Switch, Table, Tabs, Typography, message,
} from 'antd';
import { PlayCircleOutlined, PlusOutlined, ExperimentOutlined } from '@ant-design/icons';
import type { ActionTypeInfo, DryRunResult, ValidationError, Workflow, WorkflowAction } from '../types/workflow';
import {
  createWorkflow, dryRunWorkflow, getActionTypes, getWorkflow, getWorkflowConfig, renewWorkflow, runWorkflowNow, setWorkflowEnabled,
  takeOwnership, updateWorkflow, validateWorkflow, validationErrors,
} from '../api/workflows';
import { useCurrentUser } from '../hooks/useCurrentUser';
import { getErrorMessage } from '../api/client';
import { getSavedQueries } from '../api/savedQueries';
import { useProjectContext } from '../contexts/ProjectContext';
import { availableRefs, frequencyPatch, isExpired, newWorkflow, unplacedErrors } from '../utils/conditionTree';
import ActionCard from '../components/workflow/ActionCard';
import OutcomeList from '../components/workflow/OutcomeList';
import RunsTab from '../components/workflow/RunsTab';
import HealthTab from '../components/workflow/HealthTab';

const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function defaultsFor(t: ActionTypeInfo): Record<string, unknown> {
  const cfg: Record<string, unknown> = {};
  Object.entries(t.configSchema.properties ?? {}).forEach(([k, p]) => {
    if (p.default !== undefined) cfg[k] = p.default;
  });
  return cfg;
}

function uniqueId(base: string, actions: WorkflowAction[]): string {
  let i = 1;
  while (actions.some((a) => a.id === `${base}_${i}`)) i += 1;
  return `${base}_${i}`;
}

// Keyed by workflow id so navigating between workflows (or to /new) resets all editor state.
export default function WorkflowEditorPage() {
  const { wid } = useParams<{ wid: string }>();
  return <WorkflowEditor key={wid ?? 'new'} />;
}

function WorkflowEditor() {
  const { wid } = useParams<{ wid: string }>();
  const [search] = useSearchParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { user } = useCurrentUser();
  const isAdmin = user ? !user.authEnabled || user.isAdmin : false;
  const { projectId, project, savedQueryIdSet } = useProjectContext();
  const [wf, setWf] = useState<Workflow | null>(wid ? null : newWorkflow(projectId!, search.get('query') ?? ''));
  const [errors, setErrors] = useState<ValidationError[]>([]);
  const [dryRun, setDryRun] = useState<DryRunResult | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [busy, setBusy] = useState<'save' | 'dry' | 'run' | null>(null);

  const { data: types = [] } = useQuery({ queryKey: ['workflow-action-types'], queryFn: getActionTypes });
  const { data: config } = useQuery({ queryKey: ['workflow-config'], queryFn: getWorkflowConfig });
  const { data: queries = [] } = useQuery({ queryKey: ['savedQueries'], queryFn: getSavedQueries });
  const projectQueries = useMemo(() => queries.filter((q) => savedQueryIdSet.has(q.id)), [queries, savedQueryIdSet]);

  useEffect(() => {
    if (wid) getWorkflow(wid).then(setWf).catch((e) => setLoadError(getErrorMessage(e)));
  }, [wid]);

  useEffect(() => {
    if (!wf) return undefined;
    let cancelled = false;
    const t = setTimeout(() => {
      validateWorkflow(wf).then((r) => { if (!cancelled) { setErrors(r); } }).catch(() => undefined);
    }, 600);
    return () => { cancelled = true; clearTimeout(t); };
  }, [wf]);

  if (loadError) {
    return <Alert type="error" showIcon style={{ margin: 16 }} message={loadError} />;
  }
  if (!wf) {
    return <Spin />;
  }

  // The runner checks membership too; checking here shows the fix before a run fails.
  const ownerOutside = !!wf.id && (wf.status === 'owner_lost_access'
    || (!!project && !!wf.owner && wf.owner !== project.owner && !(project.sharedWith ?? []).includes(wf.owner)));

  const set = (patch: Partial<Workflow>) => setWf({ ...wf, ...patch });
  const setSchedule = (patch: Partial<Workflow['schedule']>) => set({ schedule: { ...wf.schedule, ...patch } });
  const errorAt = (p: string) => errors.find((e) => e.path === p)?.message;
  const setAction = (i: number, a: WorkflowAction) => set({ actions: wf.actions.map((x, j) => (j === i ? a : x)) });
  const move = (i: number, d: -1 | 1) => {
    const next = [...wf.actions];
    [next[i], next[i + d]] = [next[i + d], next[i]];
    set({ actions: next });
  };

  const save = async () => {
    setBusy('save');
    try {
      const saved = wf.id ? await updateWorkflow(wf.id, wf) : await createWorkflow(wf);
      setWf(saved);
      queryClient.invalidateQueries({ queryKey: ['workflows'] });
      message.success('Saved');
      if (!wf.id) navigate(`/projects/${projectId}/workflows/${saved.id}`, { replace: true });
    } catch (e) {
      const v = validationErrors(e);
      if (v) setErrors(v);
      else message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  const runDry = async () => {
    setBusy('dry');
    try {
      // Dry-run the draft, not the last saved version.
      setDryRun(await dryRunWorkflow({ ...wf, id: undefined }));
    } catch (e) {
      const v = validationErrors(e);
      if (v) setErrors(v);
      else message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  const runNow = async () => {
    setBusy('run');
    try {
      const run = await runWorkflowNow(wf.id!);
      message.info(`Run ${run.status}`);
      queryClient.invalidateQueries({ queryKey: ['workflow-runs', wf.id] });
      queryClient.invalidateQueries({ queryKey: ['workflow-health', wf.id] });
    } catch (e) {
      message.error(getErrorMessage(e));
    } finally {
      setBusy(null);
    }
  };

  // Renew and take ownership return the updated workflow; both leave it disabled until re-enabled.
  const recover = async (op: (id: string) => Promise<Workflow>, done: string) => {
    try {
      setWf(await op(wf.id!));
      queryClient.invalidateQueries({ queryKey: ['workflows'] });
      message.success(done);
    } catch (e) {
      message.error(getErrorMessage(e));
    }
  };

  const toggle = async (enabled: boolean) => {
    try {
      setWf(await setWorkflowEnabled(wf.id!, enabled));
    } catch (e) {
      message.error(getErrorMessage(e));
    }
  };

  const definition = (
    <Space direction="vertical" style={{ width: '100%', maxWidth: 900 }} size="large">
      <Form layout="vertical" component="div">
        <Form.Item label="Name" required validateStatus={errorAt('name') ? 'error' : undefined} help={errorAt('name')}>
          <Input value={wf.name} onChange={(e) => set({ name: e.target.value })} />
        </Form.Item>
        <Form.Item label="Description">
          <Input.TextArea rows={2} value={wf.description} onChange={(e) => set({ description: e.target.value })} />
        </Form.Item>
        <Form.Item label="Saved query" required validateStatus={errorAt('savedQueryId') ? 'error' : undefined}
          help={errorAt('savedQueryId')}>
          <Select showSearch optionFilterProp="label" value={wf.savedQueryId || undefined}
            onChange={(v) => set({ savedQueryId: v })}
            options={projectQueries.map((q) => ({ value: q.id, label: q.name }))} />
        </Form.Item>
        <Space wrap>
          <Form.Item label="Frequency">
            <Select value={wf.schedule.frequency} style={{ width: 140 }} onChange={(frequency) => setSchedule(frequencyPatch(wf.schedule, frequency))}
              options={['hourly', 'daily', 'weekly', 'monthly'].map((f) => ({ value: f, label: f }))} />
          </Form.Item>
          <Form.Item label={wf.schedule.frequency === 'hourly' ? 'Minute (HH:mm, hour ignored)' : 'Time'}
            validateStatus={errorAt('schedule.timeOfDay') ? 'error' : undefined} help={errorAt('schedule.timeOfDay')}>
            <input type="time" value={wf.schedule.timeOfDay} onChange={(e) => setSchedule({ timeOfDay: e.target.value })}
              className="ant-input" style={{ width: 130 }} />
          </Form.Item>
          {wf.schedule.frequency === 'weekly' && (
            <Form.Item label="Day" validateStatus={errorAt('schedule.dayOfWeek') ? 'error' : undefined}
              help={errorAt('schedule.dayOfWeek')}>
              <Select value={wf.schedule.dayOfWeek ?? 1} style={{ width: 140 }} onChange={(dayOfWeek) => setSchedule({ dayOfWeek })}
                options={DAYS.map((d, i) => ({ value: i, label: d }))} />
            </Form.Item>
          )}
          {wf.schedule.frequency === 'monthly' && (
            <Form.Item label="Day of month" validateStatus={errorAt('schedule.dayOfMonth') ? 'error' : undefined}
              help={errorAt('schedule.dayOfMonth')}>
              <InputNumber min={1} max={28} value={wf.schedule.dayOfMonth ?? 1} onChange={(v) => setSchedule({ dayOfMonth: v })} />
            </Form.Item>
          )}
          <Form.Item label="Timeout (s)" validateStatus={errorAt('schedule.timeoutSeconds') ? 'error' : undefined}
            help={errorAt('schedule.timeoutSeconds')}>
            <InputNumber min={1} max={86400} value={wf.schedule.timeoutSeconds}
              onChange={(v) => setSchedule({ timeoutSeconds: v ?? 300 })} />
          </Form.Item>
        </Space>
      </Form>

      <Typography.Title level={5}>Actions</Typography.Title>
      {wf.actions.length === 0 && <Typography.Text type="secondary">No actions yet. The query will run and its results will be recorded.</Typography.Text>}
      {wf.actions.map((a, i) => (
        <ActionCard key={i} action={a} index={i} count={wf.actions.length} type={types.find((t) => t.id === a.type)}
          refs={availableRefs(wf.actions, i, types)} errors={errors.filter((e) => e.path.startsWith(`actions[${i}]`))}
          onChange={(x) => setAction(i, x)} onMove={(d) => move(i, d)}
          onDelete={() => set({ actions: wf.actions.filter((_, j) => j !== i) })} />
      ))}
      <Dropdown trigger={['click']} menu={{
        items: types.map((t) => ({ key: t.id, label: t.displayName, title: t.description })),
        onClick: ({ key }) => {
          const t = types.find((x) => x.id === key)!;
          set({ actions: [...wf.actions, { id: uniqueId(t.id, wf.actions), type: t.id, condition: { always: true }, config: defaultsFor(t) }] });
        },
      }}>
        <Button icon={<PlusOutlined />}>Add action</Button>
      </Dropdown>
    </Space>
  );

  return (
    <div style={{ padding: 16 }}>
      <Space style={{ width: '100%', justifyContent: 'space-between', marginBottom: 12 }} wrap>
        <Typography.Title level={4} style={{ margin: 0 }}>{wf.id ? wf.name : 'New workflow'}</Typography.Title>
        <Space wrap>
          {wf.id && (
            <Space>
              <Typography.Text>Enabled</Typography.Text>
              <Switch checked={wf.schedule.enabled} onChange={toggle} />
            </Space>
          )}
          <Button icon={<ExperimentOutlined />} loading={busy === 'dry'} onClick={runDry}>Dry run</Button>
          {wf.id && <Button icon={<PlayCircleOutlined />} loading={busy === 'run'} onClick={runNow}>Run now</Button>}
          <Button type="primary" loading={busy === 'save'} disabled={errors.length > 0} onClick={save}>Save</Button>
        </Space>
      </Space>
      {wf.id && isExpired(wf, config) && (
        <Alert type="warning" showIcon style={{ marginBottom: 12 }} message="This workflow has expired"
          description="It no longer runs. Renew it, then turn on Enabled."
          action={<Button size="small" onClick={() => recover(renewWorkflow, 'Renewed. Turn on Enabled to resume.')}>Renew</Button>} />
      )}
      {ownerOutside && (
        <Alert type="warning" showIcon style={{ marginBottom: 12 }}
          message={`This workflow runs as ${wf.owner}, who is not a member of this project`}
          description={isAdmin
            ? 'It cannot run until you take ownership (it then runs as you, and needs a saved query you own) '
              + `or the project owner shares the project with ${wf.owner}.`
            : `It cannot run until an admin takes ownership or the project owner shares the project with ${wf.owner}.`}
          action={isAdmin
            ? <Button size="small" onClick={() => recover(takeOwnership, 'You own this workflow now. Point it at a saved query you own, then turn on Enabled.')}>Take ownership</Button>
            : undefined} />
      )}
      {unplacedErrors(errors).length > 0 && (
        <Alert type="error" showIcon style={{ marginBottom: 12 }} message="Fix these before saving"
          description={(
            <ul style={{ margin: 0, paddingLeft: 18 }}>
              {unplacedErrors(errors).map((e) => <li key={`${e.path}:${e.message}`}>{e.path}: {e.message}</li>)}
            </ul>
          )} />
      )}
      <Tabs items={[
        { key: 'def', label: 'Definition', children: definition },
        ...(wf.id ? [
          { key: 'runs', label: 'Runs', children: <RunsTab workflowId={wf.id} /> },
          { key: 'health', label: 'Health', children: <HealthTab workflowId={wf.id} /> },
        ] : []),
      ]} />
      <Drawer open={!!dryRun} onClose={() => setDryRun(null)} width={640} title="Dry run">
        {dryRun && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <Typography.Text>
              Query {dryRun.context.status} · {dryRun.context.rowCount} rows · {dryRun.context.durationMs} ms
            </Typography.Text>
            {dryRun.context.error && <Alert type="error" message={dryRun.context.error} />}
            <Table size="small" pagination={false} scroll={{ x: true }} rowKey={(_, i) => String(i)}
              dataSource={dryRun.context.rows.slice(0, 10)}
              columns={dryRun.context.columns.map((c) => ({ title: c, dataIndex: c }))} />
            <Typography.Text type="secondary">Side-effect actions show what they would send; nothing was sent.</Typography.Text>
            <OutcomeList outcomes={dryRun.actions} />
          </Space>
        )}
      </Drawer>
    </div>
  );
}
