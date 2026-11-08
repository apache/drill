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
import { useState } from 'react';
import { Button, Modal, Typography } from 'antd';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import axios from 'axios';
import { getWorkflow, setWorkflowEnabled } from '../../api/workflows';
import type { Workflow, WorkflowAction } from '../../types/workflow';
import { describeCondition, describeSchedule } from './WorkflowSummary';

export interface WorkflowActivationCardProps {
  workflowId: string;
  /** The name Prospector gave the workflow; the dialog uses the server's copy when it has one. */
  name: string;
}

/** Config keys that name where an action sends something. */
const DESTINATION_KEYS = ['url', 'webhook', 'endpoint', 'to'];

/** What an action does, in a line, with the detail that matters before it is switched on. */
function describeAction(a: WorkflowAction): string {
  const cfg = a.config ?? {};
  switch (a.type) {
    case 'email': {
      const to = Array.isArray(cfg.to) ? cfg.to.join(', ') : '';
      return `Send an email to ${to || '(no recipients)'}`;
    }
    case 'save_to_table': {
      const where = typeof cfg.location === 'string' && cfg.location ? ` in ${cfg.location}` : '';
      return `Save the query result to a table${where}`;
    }
    case 'record_alert':
      return 'Record an alert in the run history';
    default: {
      // A type this dialog does not know. It may well send somewhere (a webhook, say), so
      // show every config entry that looks like a destination rather than only the type.
      const where = DESTINATION_KEYS
        .filter((k) => cfg[k] !== undefined && cfg[k] !== null && cfg[k] !== '')
        .map((k) => `${k}: ${Array.isArray(cfg[k]) ? (cfg[k] as unknown[]).join(', ') : String(cfg[k])}`);
      return where.length > 0 ? `${a.type} (${where.join('; ')})` : a.type;
    }
  }
}

function ConfirmBody({ wf }: { wf: Workflow }) {
  return (
    <div>
      <p>
        Activating <strong>{wf.name}</strong> lets it run on its own from now on.
        {' '}It will run <strong>{describeSchedule(wf.schedule)}</strong>, and each run does the following.
      </p>
      <ol style={{ paddingLeft: 20, margin: 0 }}>
        {wf.actions.map((a) => (
          <li key={a.id}>
            {describeAction(a)}
            <br />
            <Typography.Text type="secondary">Run when: {describeCondition(a.condition)}</Typography.Text>
          </li>
        ))}
        {wf.actions.length === 0 && <li>Nothing. It has no actions.</li>}
      </ol>
      <p style={{ marginBottom: 0, marginTop: 8 }}>You can pause it again from the Workflows page.</p>
    </div>
  );
}

const refusal = (err: unknown): string => {
  if (axios.isAxiosError(err) || (err as { isAxiosError?: boolean })?.isAxiosError) {
    const data = (err as { response?: { data?: { error?: string } } }).response?.data;
    if (data?.error) {
      return data.error;
    }
  }
  return err instanceof Error ? err.message : 'The server refused to activate the workflow.';
};

/**
 * The one place Prospector's conversation can switch a workflow on. create_workflow only
 * ever creates it paused (the server pins that), so the workflow does nothing until a person
 * presses Activate here and confirms what it will do. The dialog is built from the workflow
 * as the server holds it, not from what the model said, so it shows what will really run.
 */
export default function WorkflowActivationCard({ workflowId, name }: WorkflowActivationCardProps) {
  const queryClient = useQueryClient();
  const [refused, setRefused] = useState<string | null>(null);
  // Same key as the canvas viewer, so activating here updates the Summary tab too.
  const { data: wf, isError, error, refetch } = useQuery({
    queryKey: ['workflow', workflowId],
    queryFn: () => getWorkflow(workflowId),
    staleTime: 0,
    retry: false,
  });

  const activate = async () => {
    try {
      const updated = await setWorkflowEnabled(workflowId, true);
      queryClient.setQueryData(['workflow', workflowId], updated);
      setRefused(null);
    } catch (err) {
      setRefused(refusal(err));
    }
  };

  const confirm = (current: Workflow) => Modal.confirm({
    title: `Activate "${current.name || name}"?`,
    content: <ConfirmBody wf={current} />,
    okText: 'Activate workflow',
    cancelText: 'Cancel',
    // Switching on something that emails people: Enter must not confirm it by default.
    autoFocusButton: 'cancel',
    onOk: activate,
  });

  let status: string;
  let canActivate = false;
  let retryable = false;
  if (isError) {
    // Only a 404 means the workflow is gone. Anything else may pass, so offer a retry.
    if ((error as { response?: { status?: number } })?.response?.status === 404) {
      status = 'This workflow no longer exists.';
    } else {
      status = 'Could not load this workflow.';
      retryable = true;
    }
  } else if (!wf) {
    return null;
  } else if (wf.status === 'expired') {
    status = 'Expired. Renew it from the Workflows page before it can run.';
  } else if (wf.status === 'owner_lost_access') {
    status = 'The owner lost access to this project, so it cannot run.';
  } else if (wf.schedule.enabled) {
    status = `Active. ${describeSchedule(wf.schedule)}.`;
  } else {
    status = 'Created paused and not running. It will not run until you activate it.';
    canActivate = true;
  }

  return (
    <div
      data-workflow-activation={workflowId}
      style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginTop: 4, fontSize: 12 }}
    >
      <span style={{ flex: 1, minWidth: 0, opacity: 0.8 }}>{status}</span>
      {retryable && <Button size="small" onClick={() => { void refetch(); }}>Retry</Button>}
      {canActivate && wf && (
        <Button size="small" type="primary" onClick={() => confirm(wf)}>Activate</Button>
      )}
      {refused && <Typography.Text type="danger" style={{ flexBasis: '100%' }}>{refused}</Typography.Text>}
    </div>
  );
}
