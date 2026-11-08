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
import { Descriptions, List, Tag, Typography } from 'antd';
import type { Condition, ConditionNode, Workflow, WorkflowSchedule } from '../../types/workflow';

const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

/** The schedule in a sentence: "Daily at 08:00", "Weekly on Monday at 06:30". */
export function describeSchedule(s: WorkflowSchedule): string {
  switch (s.frequency) {
    case 'hourly':
      return 'Hourly';
    case 'daily':
      return `Daily at ${s.timeOfDay}`;
    case 'weekly':
      return `Weekly on ${DAYS[s.dayOfWeek ?? 1] ?? 'Monday'} at ${s.timeOfDay}`;
    case 'monthly':
      return `Monthly on day ${s.dayOfMonth ?? 1} at ${s.timeOfDay}`;
    default:
      return String(s.frequency);
  }
}

const isGroup = (n: ConditionNode) => 'all' in n || 'any' in n;

function describeNode(node: ConditionNode): string {
  if ('all' in node) {
    return node.all.map((n) => (isGroup(n) ? `(${describeNode(n)})` : describeNode(n))).join(' AND ');
  }
  if ('any' in node) {
    return node.any.map((n) => (isGroup(n) ? `(${describeNode(n)})` : describeNode(n))).join(' OR ');
  }
  if ('not' in node) {
    return `NOT (${describeNode(node.not)})`;
  }
  return `${node.field} ${node.op} ${JSON.stringify(node.value)}`;
}

/** An action's "run when" as text: Always, the expression the author typed, or the tree spelled out. */
export function describeCondition(c: Condition | undefined): string {
  if (!c || c.always) {
    return 'Always';
  }
  if (c.expr?.trim()) {
    return c.expr;
  }
  if (c.tree) {
    return describeNode(c.tree);
  }
  return 'Always';
}

function stateOf(wf: Workflow): { label: string; color: string } {
  if (wf.status === 'expired') {
    return { label: 'Expired', color: 'orange' };
  }
  if (wf.status === 'owner_lost_access') {
    return { label: 'Owner lost access', color: 'red' };
  }
  if (!wf.schedule.enabled || wf.status === 'paused') {
    return { label: 'Paused', color: 'default' };
  }
  return { label: 'Enabled', color: 'green' };
}

/**
 * A read-only summary of a workflow: what it runs, when, and what each action does and
 * when. There is no read-only view of a workflow elsewhere (the editor is the only
 * surface, and its action cards are forms), so this is written for the canvas from the
 * Workflow type. The editor stays one click away through full screen.
 */
export default function WorkflowSummary({ workflow: wf }: { workflow: Workflow }) {
  const state = stateOf(wf);
  return (
    <div style={{ padding: 12, height: '100%', overflow: 'auto' }}>
      <Typography.Title level={5} style={{ marginTop: 0 }}>{wf.name}</Typography.Title>
      {wf.description && <Typography.Paragraph type="secondary">{wf.description}</Typography.Paragraph>}
      <Descriptions size="small" column={1} bordered>
        <Descriptions.Item label="State"><Tag color={state.color}>{state.label}</Tag></Descriptions.Item>
        <Descriptions.Item label="Schedule">{describeSchedule(wf.schedule)}</Descriptions.Item>
        <Descriptions.Item label="Saved query"><Typography.Text code>{wf.savedQueryId}</Typography.Text></Descriptions.Item>
        {wf.lastRunAt && (
          <Descriptions.Item label="Last run">
            {wf.lastRunAt}{wf.lastStatus ? ` (${wf.lastStatus})` : ''}
          </Descriptions.Item>
        )}
        {wf.nextRunAt && <Descriptions.Item label="Next run">{wf.nextRunAt}</Descriptions.Item>}
      </Descriptions>
      <Typography.Title level={5}>Actions</Typography.Title>
      {wf.actions.length === 0 ? (
        <Typography.Text type="secondary">This workflow has no actions.</Typography.Text>
      ) : (
        <List
          size="small"
          bordered
          dataSource={wf.actions}
          renderItem={(a, i) => (
            <List.Item style={{ display: 'block' }}>
              <div>
                <Typography.Text type="secondary">{i + 1}. </Typography.Text>
                <Typography.Text strong>{a.type}</Typography.Text>{' '}
                <Tag>{a.id}</Tag>
              </div>
              <div>
                <Typography.Text type="secondary">Run when: </Typography.Text>
                <Typography.Text code={!!a.condition?.expr}>{describeCondition(a.condition)}</Typography.Text>
              </div>
            </List.Item>
          )}
        />
      )}
    </div>
  );
}
