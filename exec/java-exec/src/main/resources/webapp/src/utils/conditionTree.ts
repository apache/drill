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
import type {
  ActionTypeInfo, Condition, ConditionLeaf, ConditionNode, Workflow, WorkflowAction,
} from '../types/workflow';

export const BASE_FIELDS = ['status', 'rowCount', 'rowCountDelta', 'deltaPercent', 'durationMs', 'intervalMs', 'error'];
export const OPS = ['==', '!=', '>', '>=', '<', '<=', 'contains', 'startsWith'];

export function isLeaf(n: ConditionNode): n is ConditionLeaf {
  return 'field' in n;
}

function literal(v: unknown): string {
  if (v === null || v === undefined) {
    return 'null';
  }
  if (typeof v === 'string') {
    return `'${v.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
  }
  return String(v);
}

export function treeToExpr(n: ConditionNode, nested = false): string {
  if (isLeaf(n)) {
    return `${n.field} ${n.op} ${literal(n.value)}`;
  }
  if ('not' in n) {
    return `!(${treeToExpr(n.not)})`;
  }
  const key = 'all' in n ? 'all' : 'any';
  const parts = (key === 'all' ? (n as { all: ConditionNode[] }).all : (n as { any: ConditionNode[] }).any)
    .map((c) => treeToExpr(c, true));
  const s = parts.join(key === 'all' ? ' && ' : ' || ');
  return nested && parts.length > 1 ? `(${s})` : s;
}

export interface BuilderModel { mode: 'all' | 'any'; leaves: ConditionLeaf[] }

/** One level of AND/OR over leaves; anything deeper returns null (edit as an expression). */
export function toBuilderModel(tree: ConditionNode | undefined): BuilderModel | null {
  if (!tree) {
    return { mode: 'all', leaves: [] };
  }
  if (isLeaf(tree)) {
    return { mode: 'all', leaves: [tree] };
  }
  if ('not' in tree) {
    return null;
  }
  const mode = 'all' in tree ? 'all' : 'any';
  const children = mode === 'all' ? (tree as { all: ConditionNode[] }).all : (tree as { any: ConditionNode[] }).any;
  return children.every(isLeaf) ? { mode, leaves: children as ConditionLeaf[] } : null;
}

export function fromBuilderModel(m: BuilderModel): ConditionNode {
  return m.mode === 'all' ? { all: m.leaves } : { any: m.leaves };
}

export function coerceValue(s: string): unknown {
  const t = s.trim();
  if (t === 'true') {
    return true;
  }
  if (t === 'false') {
    return false;
  }
  if (t === 'null') {
    return null;
  }
  if (/^-?\d+(\.\d+)?$/.test(t)) {
    return Number(t);
  }
  return s;
}

export function availableRefs(actions: WorkflowAction[], index: number, types: ActionTypeInfo[]): string[] {
  const refs = [...BASE_FIELDS];
  actions.slice(0, index).forEach((a) => {
    const t = types.find((x) => x.id === a.type);
    Object.keys(t?.outputs ?? {}).forEach((o) => refs.push(`actions.${a.id}.${o}`));
  });
  return refs;
}

export function summarizeCondition(c: Condition | undefined): string {
  if (!c || c.always) {
    return 'Always';
  }
  if (c.expr) {
    return c.expr;
  }
  return c.tree ? treeToExpr(c.tree) : 'Always';
}

/** Error paths the editor renders next to a field; action errors render inside their ActionCard. */
export const FIELD_ERROR_PATHS = [
  'name', 'savedQueryId', 'schedule.timeOfDay', 'schedule.dayOfWeek', 'schedule.dayOfMonth', 'schedule.timeoutSeconds',
];

/** Errors no field shows, so the editor can list them instead of leaving Save disabled with no reason. */
export function unplacedErrors<E extends { path: string }>(errors: E[]): E[] {
  return errors.filter((e) => !FIELD_ERROR_PATHS.includes(e.path) && !e.path.startsWith('actions['));
}

/** Frequency change that fills in the day field the new frequency requires (the inputs display 1 when unset). */
export function frequencyPatch(schedule: Workflow['schedule'], frequency: Workflow['schedule']['frequency']) {
  return {
    frequency,
    dayOfWeek: frequency === 'weekly' ? (schedule.dayOfWeek ?? 1) : schedule.dayOfWeek,
    dayOfMonth: frequency === 'monthly' ? (schedule.dayOfMonth ?? 1) : schedule.dayOfMonth,
  };
}

export function newWorkflow(projectId: string, savedQueryId = ''): Workflow {
  return {
    projectId,
    name: '',
    savedQueryId,
    schedule: { frequency: 'daily', timeOfDay: '08:00', timeoutSeconds: 300, enabled: false },
    actions: [],
    retentionCount: 30,
  };
}
