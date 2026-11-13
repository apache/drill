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
import { describe, it, expect } from 'vitest';
import {
  treeToExpr, toBuilderModel, fromBuilderModel, coerceValue, availableRefs, summarizeCondition,
  frequencyPatch, newWorkflow, unplacedErrors, isExpired,
} from './conditionTree';
import type { ActionTypeInfo, WorkflowAction } from '../types/workflow';

describe('conditionTree', () => {
  it('renders trees to expressions with quoting and grouping', () => {
    expect(treeToExpr({ field: 'status', op: '==', value: "it's" })).toBe("status == 'it\\'s'");
    expect(treeToExpr({
      any: [{ field: 'a', op: '>', value: 1 }, { all: [{ field: 'b', op: '<', value: 2 }, { field: 'c', op: '==', value: null }] }],
    })).toBe('a > 1 || (b < 2 && c == null)');
    expect(treeToExpr({ not: { field: 'x', op: '==', value: true } })).toBe('!(x == true)');
  });

  it('round-trips one-level trees through the builder model', () => {
    const tree = { all: [{ field: 'rowCount', op: '>', value: 5 }, { field: 'status', op: '==', value: 'error' }] };
    const model = toBuilderModel(tree)!;
    expect(model.mode).toBe('all');
    expect(fromBuilderModel(model)).toEqual(tree);
    expect(toBuilderModel({ field: 'a', op: '>', value: 1 })).toEqual({ mode: 'all', leaves: [{ field: 'a', op: '>', value: 1 }] });
    expect(toBuilderModel({ any: [{ all: [] }] })).toBeNull();
    expect(toBuilderModel({ not: { field: 'a', op: '==', value: 1 } })).toBeNull();
  });

  it('coerces builder values', () => {
    expect(coerceValue('42')).toBe(42);
    expect(coerceValue('-1.5')).toBe(-1.5);
    expect(coerceValue('true')).toBe(true);
    expect(coerceValue('null')).toBeNull();
    expect(coerceValue('root')).toBe('root');
    ['Infinity', '0x10', '1e3', '0.', '.5', ''].forEach((v) => expect(coerceValue(v)).toBe(v));
  });

  it('offers earlier action outputs only', () => {
    const types = [{ id: 'record_alert', outputs: { message: 'string' } }] as unknown as ActionTypeInfo[];
    const actions = [
      { id: 'a', type: 'record_alert' }, { id: 'b', type: 'record_alert' },
    ] as WorkflowAction[];
    const refs = availableRefs(actions, 1, types);
    expect(refs).toContain('rowCount');
    expect(refs).toContain('actions.a.message');
    expect(refs).not.toContain('actions.b.message');
  });

  it('summarizes conditions', () => {
    expect(summarizeCondition({ always: true })).toBe('Always');
    expect(summarizeCondition({ expr: 'rowCount > 1' })).toBe('rowCount > 1');
    expect(summarizeCondition({ tree: { field: 'a', op: '==', value: 1 } })).toBe('a == 1');
  });

  it('fills in the day a weekly or monthly schedule requires', () => {
    const s = newWorkflow('p').schedule;
    expect(frequencyPatch(s, 'weekly')).toEqual({ frequency: 'weekly', dayOfWeek: 1, dayOfMonth: undefined });
    expect(frequencyPatch(s, 'monthly')).toEqual({ frequency: 'monthly', dayOfWeek: undefined, dayOfMonth: 1 });
    expect(frequencyPatch({ ...s, dayOfWeek: 0 }, 'weekly').dayOfWeek).toBe(0);
  });

  it('lists only the errors no field renders', () => {
    const errors = [
      { path: 'name', message: 'Required' },
      { path: 'schedule.dayOfWeek', message: 'Required for weekly schedules' },
      { path: 'actions[0].condition', message: 'bad' },
      { path: 'actions', message: 'Too many actions' },
      { path: 'schedule.frequency', message: 'Unknown' },
    ];
    expect(unplacedErrors(errors).map((e) => e.path)).toEqual(['actions', 'schedule.frequency']);
  });

  it('treats a paused workflow past its expiry date as expired', () => {
    const w = newWorkflow('p', 'q');
    w.status = 'paused';
    w.schedule.expiresAt = '2026-01-01T00:00:00Z';
    const now = Date.parse('2026-06-01T00:00:00Z');
    expect(isExpired(w, { expirationEnabled: true }, now)).toBe(true);
    expect(isExpired(w, { expirationEnabled: false }, now)).toBe(false);
    expect(isExpired(w, undefined, now)).toBe(false);
    w.schedule.expiresAt = '2027-01-01T00:00:00Z';
    expect(isExpired(w, { expirationEnabled: true }, now)).toBe(false);
    w.status = 'expired';
    expect(isExpired(w, undefined, now)).toBe(true);
  });
});
