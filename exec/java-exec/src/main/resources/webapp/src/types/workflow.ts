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
export type Frequency = 'hourly' | 'daily' | 'weekly' | 'monthly';

export interface ConditionLeaf { field: string; op: string; value: unknown }
export type ConditionNode =
  | { all: ConditionNode[] }
  | { any: ConditionNode[] }
  | { not: ConditionNode }
  | ConditionLeaf;

export interface Condition { always?: boolean; tree?: ConditionNode; expr?: string }

export interface WorkflowAction {
  id: string;
  type: string;
  condition: Condition;
  config: Record<string, unknown>;
}

export interface WorkflowSchedule {
  frequency: Frequency;
  timeOfDay: string;
  dayOfWeek?: number | null;
  dayOfMonth?: number | null;
  timeoutSeconds: number;
  enabled: boolean;
  expiresAt?: string | null;
}

export interface Workflow {
  id?: string;
  projectId: string | null;
  name: string;
  description?: string;
  owner?: string;
  savedQueryId: string;
  schedule: WorkflowSchedule;
  actions: WorkflowAction[];
  retentionCount: number;
  status?: 'active' | 'paused' | 'expired' | 'owner_lost_access';
  createdAt?: string;
  lastRunAt?: string;
  nextRunAt?: string;
  lastStatus?: 'success' | 'error';
  consecutiveFailures?: number;
  isRunning?: boolean;
}

export interface ValidationError { path: string; message: string }

export interface ActionOutcome {
  id: string;
  type: string;
  outcome: 'fired' | 'skipped' | 'errored';
  reason?: string;
  outputs?: Record<string, unknown>;
  error?: string;
  durationMs: number;
}

export interface WorkflowRun {
  id: string;
  scheduleId: string;
  executedAt: string;
  status: 'success' | 'error' | 'skipped';
  trigger?: 'schedule' | 'manual';
  rowCount?: number;
  duration?: number;
  errorMessage?: string;
  previewRows?: string;
  previewColumns?: string;
  rowCountDelta?: number;
  actions?: ActionOutcome[];
}

export interface DryRunResult {
  context: {
    status: string; rowCount: number; rowCountDelta?: number; durationMs: number; error?: string;
    columns: string[]; rows: Record<string, string>[];
  };
  actions: ActionOutcome[];
}

export interface JsonSchemaProp {
  type: 'string' | 'number' | 'integer' | 'boolean' | 'array';
  title?: string;
  description?: string;
  enum?: string[];
  default?: unknown;
  format?: string;
  items?: { type: 'string'; format?: string };
  'x-template'?: boolean;
  'x-multiline'?: boolean;
}

export interface ActionTypeInfo {
  id: string;
  displayName: string;
  description: string;
  configSchema: { type: 'object'; required?: string[]; properties?: Record<string, JsonSchemaProp> };
  outputs: Record<string, string>;
  hasSideEffects: boolean;
}

export interface WorkflowConfig {
  expirationEnabled: boolean;
  expirationDays: number;
  warningDaysBeforeExpiry: number;
  actionTimeoutSeconds: number;
}

export interface ObservabilitySummary {
  setupRequired: boolean;
  /** Present on partial failure; the data keys are then empty. */
  error?: string;
  rows: { workflowId: string; runs: string; successes: string; avgDurationMs: string; lastTs: string }[];
}

export interface WorkflowHealth {
  setupRequired: boolean;
  /** Present on partial failure; the data keys are then empty. */
  error?: string;
  daily: { day: string; runs: string; successes: string; avgDurationMs: string }[];
  actions: { id: string; type: string; outcome: string; cnt: string }[];
}

export interface ObservabilityStatus {
  /** Omitted for non-admins. */
  logDir?: string | null;
  logPresent: boolean; workspace: boolean; format: boolean; ready: boolean;
}
