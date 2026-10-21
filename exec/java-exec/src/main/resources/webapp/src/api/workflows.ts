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
import axios from 'axios';
import apiClient from './client';
import type {
  ActionTypeInfo, ConditionNode, DryRunResult, ObservabilityStatus, ObservabilitySummary, ValidationError,
  Workflow, WorkflowConfig, WorkflowHealth, WorkflowRun,
} from '../types/workflow';

const BASE = '/api/v1/workflows';

export async function listWorkflows(projectId?: string): Promise<Workflow[]> {
  return (await apiClient.get(BASE, { params: projectId ? { projectId } : {} })).data;
}
export async function getWorkflow(id: string): Promise<Workflow> {
  return (await apiClient.get(`${BASE}/${id}`)).data;
}
export async function createWorkflow(wf: Workflow): Promise<Workflow> {
  return (await apiClient.post(BASE, wf)).data;
}
export async function updateWorkflow(id: string, wf: Workflow): Promise<Workflow> {
  return (await apiClient.put(`${BASE}/${id}`, wf)).data;
}
export async function deleteWorkflow(id: string): Promise<void> {
  await apiClient.delete(`${BASE}/${id}`);
}
export async function setWorkflowEnabled(id: string, enabled: boolean): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/enabled`, { enabled })).data;
}
export async function validateWorkflow(wf: Workflow): Promise<ValidationError[]> {
  return (await apiClient.post(`${BASE}/validate`, wf)).data.errors;
}
export async function parseCondition(expr: string): Promise<{ tree?: ConditionNode; error?: string; position?: number }> {
  return (await apiClient.post(`${BASE}/parse-condition`, { expr })).data;
}
export async function dryRunWorkflow(wf: Workflow): Promise<DryRunResult> {
  return (wf.id ? await apiClient.post(`${BASE}/${wf.id}/dry-run`) : await apiClient.post(`${BASE}/dry-run`, wf)).data;
}
export async function runWorkflowNow(id: string): Promise<WorkflowRun> {
  return (await apiClient.post(`${BASE}/${id}/run`)).data;
}
export async function renewWorkflow(id: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/renew`)).data;
}
export async function takeOwnership(id: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/take-ownership`)).data;
}
export async function assignProject(id: string, projectId: string): Promise<Workflow> {
  return (await apiClient.post(`${BASE}/${id}/assign-project`, { projectId })).data;
}
export async function listRuns(id: string): Promise<WorkflowRun[]> {
  return (await apiClient.get(`${BASE}/${id}/runs`)).data;
}
export async function getActionTypes(): Promise<ActionTypeInfo[]> {
  return (await apiClient.get(`${BASE}/action-types`)).data;
}
export async function getObservabilitySummary(projectId?: string): Promise<ObservabilitySummary> {
  return (await apiClient.get(`${BASE}/observability/summary`, { params: projectId ? { projectId } : {} })).data;
}
export async function getWorkflowHealth(id: string): Promise<WorkflowHealth> {
  return (await apiClient.get(`${BASE}/${id}/health`)).data;
}
export async function getRunningWorkflows(): Promise<Workflow[]> {
  return (await apiClient.get(`${BASE}/observability/running`)).data;
}
export async function getObservabilityStatus(): Promise<ObservabilityStatus> {
  return (await apiClient.get(`${BASE}/observability/status`)).data;
}
export async function setupObservability(): Promise<ObservabilityStatus> {
  return (await apiClient.post(`${BASE}/observability/setup`)).data;
}
export async function getWorkflowConfig(): Promise<WorkflowConfig> {
  return (await apiClient.get(`${BASE}/config`)).data;
}
export async function updateWorkflowConfig(cfg: WorkflowConfig): Promise<WorkflowConfig> {
  return (await apiClient.put(`${BASE}/config`, cfg)).data;
}

/** The server's 422 field errors, or null for any other failure. */
export function validationErrors(err: unknown): ValidationError[] | null {
  if (axios.isAxiosError(err) || (err as { isAxiosError?: boolean })?.isAxiosError) {
    const resp = (err as { response?: { status: number; data?: { errors?: ValidationError[] } } }).response;
    if (resp?.status === 422 && resp.data?.errors) {
      return resp.data.errors;
    }
  }
  return null;
}
