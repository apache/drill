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
import apiClient from './client';
import type { QueryRequest, QueryResult } from '../types';

/**
 * Execute a SQL query and return results
 */
export async function executeQuery(request: QueryRequest): Promise<QueryResult> {
  // Strip trailing semicolons — Drill's SQL parser doesn't accept them
  // (the CLI strips them automatically, but the REST API passes them through)
  const query = request.query.replace(/;\s*$/, '');
  const body: Record<string, unknown> = {
    query,
    queryType: request.queryType || 'SQL',
    autoLimit: request.autoLimitRowCount ? String(request.autoLimitRowCount) : '',
  };
  if (request.userName) {
    body.userName = request.userName;
  }
  if (request.defaultSchema) {
    body.defaultSchema = request.defaultSchema;
  }
  if (request.options) {
    body.options = request.options;
  }
  const response = await apiClient.post<QueryResult>('/query.json', body);

  // Drill returns HTTP 200 even for failed queries — check queryState.
  // However, when autoLimit is used, Drill may set queryState to FAILED
  // even though it successfully returned partial results (the auto-limit
  // interrupted the query).  If we got rows back, treat it as a success.
  const hasData = response.data.rows && response.data.rows.length > 0;
  if (response.data.queryState === 'FAILED' && !hasData) {
    const queryId = response.data.queryId;
    let detail = response.data.errorMessage || response.data.exception || '';

    // If no error detail in the query response, try fetching the profile
    if (!detail && queryId) {
      try {
        const profile = await apiClient.get(`/profiles/${queryId}.json`);
        const profileData = profile.data;
        // Prefer verboseError (detailed) over error (concise)
        if (profileData?.verboseError) {
          detail = profileData.verboseError;
        } else if (profileData?.error) {
          detail = profileData.error;
        }
      } catch {
        // Profile fetch failed — fall through to generic message
      }
    }

    if (!detail) {
      detail = 'Query execution failed — check /profiles/' + (queryId || '') + ' for details';
    }
    throw new Error(queryId ? `${detail} [queryId: ${queryId}]` : detail);
  }

  return response.data;
}

/**
 * Cancel a running query
 */
export async function cancelQuery(queryId: string): Promise<void> {
  await apiClient.get(`/profiles/cancel/${encodeURIComponent(queryId)}`);
}

/**
 * Get query profile/history
 */
export interface QueryProfile {
  queryId: string;
  user: string;
  startTime: number;
  endTime?: number;
  state: string;
  query: string;
  foreman: string;
  duration?: string;
  totalCost?: number;
  queueName?: string;
}

export interface QueryProfilesResponse {
  runningQueries: QueryProfile[];
  finishedQueries: QueryProfile[];
}

export async function getQueryProfiles(): Promise<QueryProfilesResponse> {
  const response = await apiClient.get<QueryProfilesResponse>('/profiles.json');
  return response.data;
}

export async function getRunningQueries(): Promise<QueryProfile[]> {
  const response = await apiClient.get<QueryProfile[]>('/profiles/running.json');
  return response.data;
}

// Detailed profile interfaces
export interface DrillbitEndpoint {
  address: string;
  userPort?: number;
  controlPort?: number;
  dataPort?: number;
}

export interface MetricValue {
  metricId: number;
  longValue?: number;
  doubleValue?: number;
}

export interface StreamProfileData {
  records: number;
  batches: number;
  schemas: number;
}

export interface OperatorProfile {
  operatorId: number;
  operatorTypeName: string;
  setupNanos: number;
  processNanos: number;
  waitNanos: number;
  peakLocalMemoryAllocated: number;
  inputProfile: StreamProfileData[];
  metric: MetricValue[];
}

export interface MinorFragmentProfile {
  minorFragmentId: number;
  state: string;
  startTime: number;
  endTime: number;
  memoryUsed: number;
  maxMemoryUsed: number;
  endpoint: DrillbitEndpoint;
  operatorProfile: OperatorProfile[];
  lastUpdate: number;
  lastProgress: number;
}

export interface MajorFragmentProfile {
  majorFragmentId: number;
  minorFragmentProfile: MinorFragmentProfile[];
}

export interface DetailedQueryProfile {
  queryId: string;
  query: string;
  plan: string;
  state: string;
  user: string;
  start: number;
  end: number;
  planEnd: number;
  queueWaitEnd: number;
  totalCost: number;
  totalFragments: number;
  finishedFragments: number;
  queueName: string;
  foreman: DrillbitEndpoint;
  fragmentProfile: MajorFragmentProfile[];
  optionsJson: string;
  error?: string;
  verboseError?: string;
  scannedPlugins: string[];
  autoLimit?: number;
}

export async function getQueryProfileDetail(queryId: string): Promise<DetailedQueryProfile> {
  const response = await apiClient.get<DetailedQueryProfile>(`/profiles/${encodeURIComponent(queryId)}.json`);
  return response.data;
}
