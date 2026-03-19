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
import type { QuerySchedule, QuerySnapshot, ScheduleFrequency, DayOfWeek } from '../types';

const SCHEDULES_BASE = '/api/v1/schedules';

export interface ScheduleConfig {
  expirationEnabled: boolean;
  expirationDays: number;
  warningDaysBeforeExpiry: number;
}

export interface ScheduleCreateData {
  savedQueryId: string;
  description?: string;
  frequency: ScheduleFrequency;
  enabled?: boolean;
  timeOfDay?: string;
  dayOfWeek?: DayOfWeek;
  dayOfMonth?: number;
  notifyOnSuccess?: boolean;
  notifyOnFailure?: boolean;
  notifyEmails?: string[];
  retentionCount?: number;
}

// ---- Schedule Config (server-side, admin only to write) ----

export async function getScheduleConfig(): Promise<ScheduleConfig> {
  try {
    const response = await apiClient.get<ScheduleConfig>('/api/v1/workflows/config');
    return response.data;
  } catch {
    return { expirationEnabled: true, expirationDays: 90, warningDaysBeforeExpiry: 14 };
  }
}

export async function updateScheduleConfig(config: Partial<ScheduleConfig>): Promise<ScheduleConfig> {
  const current = await getScheduleConfig();
  const updated = { ...current, ...config };
  const response = await apiClient.put<ScheduleConfig>('/api/v1/workflows/config', updated);
  return response.data;
}

// ---- Schedule CRUD (server-side) ----

function extractArray<T>(data: T[] | { schedules?: T[]; snapshots?: T[] }): T[] {
  if (Array.isArray(data)) {
    return data;
  }
  if (data && typeof data === 'object') {
    if ('schedules' in data && Array.isArray(data.schedules)) {
      return data.schedules;
    }
    if ('snapshots' in data && Array.isArray(data.snapshots)) {
      return data.snapshots;
    }
  }
  return [];
}

export async function getSchedules(): Promise<QuerySchedule[]> {
  const response = await apiClient.get(SCHEDULES_BASE);
  return extractArray<QuerySchedule>(response.data);
}

export async function getScheduleForQuery(queryId: string): Promise<QuerySchedule | null> {
  try {
    const response = await apiClient.get<QuerySchedule>(`${SCHEDULES_BASE}/query/${queryId}`);
    return response.data;
  } catch {
    return null;
  }
}

export async function createSchedule(data: ScheduleCreateData): Promise<QuerySchedule> {
  const response = await apiClient.post<QuerySchedule>(SCHEDULES_BASE, data);
  return response.data;
}

export async function updateSchedule(
  id: string,
  data: Partial<ScheduleCreateData>,
): Promise<QuerySchedule> {
  const response = await apiClient.put<QuerySchedule>(`${SCHEDULES_BASE}/${id}`, data);
  return response.data;
}

export async function renewSchedule(id: string): Promise<QuerySchedule> {
  const response = await apiClient.post<QuerySchedule>(`${SCHEDULES_BASE}/${id}/renew`);
  return response.data;
}

export async function deleteSchedule(id: string): Promise<void> {
  await apiClient.delete(`${SCHEDULES_BASE}/${id}`);
}

// ---- Snapshots (server-side) ----

export async function getSnapshots(scheduleId: string): Promise<QuerySnapshot[]> {
  const response = await apiClient.get(`${SCHEDULES_BASE}/${scheduleId}/snapshots`);
  return extractArray<QuerySnapshot>(response.data);
}
