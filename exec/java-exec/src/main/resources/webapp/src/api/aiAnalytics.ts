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

const ANALYTICS_BASE = '/api/v1/ai/analytics';
const PRICING_BASE = '/api/v1/ai/pricing';

export interface AiAnalyticsStatus {
  logDirConfigured: boolean;
  logDir: string | null;
  logFileExists: boolean;
  workspaceExists: boolean;
  formatExists: boolean;
  ready: boolean;
}

export interface AiPricingEntry {
  provider: string;
  model: string;
  inputPricePerMTokens: number;
  outputPricePerMTokens: number;
  currency: string;
  updatedAt?: number;
}

export interface AiAnalyticsTotals {
  totalCalls?: number;
  successCount?: number;
  failureCount?: number;
  cancelledCount?: number;
  avgDurationMs?: number;
  totalPromptTokens?: number;
  totalResponseTokens?: number;
  totalTokens?: number;
  uniqueUsers?: number;
}

export interface AiCostProjection {
  /** Cost accumulated in the current calendar month so far. */
  mtdCostUsd: number;
  /** Linear extrapolation of mtd cost to month-end. */
  projectedMonthEndCostUsd: number;
  daysElapsed: number;
  daysInMonth: number;
  /** ISO-8601 instant marking the start of the current month (UTC). */
  monthStart: string;
}

export interface AiAnalyticsSummary {
  totals: AiAnalyticsTotals;
  series: Array<Record<string, unknown>>;
  byFeature: Array<Record<string, unknown>>;
  byModel: Array<Record<string, unknown>>;
  byUser: Array<Record<string, unknown>>;
  latencyByModel: Array<Record<string, unknown>>;
  pricing: Record<string, AiPricingEntry>;
  projection?: AiCostProjection;
  /**
   * True when the data source isn't ready (no event log yet, or the
   * dfs.ai_logs workspace/format isn't set up). Distinguishes "not
   * configured" from "configured but genuinely zero usage".
   */
  notConfigured: boolean;
}

export interface AiAnalyticsEventsResponse {
  rows: Array<Record<string, string>>;
  columns: string[];
  limit: number;
  offset: number;
  /** See {@link AiAnalyticsSummary.notConfigured}. */
  notConfigured: boolean;
}

export interface AiEventsFilters {
  from?: string;
  to?: string;
  user?: string;
  feature?: string;
  provider?: string;
  model?: string;
  success?: 'true' | 'false' | 'cancelled';
  limit?: number;
  offset?: number;
}

export async function getAnalyticsStatus(): Promise<AiAnalyticsStatus> {
  const r = await apiClient.get<AiAnalyticsStatus>(`${ANALYTICS_BASE}/status`);
  return r.data;
}

export async function setupAnalytics(): Promise<{ success: boolean; message: string }> {
  const r = await apiClient.post<{ success: boolean; message: string }>(`${ANALYTICS_BASE}/setup`);
  return r.data;
}

export async function getAnalyticsSummary(from?: string, to?: string): Promise<AiAnalyticsSummary> {
  const params: Record<string, string> = {};
  if (from) {
    params.from = from;
  }
  if (to) {
    params.to = to;
  }
  const r = await apiClient.get<AiAnalyticsSummary>(`${ANALYTICS_BASE}/summary`, { params });
  return r.data;
}

export async function getAnalyticsEvents(filters: AiEventsFilters): Promise<AiAnalyticsEventsResponse> {
  const params: Record<string, string | number> = {};
  Object.entries(filters).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') {
      params[k] = v as string | number;
    }
  });
  const r = await apiClient.get<AiAnalyticsEventsResponse>(`${ANALYTICS_BASE}/events`, { params });
  return r.data;
}

export async function listPricing(): Promise<AiPricingEntry[]> {
  const r = await apiClient.get<{ entries: AiPricingEntry[] }>(PRICING_BASE);
  return r.data.entries ?? [];
}

export async function upsertPricing(entry: AiPricingEntry): Promise<AiPricingEntry> {
  const r = await apiClient.post<AiPricingEntry>(PRICING_BASE, entry);
  return r.data;
}

export async function deletePricing(provider: string, model: string): Promise<void> {
  await apiClient.delete(`${PRICING_BASE}/${encodeURIComponent(provider)}/${encodeURIComponent(model)}`);
}
