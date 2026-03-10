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

export interface GaugeValue {
  value: number | string | unknown[];
}

export interface CounterValue {
  count: number;
}

export interface HistogramValue {
  count: number;
  max: number;
  mean: number;
  min: number;
  p50: number;
  p75: number;
  p95: number;
  p98: number;
  p99: number;
  p999: number;
  stddev: number;
}

export interface TimerValue {
  count: number;
  max: number;
  mean: number;
  min: number;
  p50: number;
  p75: number;
  p95: number;
  p98: number;
  p99: number;
  p999: number;
  stddev: number;
  m1_rate: number;
  m5_rate: number;
  m15_rate: number;
  mean_rate: number;
  duration_units: string;
  rate_units: string;
}

export interface MeterValue {
  count: number;
  m1_rate: number;
  m5_rate: number;
  m15_rate: number;
  mean_rate: number;
  units: string;
}

export interface MetricsResponse {
  gauges: Record<string, GaugeValue>;
  counters: Record<string, CounterValue>;
  histograms: Record<string, HistogramValue>;
  meters: Record<string, MeterValue>;
  timers: Record<string, TimerValue>;
}

export async function getMetrics(): Promise<MetricsResponse> {
  const response = await apiClient.get<MetricsResponse>('/status/metrics');
  return response.data;
}
