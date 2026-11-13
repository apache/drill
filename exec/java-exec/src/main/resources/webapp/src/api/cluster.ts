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

/** A single Drillbit as reported by {@code GET /cluster.json}. */
export interface DrillbitInfo {
  address: string;
  httpPort: string;
  userPort: string;
  controlPort: string;
  dataPort: string;
  version: string;
  current: boolean;
  versionMatch: boolean;
  state: string;
}

/** Distributed-queue summary (only populated when ZK-based queues are enabled). */
export interface QueueInfo {
  enabled: boolean;
  smallQueueSize: number;
  largeQueueSize: number;
  threshold: string;
  smallQueueMemory: string;
  largeQueueMemory: string;
  totalMemory: string;
}

/**
 * Full cluster snapshot returned by {@code GET /cluster.json}. Admin-only
 * fields (process / admin users) are absent when the caller is not an admin.
 */
export interface ClusterInfo {
  drillbits: DrillbitInfo[];
  currentVersion: string;
  mismatchedVersions: string[];
  userEncryptionEnabled: boolean;
  bitEncryptionEnabled: boolean;
  shouldShowAdminInfo: boolean;
  queueInfo: QueueInfo;
  authEnabled: boolean;
  processUser?: string;
  processUserGroups?: string;
  adminUsers?: string;
  adminUserGroups?: string;
}

export async function getClusterInfo(): Promise<ClusterInfo> {
  const response = await apiClient.get<ClusterInfo>('/cluster.json');
  return response.data;
}

/** Map of {@code address-httpPort} → state for every known Drillbit. */
export async function getDrillbitStates(): Promise<Record<string, string>> {
  const response = await apiClient.get<Record<string, string>>('/state');
  return response.data;
}

export async function getRemainingQueries(): Promise<Record<string, number>> {
  const response = await apiClient.get<Record<string, number>>('/queriesCount');
  return response.data;
}

export async function getGracePeriod(): Promise<number> {
  const response = await apiClient.get<{ gracePeriod: number }>('/gracePeriod');
  return response.data.gracePeriod;
}

export async function getHttpPort(): Promise<number> {
  const response = await apiClient.get<{ port: number }>('/portNum');
  return response.data.port;
}

/** Triggers a graceful (quiescent) shutdown of the local Drillbit. Admin-only. */
export async function gracefulShutdown(): Promise<{ response: string }> {
  const response = await apiClient.post<{ response: string }>('/gracefulShutdown');
  return response.data;
}

/** Triggers a graceful shutdown of a remote Drillbit by hostname. Admin-only. */
export async function gracefulShutdownHost(hostname: string): Promise<string> {
  const response = await apiClient.post<string>(`/gracefulShutdown/${encodeURIComponent(hostname)}`);
  return response.data;
}

/** Triggers a forceful shutdown. Admin-only. */
export async function forcefulShutdown(): Promise<{ response: string }> {
  const response = await apiClient.post<{ response: string }>('/shutdown');
  return response.data;
}

/** Puts the local Drillbit into quiescent mode. Admin-only. */
export async function quiescentMode(): Promise<{ response: string }> {
  const response = await apiClient.post<{ response: string }>('/quiescent');
  return response.data;
}
