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
import type { StoragePluginDetail } from '../types';

/**
 * Fetch all storage plugins.
 * GET /storage.json returns an array of { name, config } objects.
 */
export async function getPlugins(): Promise<StoragePluginDetail[]> {
  const response = await apiClient.get<StoragePluginDetail[]>('/storage.json');
  return response.data;
}

/**
 * Fetch a single storage plugin by name.
 * GET /storage/{name}.json returns { name, config }.
 */
export async function getPlugin(name: string): Promise<StoragePluginDetail> {
  const response = await apiClient.get<StoragePluginDetail>(
    `/storage/${encodeURIComponent(name)}.json`
  );
  return response.data;
}

/**
 * Create or update a storage plugin.
 * POST /storage/{name}.json with body { name, config }.
 */
export async function savePlugin(
  name: string,
  config: Record<string, unknown>
): Promise<{ result: string }> {
  const response = await apiClient.post<{ result: string }>(
    `/storage/${encodeURIComponent(name)}.json`,
    { name, config }
  );
  return response.data;
}

/**
 * Delete a storage plugin.
 * DELETE /storage/{name}.json
 */
export async function deletePlugin(name: string): Promise<{ result: string }> {
  const response = await apiClient.delete<{ result: string }>(
    `/storage/${encodeURIComponent(name)}.json`
  );
  return response.data;
}

/**
 * Enable or disable a storage plugin.
 * POST /storage/{name}/enable/{val}
 */
export async function enablePlugin(
  name: string,
  enable: boolean
): Promise<{ result: string }> {
  const response = await apiClient.post<{ result: string }>(
    `/storage/${encodeURIComponent(name)}/enable/${enable}`
  );
  return response.data;
}

/**
 * Get the export URL for a storage plugin config.
 */
export function getExportUrl(name: string): string {
  return `/storage/${encodeURIComponent(name)}/export/json`;
}

/**
 * Result of a test-connection call.
 */
export interface TestConnectionResult {
  success: boolean;
  message: string;
  errorClass?: string;
}

/**
 * Test a storage plugin connection without saving.
 * POST /storage/test-connection with body { name, config }.
 */
export async function testConnection(
  name: string,
  config: Record<string, unknown>
): Promise<TestConnectionResult> {
  const response = await apiClient.post<TestConnectionResult>(
    '/storage/test-connection',
    { name, config }
  );
  return response.data;
}
