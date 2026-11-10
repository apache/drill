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

export interface UploadStatus {
  configured: boolean;
  plugin?: string | null;
  workspace?: string | null;
  maxSizeBytes: number;
}

export interface UploadConfig {
  plugin?: string | null;
  workspace?: string | null;
  maxSizeBytes?: number;
}

export interface SaveUploadConfigResult {
  config: UploadConfig;
  /** Advisory only (e.g. a local file: target on a multi-node cluster); never a failure. */
  warning?: string;
}

export interface UploadedFile {
  name: string;
  path: string;
  schema?: unknown;
}

/** GET /api/v1/uploads/status, open to any authenticated user. */
export async function getUploadStatus(): Promise<UploadStatus> {
  const response = await apiClient.get<UploadStatus>('/api/v1/uploads/status');
  return response.data;
}

/** GET /api/v1/uploads/config, admin only. */
export async function getUploadConfig(): Promise<UploadConfig> {
  const response = await apiClient.get<UploadConfig>('/api/v1/uploads/config');
  return response.data;
}

/** PUT /api/v1/uploads/config, admin only. A 200 may carry a warning string. */
export async function setUploadConfig(cfg: UploadConfig): Promise<SaveUploadConfigResult> {
  const response = await apiClient.put<SaveUploadConfigResult>('/api/v1/uploads/config', cfg);
  return response.data;
}

/**
 * Upload a file into a project. A name collision without opts.overwrite rejects with an
 * axios error whose response.status is 409; use isUploadCollision() to tell it apart from
 * other failures, then retry with { overwrite: true } if the user agrees.
 */
export async function uploadProjectFile(
  projectId: string,
  file: File,
  opts?: { overwrite?: boolean }
): Promise<UploadedFile> {
  const form = new FormData();
  form.append('file', file);
  const response = await apiClient.post<UploadedFile>(
    `/api/v1/projects/${encodeURIComponent(projectId)}/files`,
    form,
    {
      params: opts?.overwrite ? { overwrite: true } : undefined,
      // Clear the client's JSON default so the browser sets multipart with its own boundary.
      headers: { 'Content-Type': undefined },
    }
  );
  return response.data;
}

/** True when an upload failed because a file of that name already exists (HTTP 409). */
export function isUploadCollision(error: unknown): boolean {
  return (error as { response?: { status?: number } } | null)?.response?.status === 409;
}

/** DELETE /api/v1/projects/{id}/files/{name} */
export async function deleteProjectFile(projectId: string, name: string): Promise<void> {
  await apiClient.delete(
    `/api/v1/projects/${encodeURIComponent(projectId)}/files/${encodeURIComponent(name)}`
  );
}

/** URL that serves the file's bytes with a Content-Disposition filename. */
export function downloadProjectFileUrl(projectId: string, name: string): string {
  return `/api/v1/projects/${encodeURIComponent(projectId)}/files/${encodeURIComponent(name)}/content`;
}
