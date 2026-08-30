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

const TABS_BASE = '/api/v1/tabs';

/**
 * A query tab as stored on the server. Only promoted tabs exist here — a tab that has
 * never been executed and was never closed with content lives solely in localStorage.
 * See docs/dev/TabPersistence.md.
 */
export interface ServerTab {
  id: string;
  /** Absent for global tabs opened outside a project. */
  projectId?: string;
  name: string;
  sql: string;
  defaultSchema?: string;
  hidden: boolean;
  createdAt: number;
  updatedAt: number;
  vizIds?: string[];
  locked?: boolean;
  lockReason?: string;
  lockType?: 'manual' | 'api';
  pinned?: boolean;
  cacheId?: string;
}

interface TabsResponse {
  tabs: ServerTab[];
}

/**
 * The calling user's tabs for a project, or their global tabs when projectId is
 * omitted. Hidden tabs are included: they are what the project tree lists.
 *
 * Degrades to an empty list when the request fails. The server tier is additive —
 * reconciliation unions it with local tabs — so an unreachable drillbit must leave the
 * user's working set intact rather than emptying it.
 */
export async function listTabs(projectId?: string): Promise<ServerTab[]> {
  try {
    const response = await apiClient.get<TabsResponse>(TABS_BASE, {
      params: projectId ? { projectId } : {},
    });
    return response.data.tabs ?? [];
  } catch {
    return [];
  }
}

/** Promotes a tab to the server. The server assigns owner, timestamps and any missing id. */
export async function createTab(tab: Partial<ServerTab>): Promise<ServerTab> {
  const response = await apiClient.post<ServerTab>(TABS_BASE, tab);
  return response.data;
}

export async function updateTab(id: string, tab: Partial<ServerTab>): Promise<ServerTab> {
  const response = await apiClient.put<ServerTab>(`${TABS_BASE}/${id}`, tab);
  return response.data;
}

/**
 * Permanently removes a tab. Rejects with a 409 when the tab is locked; callers should
 * surface that rather than swallow it, since the user's next move is to unlock or hide.
 */
export async function deleteTab(id: string): Promise<void> {
  await apiClient.delete(`${TABS_BASE}/${id}`);
}
