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
  } catch (err) {
    // Degrading to empty is right for a transient failure, but it also makes a real
    // server error look exactly like "this project has no tabs" — which is a very
    // confusing way to lose a feature. Say something for an actual HTTP response.
    const status = (err as { response?: { status?: number } })?.response?.status;
    if (status) {
      console.error(`Failed to list tabs (HTTP ${status}); showing none.`, err);
    }
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

/** One message in a tab's Prospector conversation, as stored server-side. */
export interface StoredChatMessage {
  role: string;
  content?: string;
  [key: string]: unknown;
}

/**
 * A tab's Prospector conversation. Degrades to an empty list on failure: a tab that
 * has never been talked to and an unreachable drillbit look the same to the caller,
 * and in both cases the local copy is what the panel shows.
 */
export async function getConversation(tabId: string): Promise<StoredChatMessage[]> {
  try {
    const response = await apiClient.get<{ messages: StoredChatMessage[] }>(
      `${TABS_BASE}/${tabId}/conversation`,
    );
    return response.data.messages ?? [];
  } catch {
    return [];
  }
}

/** Outcome of a conversation write. */
export type ConversationWriteResult = 'ok' | 'too-large' | 'failed';

/**
 * Replaces a tab's conversation server-side.
 *
 * Reports rather than throws, because the local copy has already been written and the
 * conversation is not at risk — but 'too-large' is worth telling the user about: the
 * server caps conversations at 512 KB (ZooKeeper's znode limit), and past that point
 * this thread has silently stopped syncing to other devices.
 */
export async function putConversation(
  tabId: string,
  messages: StoredChatMessage[],
): Promise<ConversationWriteResult> {
  try {
    await apiClient.put(`${TABS_BASE}/${tabId}/conversation`, { messages });
    return 'ok';
  } catch (err) {
    const status = (err as { response?: { status?: number } })?.response?.status;
    return status === 413 ? 'too-large' : 'failed';
  }
}
