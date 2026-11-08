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
import type { ChatMessage, ConversationRecord } from '../types/ai';

const projectBase = (projectId: string) => `/api/v1/projects/${projectId}/conversations`;

export interface ConversationDetail {
  record: ConversationRecord;
  messages: ChatMessage[];
}

export interface SaveMessagesResult {
  status: 'ok' | 'too-large' | 'conflict';
  /** The new version, present only when status is 'ok'. */
  version?: number;
}

export async function listConversations(projectId: string): Promise<ConversationRecord[]> {
  const response = await apiClient.get<{ conversations: ConversationRecord[] }>(
    projectBase(projectId),
  );
  return response.data.conversations;
}

export async function createConversation(
  projectId: string,
  name?: string,
): Promise<ConversationRecord> {
  const response = await apiClient.post<ConversationRecord>(
    projectBase(projectId),
    name === undefined ? {} : { name },
  );
  return response.data;
}

/** Errors propagate rather than degrading to an empty list: the server is the source of truth. */
export async function getConversation(projectId: string, cid: string): Promise<ConversationDetail> {
  const response = await apiClient.get<ConversationDetail>(`${projectBase(projectId)}/${cid}`);
  return response.data;
}

export async function renameConversation(
  projectId: string,
  cid: string,
  name: string,
): Promise<ConversationRecord> {
  const response = await apiClient.put<ConversationRecord>(`${projectBase(projectId)}/${cid}`, {
    name,
  });
  return response.data;
}

/**
 * Replaces a conversation's messages, guarded by the version last read. Resolves rather
 * than throws on 413 (over the 512 KB cap) and 409 (another device wrote first) so the
 * caller can tell the user which happened; any other failure rejects.
 */
export async function saveConversationMessages(
  projectId: string,
  cid: string,
  messages: ChatMessage[],
  version: number,
): Promise<SaveMessagesResult> {
  try {
    const response = await apiClient.put<{ version: number }>(
      `${projectBase(projectId)}/${cid}/messages`,
      { messages, version },
    );
    return { status: 'ok', version: response.data.version };
  } catch (err) {
    const status = (err as { response?: { status?: number } })?.response?.status;
    if (status === 413) {
      return { status: 'too-large' };
    }
    if (status === 409) {
      return { status: 'conflict' };
    }
    throw err;
  }
}

export async function deleteConversation(projectId: string, cid: string): Promise<void> {
  await apiClient.delete(`${projectBase(projectId)}/${cid}`);
}

/** The caller's personal project, created on first use; home for conversations outside a project. */
export async function getScratchProjectId(): Promise<string> {
  const response = await apiClient.get<{ id: string }>('/api/v1/projects/scratch');
  return response.data.id;
}
