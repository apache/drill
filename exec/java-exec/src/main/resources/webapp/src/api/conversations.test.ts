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
import { describe, it, expect, vi, beforeEach } from 'vitest';

const mockGet = vi.hoisted(() => vi.fn());
const mockPost = vi.hoisted(() => vi.fn());
const mockPut = vi.hoisted(() => vi.fn());
const mockDelete = vi.hoisted(() => vi.fn());

vi.mock('./client', () => ({
  default: { get: mockGet, post: mockPost, put: mockPut, delete: mockDelete },
}));

import {
  listConversations,
  createConversation,
  getConversation,
  renameConversation,
  saveConversationMessages,
  deleteConversation,
  getScratchProjectId,
} from './conversations';

const BASE = '/api/v1/projects/p1/conversations';

describe('conversations api', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('posts to the project-scoped conversations path', async () => {
    mockPost.mockResolvedValue({ data: { id: 'c1' } });
    const rec = await createConversation('p1');
    expect(mockPost).toHaveBeenCalledWith(BASE, {});
    expect(rec).toEqual({ id: 'c1' });
  });

  it('passes the name when creating', async () => {
    mockPost.mockResolvedValue({ data: { id: 'c1' } });
    await createConversation('p1', 'Chat');
    expect(mockPost).toHaveBeenCalledWith(BASE, { name: 'Chat' });
  });

  it('unwraps the conversation list', async () => {
    mockGet.mockResolvedValue({ data: { conversations: [{ id: 'c1' }] } });
    expect(await listConversations('p1')).toEqual([{ id: 'c1' }]);
    expect(mockGet).toHaveBeenCalledWith(BASE);
  });

  it('returns record and messages from getConversation', async () => {
    const data = { record: { id: 'c1' }, messages: [{ role: 'user', content: 'hi' }] };
    mockGet.mockResolvedValue({ data });
    expect(await getConversation('p1', 'c1')).toEqual(data);
    expect(mockGet).toHaveBeenCalledWith(`${BASE}/c1`);
  });

  it('does not swallow a failed read', async () => {
    mockGet.mockRejectedValue({ response: { status: 500 } });
    await expect(getConversation('p1', 'c1')).rejects.toBeDefined();
  });

  it('renames via PUT with the name', async () => {
    mockPut.mockResolvedValue({ data: { id: 'c1', name: 'New' } });
    expect(await renameConversation('p1', 'c1', 'New')).toEqual({ id: 'c1', name: 'New' });
    expect(mockPut).toHaveBeenCalledWith(`${BASE}/c1`, { name: 'New' });
  });

  it('deletes the conversation', async () => {
    mockDelete.mockResolvedValue({});
    await deleteConversation('p1', 'c1');
    expect(mockDelete).toHaveBeenCalledWith(`${BASE}/c1`);
  });

  it('resolves the scratch project id', async () => {
    mockGet.mockResolvedValue({ data: { id: 's1' } });
    expect(await getScratchProjectId()).toBe('s1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/projects/scratch');
  });

  describe('saveConversationMessages', () => {
    const msgs = [{ role: 'user', content: 'hi' }] as never;

    it('sends the version with a message save and returns the new one', async () => {
      mockPut.mockResolvedValue({ data: { version: 4 } });
      const result = await saveConversationMessages('p1', 'c1', msgs, 3);
      expect(mockPut).toHaveBeenCalledWith(`${BASE}/c1/messages`, { messages: msgs, version: 3 });
      expect(result).toEqual({ status: 'ok', version: 4 });
    });

    it('maps a 413 to too-large rather than throwing', async () => {
      mockPut.mockRejectedValue({ response: { status: 413 } });
      expect(await saveConversationMessages('p1', 'c1', msgs, 3)).toEqual({ status: 'too-large' });
    });

    it('maps a 409 to conflict rather than throwing', async () => {
      mockPut.mockRejectedValue({ response: { status: 409 } });
      expect(await saveConversationMessages('p1', 'c1', msgs, 3)).toEqual({ status: 'conflict' });
    });

    it('rejects on other failures', async () => {
      mockPut.mockRejectedValue({ response: { status: 500 } });
      await expect(saveConversationMessages('p1', 'c1', msgs, 3)).rejects.toBeDefined();
    });
  });
});
