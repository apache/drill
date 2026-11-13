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

// vi.hoisted ensures the mocks exist when the vi.mock factory runs (hoisted).
const mockGet = vi.hoisted(() => vi.fn());
const mockPost = vi.hoisted(() => vi.fn());
const mockPut = vi.hoisted(() => vi.fn());
const mockDelete = vi.hoisted(() => vi.fn());

vi.mock('./client', () => ({
  default: {
    get: mockGet,
    post: mockPost,
    put: mockPut,
    delete: mockDelete,
  },
}));

import { listTabs, createTab, updateTab, deleteTab } from './tabs';

describe('tabs api', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('scopes the list request to a project', async () => {
    mockGet.mockResolvedValue({ data: { tabs: [] } });
    await listTabs('p1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/tabs', { params: { projectId: 'p1' } });
  });

  it('omits projectId entirely for global tabs', async () => {
    mockGet.mockResolvedValue({ data: { tabs: [] } });
    await listTabs(undefined);
    expect(mockGet).toHaveBeenCalledWith('/api/v1/tabs', { params: {} });
  });

  it('unwraps the tabs array from the response envelope', async () => {
    mockGet.mockResolvedValue({ data: { tabs: [{ id: 'a', name: 'A' }] } });
    await expect(listTabs('p1')).resolves.toEqual([{ id: 'a', name: 'A' }]);
  });

  /**
   * The drillbit being unreachable must not wipe the caller's working set, so a
   * failed list degrades to empty rather than throwing. Local tabs survive.
   */
  it('returns an empty list when the request fails', async () => {
    mockGet.mockRejectedValue(new Error('network'));
    await expect(listTabs('p1')).resolves.toEqual([]);
  });

  it('returns the created tab', async () => {
    mockPost.mockResolvedValue({ data: { id: 'x', name: 'T' } });
    await expect(createTab({ name: 'T' })).resolves.toMatchObject({ id: 'x' });
  });

  it('puts updates to the tab id', async () => {
    mockPut.mockResolvedValue({ data: { id: 'x', name: 'Renamed' } });
    await updateTab('x', { name: 'Renamed' });
    expect(mockPut).toHaveBeenCalledWith('/api/v1/tabs/x', { name: 'Renamed' });
  });

  /** A 409 means the tab is locked; callers surface that rather than swallowing it. */
  it('surfaces a 409 from deleting a locked tab', async () => {
    mockDelete.mockRejectedValue({ response: { status: 409 } });
    await expect(deleteTab('t1')).rejects.toMatchObject({ response: { status: 409 } });
  });
});
