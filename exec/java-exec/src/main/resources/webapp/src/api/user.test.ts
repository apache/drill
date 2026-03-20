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

vi.mock('./client', () => ({
  default: {
    get: mockGet,
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  },
}));

import { getCurrentUser } from './user';

describe('getCurrentUser', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls /api/v1/user/me', async () => {
    mockGet.mockResolvedValue({
      data: { username: 'admin', isAdmin: true, authEnabled: true },
    });
    await getCurrentUser();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/user/me');
  });

  it('returns user info with correct shape', async () => {
    const userInfo = { username: 'alice', isAdmin: false, authEnabled: true };
    mockGet.mockResolvedValue({ data: userInfo });

    const result = await getCurrentUser();
    expect(result).toEqual(userInfo);
    expect(result.username).toBe('alice');
    expect(result.isAdmin).toBe(false);
    expect(result.authEnabled).toBe(true);
  });

  it('propagates errors', async () => {
    mockGet.mockRejectedValue(new Error('Unauthorized'));
    await expect(getCurrentUser()).rejects.toThrow('Unauthorized');
  });
});
