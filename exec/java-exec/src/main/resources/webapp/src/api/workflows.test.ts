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
vi.mock('./client', () => ({ default: { get: mockGet, post: mockPost, put: mockPut, delete: mockDelete } }));

import { listWorkflows, dryRunWorkflow, validationErrors, setWorkflowEnabled } from './workflows';

describe('workflows api', () => {
  beforeEach(() => vi.clearAllMocks());

  it('lists by project', async () => {
    mockGet.mockResolvedValue({ data: [{ id: 'w1' }] });
    expect(await listWorkflows('p1')).toEqual([{ id: 'w1' }]);
    expect(mockGet).toHaveBeenCalledWith('/api/v1/workflows', { params: { projectId: 'p1' } });
  });

  it('dry-runs saved vs unsaved', async () => {
    mockPost.mockResolvedValue({ data: { actions: [] } });
    await dryRunWorkflow({ id: 'w1' } as never);
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/workflows/w1/dry-run');
    await dryRunWorkflow({ name: 'x' } as never);
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/workflows/dry-run', { name: 'x' });
  });

  it('toggles enabled', async () => {
    mockPost.mockResolvedValue({ data: {} });
    await setWorkflowEnabled('w1', true);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/workflows/w1/enabled', { enabled: true });
  });

  it('extracts 422 errors', () => {
    const err = { isAxiosError: true, response: { status: 422, data: { errors: [{ path: 'name', message: 'x' }] } } };
    expect(validationErrors(err)).toEqual([{ path: 'name', message: 'x' }]);
    expect(validationErrors(new Error('no'))).toBeNull();
  });
});
