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

const mocks = vi.hoisted(() => ({ post: vi.fn(), delete: vi.fn() }));

vi.mock('./client', () => ({
  default: { get: vi.fn(), put: vi.fn(), post: mocks.post, delete: mocks.delete },
}));

import {
  uploadProjectFile,
  deleteProjectFile,
  downloadProjectFileUrl,
  UploadCollisionError,
  UploadNotConfiguredError,
} from './uploads';

describe('uploadProjectFile', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.post.mockResolvedValue({ data: { name: 'a.csv', path: '/p/a.csv' } });
  });

  it('posts the file as a multipart part named file, without a forced JSON content type', async () => {
    const file = new File(['x'], 'a.csv');
    await uploadProjectFile('proj 1', file);
    const [url, body, config] = mocks.post.mock.calls[0];
    expect(url).toBe('/api/v1/projects/proj%201/files');
    expect(body).toBeInstanceOf(FormData);
    expect(body.get('file')).toBe(file);
    expect(config.headers['Content-Type']).toBeUndefined();
    expect('Content-Type' in config.headers).toBe(true);
  });

  it('sends overwrite only when asked', async () => {
    const file = new File(['x'], 'a.csv');
    await uploadProjectFile('p', file);
    await uploadProjectFile('p', file, { overwrite: false });
    await uploadProjectFile('p', file, { overwrite: true });
    expect(mocks.post.mock.calls[0][2].params).toBeUndefined();
    expect(mocks.post.mock.calls[1][2].params).toBeUndefined();
    expect(mocks.post.mock.calls[2][2].params).toEqual({ overwrite: true });
  });
});

describe('uploadProjectFile 409 classification', () => {
  const file = new File(['x'], 'a.csv');
  const reject = (status: number, data: unknown) =>
    mocks.post.mockRejectedValue({ response: { status, data } });

  it('throws UploadCollisionError for code EXISTS', async () => {
    reject(409, { code: 'EXISTS', message: 'already exists' });
    await expect(uploadProjectFile('p', file)).rejects.toBeInstanceOf(UploadCollisionError);
  });

  it('throws UploadNotConfiguredError for code NOT_CONFIGURED, not a collision', async () => {
    reject(409, { code: 'NOT_CONFIGURED', message: 'No upload folder' });
    const err = await uploadProjectFile('p', file).catch((e) => e);
    expect(err).toBeInstanceOf(UploadNotConfiguredError);
    expect(err).not.toBeInstanceOf(UploadCollisionError);
  });

  it('passes other failures through untouched', async () => {
    const raw = { response: { status: 413, data: { message: 'too big' } } };
    mocks.post.mockRejectedValue(raw);
    await expect(uploadProjectFile('p', file)).rejects.toBe(raw);
  });
});

describe('file name encoding', () => {
  const name = 'my file/#%.csv';
  const enc = 'my%20file%2F%23%25.csv';

  it('encodes the name on delete', async () => {
    mocks.delete.mockResolvedValue({});
    await deleteProjectFile('p', name);
    expect(mocks.delete).toHaveBeenCalledWith(`/api/v1/projects/p/files/${enc}`);
  });

  it('encodes the name on the download url', () => {
    expect(downloadProjectFileUrl('p', name)).toBe(`/api/v1/projects/p/files/${enc}/content`);
  });
});
