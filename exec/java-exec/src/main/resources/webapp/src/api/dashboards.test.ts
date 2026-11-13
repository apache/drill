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

// vi.hoisted ensures the mock is available when vi.mock factory runs (hoisted)
const mockPost = vi.hoisted(() => vi.fn());

vi.mock('./client', () => ({
  default: {
    get: vi.fn(),
    post: mockPost,
    put: vi.fn(),
    delete: vi.fn(),
  },
}));

import { uploadImage } from './dashboards';

describe('uploadImage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends a multipart form data POST request', async () => {
    const responseData = {
      url: '/api/v1/dashboards/images/uuid-123.png',
      filename: 'photo.png',
    };
    mockPost.mockResolvedValue({ data: responseData });

    const file = new File(['image-data'], 'photo.png', { type: 'image/png' });
    const result = await uploadImage(file);

    expect(mockPost).toHaveBeenCalledTimes(1);

    const [url, body, config] = mockPost.mock.calls[0];
    expect(url).toBe('/api/v1/dashboards/upload-image');
    expect(body).toBeInstanceOf(FormData);
    expect(body.get('file')).toBe(file);
    expect(config.headers['Content-Type']).toBe('multipart/form-data');

    expect(result).toEqual(responseData);
  });

  it('propagates errors from the API client', async () => {
    mockPost.mockRejectedValue(new Error('Server error'));

    const file = new File(['data'], 'test.png', { type: 'image/png' });
    await expect(uploadImage(file)).rejects.toThrow('Server error');
  });
});
