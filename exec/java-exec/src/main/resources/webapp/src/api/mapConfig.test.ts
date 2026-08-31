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
vi.mock('./client', () => ({ default: { get: mockGet } }));

import { getMapConfig } from './mapConfig';

describe('getMapConfig', () => {
  beforeEach(() => vi.clearAllMocks());

  it('returns the configured tile url', async () => {
    mockGet.mockResolvedValue({
      data: { tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: '© T' },
    });
    await expect(getMapConfig()).resolves.toEqual({
      tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: '© T',
    });
    expect(mockGet).toHaveBeenCalledWith('/api/v1/map/config');
  });

  /**
   * An unreachable config must fall back to the bundled vector basemap. Failing loudly
   * here would break a map that is perfectly usable without tiles.
   */
  it('falls back to no tile server when the request fails', async () => {
    mockGet.mockRejectedValue(new Error('network'));
    await expect(getMapConfig()).resolves.toEqual({ tileUrl: '', attribution: '' });
  });

  it('tolerates a response missing fields', async () => {
    mockGet.mockResolvedValue({ data: {} });
    await expect(getMapConfig()).resolves.toEqual({ tileUrl: '', attribution: '' });
  });

  /**
   * A tile URL that arrived without an attribution should not be used: the server
   * rejects that combination, so seeing it means something bypassed validation, and
   * rendering it would credit nobody.
   */
  it('ignores a tile url that arrived with no attribution', async () => {
    mockGet.mockResolvedValue({ data: { tileUrl: 'https://t/{z}/{x}/{y}.png' } });
    await expect(getMapConfig()).resolves.toEqual({ tileUrl: '', attribution: '' });
  });
});
