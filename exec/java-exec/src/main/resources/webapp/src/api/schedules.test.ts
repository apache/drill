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
  default: {
    get: mockGet,
    post: mockPost,
    put: mockPut,
    delete: mockDelete,
  },
}));

import {
  getSchedules,
  getScheduleForQuery,
  createSchedule,
  updateSchedule,
  renewSchedule,
  deleteSchedule,
  runScheduleNow,
  getSnapshots,
  getScheduleConfig,
} from './schedules';

// ===========================================================================
// getSchedules
// ===========================================================================

describe('getSchedules', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls correct endpoint', async () => {
    mockGet.mockResolvedValue({ data: [] });
    await getSchedules();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/schedules');
  });

  it('extracts array from response when data is an array', async () => {
    const schedules = [{ id: 's1' }, { id: 's2' }];
    mockGet.mockResolvedValue({ data: schedules });
    const result = await getSchedules();
    expect(result).toEqual(schedules);
  });

  it('extracts array from wrapped response with schedules key', async () => {
    const schedules = [{ id: 's1' }];
    mockGet.mockResolvedValue({ data: { schedules } });
    const result = await getSchedules();
    expect(result).toEqual(schedules);
  });

  it('returns empty array when response has no recognized shape', async () => {
    mockGet.mockResolvedValue({ data: { unexpected: 'data' } });
    const result = await getSchedules();
    expect(result).toEqual([]);
  });
});

// ===========================================================================
// getScheduleForQuery
// ===========================================================================

describe('getScheduleForQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls correct endpoint with queryId', async () => {
    mockGet.mockResolvedValue({ data: { id: 's1' } });
    await getScheduleForQuery('q-abc');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/schedules/query/q-abc');
  });

  it('returns schedule data on success', async () => {
    const schedule = { id: 's1', savedQueryId: 'q-abc' };
    mockGet.mockResolvedValue({ data: schedule });
    const result = await getScheduleForQuery('q-abc');
    expect(result).toEqual(schedule);
  });

  it('returns null on 404', async () => {
    mockGet.mockRejectedValue({ response: { status: 404 } });
    const result = await getScheduleForQuery('q-missing');
    expect(result).toBeNull();
  });
});

// ===========================================================================
// createSchedule
// ===========================================================================

describe('createSchedule', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('posts to correct endpoint', async () => {
    const data = { savedQueryId: 'q1', frequency: 'daily' as const };
    const created = { id: 's1', ...data };
    mockPost.mockResolvedValue({ data: created });

    const result = await createSchedule(data);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/schedules', data);
    expect(result).toEqual(created);
  });
});

// ===========================================================================
// updateSchedule
// ===========================================================================

describe('updateSchedule', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('puts to correct endpoint', async () => {
    const data = { enabled: false };
    const updated = { id: 's1', enabled: false };
    mockPut.mockResolvedValue({ data: updated });

    const result = await updateSchedule('s1', data);
    expect(mockPut).toHaveBeenCalledWith('/api/v1/schedules/s1', data);
    expect(result).toEqual(updated);
  });
});

// ===========================================================================
// renewSchedule
// ===========================================================================

describe('renewSchedule', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('posts to renew endpoint', async () => {
    const renewed = { id: 's1', expiresAt: '2026-06-01' };
    mockPost.mockResolvedValue({ data: renewed });

    const result = await renewSchedule('s1');
    expect(mockPost).toHaveBeenCalledWith('/api/v1/schedules/s1/renew');
    expect(result).toEqual(renewed);
  });
});

// ===========================================================================
// deleteSchedule
// ===========================================================================

describe('deleteSchedule', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls delete on correct endpoint', async () => {
    mockDelete.mockResolvedValue({});
    await deleteSchedule('s1');
    expect(mockDelete).toHaveBeenCalledWith('/api/v1/schedules/s1');
  });
});

// ===========================================================================
// runScheduleNow
// ===========================================================================

describe('runScheduleNow', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('posts to run endpoint', async () => {
    const snapshot = { id: 'snap1', scheduleId: 's1' };
    mockPost.mockResolvedValue({ data: snapshot });

    const result = await runScheduleNow('s1');
    expect(mockPost).toHaveBeenCalledWith('/api/v1/schedules/s1/run');
    expect(result).toEqual(snapshot);
  });
});

// ===========================================================================
// getSnapshots
// ===========================================================================

describe('getSnapshots', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls correct endpoint', async () => {
    mockGet.mockResolvedValue({ data: [] });
    await getSnapshots('s1');
    expect(mockGet).toHaveBeenCalledWith('/api/v1/schedules/s1/snapshots');
  });

  it('extracts array from response', async () => {
    const snapshots = [{ id: 'snap1' }];
    mockGet.mockResolvedValue({ data: snapshots });
    const result = await getSnapshots('s1');
    expect(result).toEqual(snapshots);
  });

  it('extracts array from wrapped response with snapshots key', async () => {
    const snapshots = [{ id: 'snap1' }];
    mockGet.mockResolvedValue({ data: { snapshots } });
    const result = await getSnapshots('s1');
    expect(result).toEqual(snapshots);
  });
});

// ===========================================================================
// getScheduleConfig
// ===========================================================================

describe('getScheduleConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches config from correct endpoint', async () => {
    const config = { expirationEnabled: true, expirationDays: 90, warningDaysBeforeExpiry: 14 };
    mockGet.mockResolvedValue({ data: config });
    const result = await getScheduleConfig();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/workflows/config');
    expect(result).toEqual(config);
  });

  it('falls back to defaults on error', async () => {
    mockGet.mockRejectedValue(new Error('Network error'));
    const result = await getScheduleConfig();
    expect(result).toEqual({
      expirationEnabled: true,
      expirationDays: 90,
      warningDaysBeforeExpiry: 14,
    });
  });
});
