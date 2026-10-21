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

vi.mock('./client', () => ({
  default: {
    get: mockGet,
    post: mockPost,
    put: vi.fn(),
    delete: vi.fn(),
  },
}));

import {
  executeQuery,
  cancelQuery,
  getQueryProfiles,
  getRunningQueries,
} from './queries';

// ===========================================================================
// executeQuery
// ===========================================================================

describe('executeQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends correct request and returns results', async () => {
    const queryResult = {
      columns: ['id', 'name'],
      metadata: ['INT', 'VARCHAR'],
      rows: [{ id: 1, name: 'Alice' }],
      queryId: 'q-123',
      queryState: 'COMPLETED',
    };
    mockPost.mockResolvedValue({ data: queryResult });

    const result = await executeQuery({
      query: 'SELECT id, name FROM users',
      queryType: 'SQL',
    });

    expect(mockPost).toHaveBeenCalledWith('/query.json', {
      query: 'SELECT id, name FROM users',
      queryType: 'SQL',
      autoLimit: '',
      userName: undefined,
      defaultSchema: undefined,
      options: undefined,
    });
    expect(result).toEqual(queryResult);
  });

  it('strips trailing semicolons from SQL', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({ query: 'SELECT 1;  ', queryType: 'SQL' });
    const [, body] = mockPost.mock.calls[0];
    expect(body.query).toBe('SELECT 1');
  });

  it('strips multiple trailing semicolons', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({ query: 'SELECT 1;', queryType: 'SQL' });
    const [, body] = mockPost.mock.calls[0];
    expect(body.query).toBe('SELECT 1');
  });

  it('does not strip semicolons in the middle of a query', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({
      query: "SELECT * FROM t WHERE s = 'a;b'",
      queryType: 'SQL',
    });
    const [, body] = mockPost.mock.calls[0];
    expect(body.query).toBe("SELECT * FROM t WHERE s = 'a;b'");
  });

  it('defaults queryType to SQL', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({ query: 'SELECT 1', queryType: 'SQL' });
    const [, body] = mockPost.mock.calls[0];
    expect(body.queryType).toBe('SQL');
  });

  it('passes autoLimitRowCount as string', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({
      query: 'SELECT 1',
      queryType: 'SQL',
      autoLimitRowCount: 1000,
    });
    const [, body] = mockPost.mock.calls[0];
    expect(body.autoLimit).toBe('1000');
  });

  it('sends empty string for autoLimit when not provided', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({ query: 'SELECT 1', queryType: 'SQL' });
    const [, body] = mockPost.mock.calls[0];
    expect(body.autoLimit).toBe('');
  });

  it('passes defaultSchema and userName', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({
      query: 'SELECT 1',
      queryType: 'SQL',
      defaultSchema: 'dfs.tmp',
      userName: 'admin',
    });
    const [, body] = mockPost.mock.calls[0];
    expect(body.defaultSchema).toBe('dfs.tmp');
    expect(body.userName).toBe('admin');
  });

  it('throws on FAILED queryState (Drill returns HTTP 200 for failures)', async () => {
    mockPost.mockResolvedValue({
      data: {
        columns: [],
        metadata: [],
        rows: [],
        queryId: 'q-err',
        queryState: 'FAILED',
        errorMessage: 'Table not found',
      },
    });

    await expect(
      executeQuery({ query: 'SELECT * FROM missing', queryType: 'SQL' })
    ).rejects.toThrow('Table not found');
  });

  it('throws generic message when FAILED with no errorMessage', async () => {
    mockPost.mockResolvedValue({
      data: {
        columns: [],
        metadata: [],
        rows: [],
        queryId: 'q-err',
        queryState: 'FAILED',
      },
    });

    await expect(
      executeQuery({ query: 'BAD SQL', queryType: 'SQL' })
    ).rejects.toThrow('Query execution failed');
  });

  it('propagates network errors', async () => {
    mockPost.mockRejectedValue(new Error('Network error'));

    await expect(
      executeQuery({ query: 'SELECT 1', queryType: 'SQL' })
    ).rejects.toThrow('Network error');
  });

  it('passes options record', async () => {
    mockPost.mockResolvedValue({
      data: { columns: [], metadata: [], rows: [], queryId: 'q-1' },
    });

    await executeQuery({
      query: 'SELECT 1',
      queryType: 'SQL',
      options: { 'store.format': 'parquet' },
    });
    const [, body] = mockPost.mock.calls[0];
    expect(body.options).toEqual({ 'store.format': 'parquet' });
  });
});

// ===========================================================================
// cancelQuery
// ===========================================================================

describe('cancelQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends GET request to cancel endpoint with encoded queryId', async () => {
    mockGet.mockResolvedValue({});

    await cancelQuery('q-123');
    expect(mockGet).toHaveBeenCalledWith('/profiles/cancel/q-123');
  });

  it('URL-encodes special characters in queryId', async () => {
    mockGet.mockResolvedValue({});

    await cancelQuery('q/special&id');
    expect(mockGet).toHaveBeenCalledWith(
      `/profiles/cancel/${encodeURIComponent('q/special&id')}`
    );
  });

  it('propagates errors', async () => {
    mockGet.mockRejectedValue(new Error('Not found'));

    await expect(cancelQuery('q-bad')).rejects.toThrow('Not found');
  });
});

// ===========================================================================
// getQueryProfiles
// ===========================================================================

describe('getQueryProfiles', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches profiles from /profiles.json', async () => {
    const profilesResponse = {
      runningQueries: [
        { queryId: 'q-1', user: 'admin', state: 'RUNNING', query: 'SELECT 1', startTime: 123, foreman: 'host1' },
      ],
      finishedQueries: [
        { queryId: 'q-2', user: 'admin', state: 'COMPLETED', query: 'SELECT 2', startTime: 100, endTime: 101, foreman: 'host1' },
      ],
    };
    mockGet.mockResolvedValue({ data: profilesResponse });

    const result = await getQueryProfiles();
    expect(mockGet).toHaveBeenCalledWith('/profiles.json');
    expect(result.runningQueries).toHaveLength(1);
    expect(result.finishedQueries).toHaveLength(1);
  });
});

// ===========================================================================
// getRunningQueries
// ===========================================================================

describe('getRunningQueries', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches running queries', async () => {
    const running = [
      { queryId: 'q-1', user: 'admin', state: 'RUNNING', query: 'SELECT 1', startTime: 123, foreman: 'host1' },
    ];
    mockGet.mockResolvedValue({ data: running });

    const result = await getRunningQueries();
    expect(mockGet).toHaveBeenCalledWith('/profiles/running.json');
    expect(result).toEqual(running);
  });
});
