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

vi.mock('./client', () => ({
  default: {
    get: mockGet,
    post: mockPost,
    put: mockPut,
    delete: vi.fn(),
  },
}));

import {
  transpileSql,
  formatSql,
  convertDataType,
  getAiStatus,
  getAiConfig,
  updateAiConfig,
  testAiConfig,
  getAiProviders,
} from './ai';

// ===========================================================================
// transpileSql
// ===========================================================================

describe('transpileSql', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends correct request and returns transpiled SQL', async () => {
    mockPost.mockResolvedValue({
      data: { sql: 'SELECT * FROM dfs.`data.csv`', success: true },
    });

    const result = await transpileSql('SELECT * FROM data');
    expect(mockPost).toHaveBeenCalledWith('/api/v1/transpile', {
      sql: 'SELECT * FROM data',
      sourceDialect: 'mysql',
      targetDialect: 'drill',
      schemas: undefined,
    });
    expect(result).toBe('SELECT * FROM dfs.`data.csv`');
  });

  it('uses default dialects (mysql -> drill)', async () => {
    mockPost.mockResolvedValue({
      data: { sql: 'SELECT 1', success: true },
    });

    await transpileSql('SELECT 1');
    const [, body] = mockPost.mock.calls[0];
    expect(body.sourceDialect).toBe('mysql');
    expect(body.targetDialect).toBe('drill');
  });

  it('passes custom dialects', async () => {
    mockPost.mockResolvedValue({
      data: { sql: 'SELECT 1', success: true },
    });

    await transpileSql('SELECT 1', 'postgres', 'bigquery');
    const [, body] = mockPost.mock.calls[0];
    expect(body.sourceDialect).toBe('postgres');
    expect(body.targetDialect).toBe('bigquery');
  });

  it('passes schema metadata', async () => {
    mockPost.mockResolvedValue({
      data: { sql: 'SELECT 1', success: true },
    });

    const schemas = [
      {
        name: 'dfs',
        tables: [{ name: 'users', columns: ['id', 'name'] }],
      },
    ];
    await transpileSql('SELECT 1', 'mysql', 'drill', schemas);
    const [, body] = mockPost.mock.calls[0];
    expect(body.schemas).toEqual(schemas);
  });

  it('returns original SQL on API error (silent fallback)', async () => {
    mockPost.mockRejectedValue(new Error('Network error'));

    const result = await transpileSql('SELECT * FROM my_table');
    expect(result).toBe('SELECT * FROM my_table');
  });

  it('returns original SQL on non-200 response', async () => {
    mockPost.mockRejectedValue({ response: { status: 500 } });

    const result = await transpileSql('SELECT 1');
    expect(result).toBe('SELECT 1');
  });
});

// ===========================================================================
// formatSql
// ===========================================================================

describe('formatSql', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends correct request and returns formatted SQL', async () => {
    const formatted = 'SELECT\n  id,\n  name\nFROM\n  users';
    mockPost.mockResolvedValue({
      data: { sql: formatted, success: true },
    });

    const result = await formatSql('SELECT id, name FROM users');
    expect(mockPost).toHaveBeenCalledWith('/api/v1/transpile/format', {
      sql: 'SELECT id, name FROM users',
    });
    expect(result).toBe(formatted);
  });

  it('returns original SQL on API error (silent fallback)', async () => {
    mockPost.mockRejectedValue(new Error('Server error'));

    const result = await formatSql('SELECT 1');
    expect(result).toBe('SELECT 1');
  });
});

// ===========================================================================
// convertDataType
// ===========================================================================

describe('convertDataType', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends correct request and returns result', async () => {
    const responseData = {
      sql: 'SELECT CAST(price AS INTEGER) AS price FROM products',
      success: true,
      formattedOriginal: 'SELECT\n  price\nFROM\n  products',
    };
    mockPost.mockResolvedValue({ data: responseData });

    const result = await convertDataType(
      'SELECT price FROM products',
      'price',
      'INTEGER'
    );
    expect(mockPost).toHaveBeenCalledWith('/api/v1/transpile/convert-type', {
      sql: 'SELECT price FROM products',
      columnName: 'price',
      dataType: 'INTEGER',
      columns: undefined,
    });
    expect(result).toEqual(responseData);
  });

  it('passes column mapping for star queries', async () => {
    mockPost.mockResolvedValue({
      data: { sql: 'SELECT CAST(id AS VARCHAR) AS id, name FROM users', success: true },
    });

    const columns = { id: 'INTEGER', name: 'VARCHAR' };
    await convertDataType('SELECT * FROM users', 'id', 'VARCHAR', columns);
    const [, body] = mockPost.mock.calls[0];
    expect(body.columns).toEqual(columns);
  });

  it('returns success: false when conversion fails', async () => {
    mockPost.mockResolvedValue({
      data: { sql: '', success: false },
    });

    const result = await convertDataType('SELECT 1', 'col', 'INTEGER');
    expect(result.success).toBe(false);
  });

  it('throws on API error (does NOT silently fallback)', async () => {
    mockPost.mockRejectedValue(new Error('Bad Request'));

    await expect(
      convertDataType('SELECT 1', 'col', 'INTEGER')
    ).rejects.toThrow('Bad Request');
  });
});

// ===========================================================================
// getAiStatus
// ===========================================================================

describe('getAiStatus', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches AI status', async () => {
    mockGet.mockResolvedValue({ data: { enabled: true } });

    const result = await getAiStatus();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/ai/status');
    expect(result).toEqual({ enabled: true });
  });

  it('propagates errors', async () => {
    mockGet.mockRejectedValue(new Error('Unauthorized'));

    await expect(getAiStatus()).rejects.toThrow('Unauthorized');
  });
});

// ===========================================================================
// getAiConfig
// ===========================================================================

describe('getAiConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches AI config', async () => {
    const config = { provider: 'openai', model: 'gpt-4', apiKey: '***' };
    mockGet.mockResolvedValue({ data: config });

    const result = await getAiConfig();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/ai/config');
    expect(result).toEqual(config);
  });
});

// ===========================================================================
// updateAiConfig
// ===========================================================================

describe('updateAiConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends PUT request with config', async () => {
    const update = { provider: 'anthropic', model: 'claude-3', apiKey: 'sk-test' };
    const response = { ...update, apiKey: '***' };
    mockPut.mockResolvedValue({ data: response });

    const result = await updateAiConfig(update);
    expect(mockPut).toHaveBeenCalledWith('/api/v1/ai/config', update);
    expect(result).toEqual(response);
  });
});

// ===========================================================================
// testAiConfig
// ===========================================================================

describe('testAiConfig', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('sends POST to test endpoint', async () => {
    const config = { provider: 'openai', model: 'gpt-4', apiKey: 'sk-test' };
    mockPost.mockResolvedValue({ data: { valid: true, message: 'OK' } });

    const result = await testAiConfig(config);
    expect(mockPost).toHaveBeenCalledWith('/api/v1/ai/config/test', config);
    expect(result).toEqual({ valid: true, message: 'OK' });
  });
});

// ===========================================================================
// getAiProviders
// ===========================================================================

describe('getAiProviders', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches providers from nested response', async () => {
    const providers = [
      { id: 'openai', name: 'OpenAI' },
      { id: 'anthropic', name: 'Anthropic' },
    ];
    mockGet.mockResolvedValue({ data: { providers } });

    const result = await getAiProviders();
    expect(mockGet).toHaveBeenCalledWith('/api/v1/ai/config/providers');
    expect(result).toEqual(providers);
  });
});
