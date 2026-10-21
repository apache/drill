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
import { describe, it, expect } from 'vitest';
import { parseSqlUsage } from './sqlUsageParser';

describe('parseSqlUsage', () => {
  it('returns empty maps for empty input', () => {
    const result = parseSqlUsage([]);
    expect(result.tables.size).toBe(0);
    expect(result.columns.size).toBe(0);
  });

  it('extracts table names from FROM clause', () => {
    const result = parseSqlUsage(['SELECT id FROM employees']);
    expect(result.tables.get('employees')).toBe(1);
  });

  it('extracts table names from JOIN clause', () => {
    const result = parseSqlUsage([
      'SELECT e.id FROM employees e JOIN departments d ON e.dept_id = d.id',
    ]);
    expect(result.tables.get('employees')).toBe(1);
    expect(result.tables.get('departments')).toBe(1);
  });

  it('handles backtick-quoted identifiers', () => {
    const result = parseSqlUsage(['SELECT `name` FROM `my_table`']);
    expect(result.tables.get('my_table')).toBe(1);
    expect(result.columns.has('name')).toBe(true);
  });

  it('extracts column names from SELECT clause', () => {
    const result = parseSqlUsage(['SELECT first_name, last_name FROM users']);
    expect(result.columns.get('first_name')).toBe(1);
    expect(result.columns.get('last_name')).toBe(1);
  });

  it('extracts column names from WHERE clause', () => {
    const result = parseSqlUsage([
      'SELECT id FROM users WHERE status = 1 LIMIT 10',
    ]);
    expect(result.columns.has('status')).toBe(true);
  });

  it('ignores SQL keywords', () => {
    const result = parseSqlUsage([
      'SELECT DISTINCT name FROM users WHERE active IS NOT NULL',
    ]);
    // Keywords like DISTINCT, IS, NOT, NULL should not appear as columns
    expect(result.columns.has('distinct')).toBe(false);
    expect(result.columns.has('is')).toBe(false);
    expect(result.columns.has('not')).toBe(false);
    expect(result.columns.has('null')).toBe(false);
    // Actual columns should be present
    expect(result.columns.has('name')).toBe(true);
    expect(result.columns.has('active')).toBe(true);
  });

  it('counts multiple occurrences across queries', () => {
    const result = parseSqlUsage([
      'SELECT id FROM users',
      'SELECT name FROM users',
      'SELECT id FROM orders',
    ]);
    expect(result.tables.get('users')).toBe(2);
    expect(result.tables.get('orders')).toBe(1);
  });

  it('handles schema-qualified table names', () => {
    const result = parseSqlUsage(['SELECT id FROM dfs.tmp.my_table']);
    expect(result.tables.get('dfs.tmp.my_table')).toBe(1);
  });

  it('ignores information_schema tables', () => {
    const result = parseSqlUsage([
      'SELECT * FROM information_schema.columns',
    ]);
    expect(result.tables.has('information_schema.columns')).toBe(false);
  });
});
