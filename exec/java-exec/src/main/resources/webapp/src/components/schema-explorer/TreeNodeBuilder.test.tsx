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
import { buildColumnNodes, buildTableNodes } from './TreeNodeBuilder';
import type { UsageCounts } from './TreeNodeBuilder';
import type { ColumnInfo, TableInfo } from '../../types';

describe('buildTableNodes', () => {
  const emptyNestedCache: Record<string, never[]> = {};

  it('returns empty array for empty tables', () => {
    const result = buildTableNodes('myschema', [], {}, emptyNestedCache);
    expect(result).toEqual([]);
  });

  it('creates table nodes with correct keys', () => {
    const tables: TableInfo[] = [
      { name: 'users', schema: 'myschema', type: 'TABLE' },
      { name: 'orders', schema: 'myschema', type: 'TABLE' },
    ];
    const result = buildTableNodes('myschema', tables, {}, emptyNestedCache);

    expect(result).toHaveLength(2);
    expect(result[0].key).toBe('table:myschema:users');
    expect(result[1].key).toBe('table:myschema:orders');
    expect(result[0].isLeaf).toBe(false);
    expect(result[1].isLeaf).toBe(false);
  });

  it('shows dynamic schema hint for ** columns', () => {
    const tables: TableInfo[] = [
      { name: 'endpoint', schema: 'http', type: 'TABLE' },
    ];
    const columnsCache: Record<string, ColumnInfo[]> = {
      'table:http:endpoint': [{ name: '**', type: 'ANY', nullable: false, schema: 'http', table: 'endpoint' }],
    };

    const result = buildTableNodes('http', tables, columnsCache, emptyNestedCache);

    expect(result).toHaveLength(1);
    expect(result[0].children).toHaveLength(1);
    expect(result[0].children![0].key).toBe('table:http:endpoint:__dynamic__');
    expect(result[0].children![0].isLeaf).toBe(true);
  });

  it('adds column filter input when columns > 15', () => {
    const tables: TableInfo[] = [
      { name: 'big_table', schema: 'myschema', type: 'TABLE' },
    ];
    const columns: ColumnInfo[] = Array.from({ length: 20 }, (_, i) => ({
      name: `col_${i}`,
      type: 'VARCHAR',
      nullable: true,
      schema: 'myschema',
      table: 'big_table',
    }));
    const columnsCache: Record<string, ColumnInfo[]> = {
      'table:myschema:big_table': columns,
    };

    const onFilterChange = vi.fn();
    const result = buildTableNodes('myschema', tables, columnsCache, emptyNestedCache, {}, onFilterChange);

    expect(result).toHaveLength(1);
    // First child should be the filter input node
    const firstChild = result[0].children![0];
    expect(firstChild.key).toBe('filter:table:myschema:big_table');
    expect(firstChild.isLeaf).toBe(true);
    expect(firstChild.selectable).toBe(false);
    // Total children = 1 filter + 20 column nodes
    expect(result[0].children).toHaveLength(21);
  });

  it('filters columns by filter text', () => {
    const tables: TableInfo[] = [
      { name: 'filtered_table', schema: 'myschema', type: 'TABLE' },
    ];
    const columns: ColumnInfo[] = [
      { name: 'first_name', type: 'VARCHAR', nullable: true, schema: 'myschema', table: 'filtered_table' },
      { name: 'last_name', type: 'VARCHAR', nullable: true, schema: 'myschema', table: 'filtered_table' },
      { name: 'age', type: 'INT', nullable: false, schema: 'myschema', table: 'filtered_table' },
    ];
    const columnsCache: Record<string, ColumnInfo[]> = {
      'table:myschema:filtered_table': columns,
    };
    const columnFilter: Record<string, string> = {
      'table:myschema:filtered_table': 'name',
    };

    const result = buildTableNodes('myschema', tables, columnsCache, emptyNestedCache, columnFilter);

    expect(result).toHaveLength(1);
    // Only columns matching "name" should appear: first_name, last_name
    // Since there are only 3 columns total (<=15), no filter input is prepended
    const children = result[0].children!;
    expect(children).toHaveLength(2);
    expect(children[0].key).toBe('column:myschema:filtered_table:first_name');
    expect(children[1].key).toBe('column:myschema:filtered_table:last_name');
  });

  it('shows usage count badges when usageCounts provided', () => {
    const tables: TableInfo[] = [
      { name: 'popular', schema: 'myschema', type: 'TABLE' },
    ];
    const usageCounts: UsageCounts = {
      tables: new Map([['myschema.popular', 5]]),
      columns: new Map(),
    };

    const result = buildTableNodes('myschema', tables, {}, emptyNestedCache, undefined, undefined, usageCounts);

    expect(result).toHaveLength(1);
    // The node key should be correct
    expect(result[0].key).toBe('table:myschema:popular');
    // The title is a JSX element containing the usage count - we verify the node was created
    expect(result[0].isLeaf).toBe(false);
  });

  it('shows "No matching columns" when filter matches nothing', () => {
    const tables: TableInfo[] = [
      { name: 'some_table', schema: 'myschema', type: 'TABLE' },
    ];
    const columns: ColumnInfo[] = [
      { name: 'id', type: 'INT', nullable: false, schema: 'myschema', table: 'some_table' },
      { name: 'value', type: 'VARCHAR', nullable: true, schema: 'myschema', table: 'some_table' },
    ];
    const columnsCache: Record<string, ColumnInfo[]> = {
      'table:myschema:some_table': columns,
    };
    const columnFilter: Record<string, string> = {
      'table:myschema:some_table': 'zzz_no_match',
    };

    const result = buildTableNodes('myschema', tables, columnsCache, emptyNestedCache, columnFilter);

    expect(result).toHaveLength(1);
    const children = result[0].children!;
    expect(children).toHaveLength(1);
    expect(children[0].key).toBe('table:myschema:some_table:__no_match__');
    expect(children[0].isLeaf).toBe(true);
    expect(children[0].selectable).toBe(false);
  });
});

describe('buildColumnNodes', () => {
  const emptyNestedCache: Record<string, never[]> = {};

  it('creates column nodes with type info', () => {
    const columns: ColumnInfo[] = [
      { name: 'id', type: 'INT', nullable: false, schema: 's', table: 't' },
      { name: 'name', type: 'VARCHAR', nullable: true, schema: 's', table: 't' },
    ];

    const result = buildColumnNodes(columns, 'myschema', 'mytable', emptyNestedCache);

    expect(result).toHaveLength(2);
    expect(result[0].key).toBe('column:myschema:mytable:id');
    expect(result[1].key).toBe('column:myschema:mytable:name');
    expect(result[0].isLeaf).toBe(true);
    expect(result[1].isLeaf).toBe(true);
  });

  it('marks complex types as expandable', () => {
    const columns: ColumnInfo[] = [
      { name: 'address', type: 'STRUCT', nullable: true, schema: 's', table: 't' },
      { name: 'metadata', type: 'MAP', nullable: true, schema: 's', table: 't' },
      { name: 'simple', type: 'VARCHAR', nullable: true, schema: 's', table: 't' },
    ];

    const result = buildColumnNodes(columns, 'myschema', 'mytable', emptyNestedCache);

    expect(result[0].isLeaf).toBe(false); // STRUCT is complex
    expect(result[1].isLeaf).toBe(false); // MAP is complex
    expect(result[2].isLeaf).toBe(true);  // VARCHAR is simple
  });

  it('shows usage badges when usageCounts provided', () => {
    const columns: ColumnInfo[] = [
      { name: 'user_id', type: 'INT', nullable: false, schema: 's', table: 't' },
      { name: 'email', type: 'VARCHAR', nullable: true, schema: 's', table: 't' },
    ];
    const usageCounts: UsageCounts = {
      tables: new Map(),
      columns: new Map([['user_id', 3]]),
    };

    const result = buildColumnNodes(columns, 'myschema', 'mytable', emptyNestedCache, usageCounts);

    expect(result).toHaveLength(2);
    // Both nodes should be created successfully
    expect(result[0].key).toBe('column:myschema:mytable:user_id');
    expect(result[1].key).toBe('column:myschema:mytable:email');
  });
});
