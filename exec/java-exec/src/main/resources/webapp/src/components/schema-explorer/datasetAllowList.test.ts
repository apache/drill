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
import { buildDatasetAllowList } from './datasetAllowList';
import type { DatasetRef } from '../../types';

describe('buildDatasetAllowList', () => {
  it('adds a view dataset to tables', () => {
    const datasets: DatasetRef[] = [
      { id: '1', type: 'view', schema: 'dfs.tmp', table: 'myview', label: 'myview' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.tables.get('dfs.tmp')?.has('myview')).toBe(true);
  });

  it('adds a materialized_view dataset to tables', () => {
    const datasets: DatasetRef[] = [
      { id: '2', type: 'materialized_view', schema: 'dfs.tmp', table: 'mymv', label: 'mymv' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.tables.get('dfs.tmp')?.has('mymv')).toBe(true);
  });

  it('adds a table dataset to tables (control)', () => {
    const datasets: DatasetRef[] = [
      { id: '3', type: 'table', schema: 'dfs.tmp', table: 'mytable', label: 'mytable' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.tables.get('dfs.tmp')?.has('mytable')).toBe(true);
  });

  it('adds a plugin dataset to plugins', () => {
    const datasets: DatasetRef[] = [
      { id: '4', type: 'plugin', schema: 'dfs', label: 'dfs' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.plugins.has('dfs')).toBe(true);
  });

  it('adds a schema dataset to schemas', () => {
    const datasets: DatasetRef[] = [
      { id: '5', type: 'schema', schema: 'dfs.tmp', label: 'dfs.tmp' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.schemas.has('dfs.tmp')).toBe(true);
  });

  it('ignores a dataset missing table', () => {
    const datasets: DatasetRef[] = [
      { id: '6', type: 'view', schema: 'dfs.tmp', label: 'incomplete' },
    ];
    const result = buildDatasetAllowList(datasets);
    expect(result.tables.size).toBe(0);
    expect(result.plugins.size).toBe(0);
    expect(result.schemas.size).toBe(0);
  });
});
