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
import {
  listConfigColumnRefs,
  findMissingColumnRefs,
  groupMissingByColumn,
} from './vizColumnDeps';

describe('listConfigColumnRefs', () => {
  it('returns empty list for undefined / empty config', () => {
    expect(listConfigColumnRefs(undefined)).toEqual([]);
    expect(listConfigColumnRefs({})).toEqual([]);
  });

  it('flattens every populated slot in canonical order', () => {
    const refs = listConfigColumnRefs({
      xAxis: 'ts',
      yAxis: 'value',
      metrics: ['hits', 'errors'],
      dimensions: ['region'],
    });
    expect(refs.map((r) => r.slot)).toEqual(['x-axis', 'y-axis', 'metric', 'metric', 'dimension']);
    expect(refs.map((r) => r.column)).toEqual(['ts', 'value', 'hits', 'errors', 'region']);
  });

  it('skips empty entries inside metrics/dimensions arrays', () => {
    const refs = listConfigColumnRefs({
      metrics: ['', 'hits'],
      dimensions: [''],
    });
    expect(refs).toEqual([{ slot: 'metric', column: 'hits' }]);
  });
});

describe('findMissingColumnRefs', () => {
  it('returns nothing when columns are empty/undefined (treat as not-yet-known)', () => {
    expect(findMissingColumnRefs({ xAxis: 'ts' }, undefined)).toEqual([]);
    expect(findMissingColumnRefs({ xAxis: 'ts' }, [])).toEqual([]);
  });

  it('returns nothing when every referenced column is present', () => {
    expect(
      findMissingColumnRefs(
        { xAxis: 'ts', metrics: ['hits'] },
        ['ts', 'hits', 'extra'],
      ),
    ).toEqual([]);
  });

  it('returns each (slot, column) that is missing — including duplicates per slot', () => {
    const missing = findMissingColumnRefs(
      { xAxis: 'ts', metrics: ['hits', 'errors'] },
      ['ts', 'hits'],
    );
    expect(missing).toEqual([{ slot: 'metric', column: 'errors' }]);
  });

  it('detects xAxis and yAxis removals', () => {
    const missing = findMissingColumnRefs(
      { xAxis: 'ts', yAxis: 'val' },
      ['day'],
    );
    expect(missing).toHaveLength(2);
  });
});

describe('groupMissingByColumn', () => {
  it('groups distinct slots that share a column', () => {
    const grouped = groupMissingByColumn([
      { slot: 'x-axis', column: 'ts' },
      { slot: 'metric', column: 'ts' },
      { slot: 'metric', column: 'errors' },
    ]);
    expect(grouped).toEqual([
      { column: 'ts', slots: ['x-axis', 'metric'] },
      { column: 'errors', slots: ['metric'] },
    ]);
  });

  it('deduplicates a slot listed twice for the same column', () => {
    const grouped = groupMissingByColumn([
      { slot: 'metric', column: 'ts' },
      { slot: 'metric', column: 'ts' },
    ]);
    expect(grouped).toEqual([{ column: 'ts', slots: ['metric'] }]);
  });
});
