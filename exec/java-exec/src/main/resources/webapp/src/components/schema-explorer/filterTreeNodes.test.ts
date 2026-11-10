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
import type { DataNode } from 'antd/es/tree';
import { filterTreeNodes } from './SchemaExplorer';
import { buildDatasetAllowList } from './datasetAllowList';

const allow = buildDatasetAllowList([
  { id: '1', type: 'folder', schema: 'dfs.uploads', table: 'proj-a', label: 'Uploaded files' },
]);

const keys = (nodes: DataNode[]): string[] =>
  nodes.flatMap((n) => [n.key as string, ...keys(n.children ?? [])]);

describe('filterTreeNodes with a folder ref', () => {
  it('keeps a dir node that is an allowed folder', () => {
    const tree: DataNode[] = [{ key: 'dir:dfs.uploads:proj-a' }];
    expect(keys(filterTreeNodes(tree, allow))).toEqual(['dir:dfs.uploads:proj-a']);
  });

  it('keeps a file inside an allowed folder', () => {
    const tree: DataNode[] = [{ key: 'file:dfs.uploads:proj-a/sales.xlsx' }];
    expect(keys(filterTreeNodes(tree, allow))).toEqual(['file:dfs.uploads:proj-a/sales.xlsx']);
  });

  it('drops another project\'s folder and its files', () => {
    const tree: DataNode[] = [
      { key: 'dir:dfs.uploads:proj-b', children: [{ key: 'file:dfs.uploads:proj-b/x.csv' }] },
      { key: 'dir:dfs.uploads:proj-b' },
      { key: 'file:dfs.uploads:proj-b/x.csv' },
    ];
    expect(filterTreeNodes(tree, allow)).toEqual([]);
  });

  it('does not match a sibling folder that shares the allowed name as a prefix', () => {
    const tree: DataNode[] = [
      { key: 'dir:dfs.uploads:proj-a-backup' },
      { key: 'file:dfs.uploads:proj-a-backup/x.csv' },
    ];
    expect(filterTreeNodes(tree, allow)).toEqual([]);
  });

  it('keeps a nested path under an allowed folder', () => {
    const tree: DataNode[] = [
      { key: 'dir:dfs.uploads:proj-a/sub' },
      { key: 'file:dfs.uploads:proj-a/sub/x.csv' },
    ];
    expect(keys(filterTreeNodes(tree, allow))).toEqual([
      'dir:dfs.uploads:proj-a/sub',
      'file:dfs.uploads:proj-a/sub/x.csv',
    ]);
  });

  it('keeps the ancestor chain to the folder and drops other projects beside it', () => {
    const tree: DataNode[] = [
      { key: 'plugin:dfs', children: [
        { key: 'schema:dfs.uploads', children: [
          { key: 'dir:dfs.uploads:proj-a' },
          { key: 'dir:dfs.uploads:proj-b' },
          { key: 'dir:dfs.uploads:proj-a-backup' },
        ] },
        { key: 'schema:dfs.tmp', children: [{ key: 'table:dfs.tmp:t' }] },
      ] },
    ];
    expect(keys(filterTreeNodes(tree, allow))).toEqual([
      'plugin:dfs', 'schema:dfs.uploads', 'dir:dfs.uploads:proj-a',
    ]);
  });

  it('keeps a parent dir of a nested folder and drops its other children', () => {
    const nested = buildDatasetAllowList([
      { id: '2', type: 'folder', schema: 'dfs.uploads', table: 'org/proj-a', label: 'f' },
    ]);
    const tree: DataNode[] = [{ key: 'dir:dfs.uploads:org', children: [
      { key: 'dir:dfs.uploads:org/proj-a' },
      { key: 'dir:dfs.uploads:org/proj-b' },
    ] }];
    expect(keys(filterTreeNodes(tree, nested))).toEqual([
      'dir:dfs.uploads:org', 'dir:dfs.uploads:org/proj-a',
    ]);
  });
});
