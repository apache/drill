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
import { reconcileTabs } from './tabReconcile';
import type { PersistedTab } from './workspacePersistence';
import type { ServerTab } from '../api/tabs';

function local(id: string, sql: string, updatedAt: number): PersistedTab {
  return { id, name: `local ${id}`, sql, updatedAt };
}

function server(id: string, sql: string, updatedAt: number, extra: Partial<ServerTab> = {}): ServerTab {
  return {
    id, name: `server ${id}`, sql, updatedAt, createdAt: 0, hidden: false, ...extra,
  };
}

describe('reconcileTabs', () => {
  it('unions local-only and server-only tabs', () => {
    const out = reconcileTabs([local('a', 'SELECT 1', 100)], [server('b', 'SELECT 2', 100)]);
    expect(out.map((t) => t.id).sort()).toEqual(['a', 'b']);
  });

  it('prefers whichever copy was updated most recently', () => {
    const out = reconcileTabs([local('a', 'SELECT local', 200)], [server('a', 'SELECT server', 100)]);
    expect(out).toHaveLength(1);
    expect(out[0].sql).toBe('SELECT local');
  });

  it('takes the server copy when it is newer', () => {
    const out = reconcileTabs([local('a', 'SELECT local', 100)], [server('a', 'SELECT server', 200)]);
    expect(out).toHaveLength(1);
    expect(out[0].sql).toBe('SELECT server');
  });

  // The drillbit being unreachable must not wipe the user's working set.
  it('returns the local set unchanged when the server list is empty', () => {
    const localTabs = [local('a', 'SELECT 1', 100)];
    expect(reconcileTabs(localTabs, [])).toEqual(localTabs);
  });

  it('carries the hidden flag through from the server copy', () => {
    const out = reconcileTabs([], [server('a', 'SELECT 1', 100, { hidden: true })]);
    expect(out[0].hidden).toBe(true);
  });

  /**
   * ServerTab uses `locked`; PersistedTab has always used `isLocked`. The mapping has
   * to happen here or a lock silently disappears on the way in.
   */
  it('maps the server locked flag onto isLocked', () => {
    const out = reconcileTabs([], [
      server('a', 'SELECT 1', 100, { locked: true, lockReason: 'API endpoint active', lockType: 'api' }),
    ]);
    expect(out[0].isLocked).toBe(true);
    expect(out[0].lockReason).toBe('API endpoint active');
    expect(out[0].lockType).toBe('api');
  });

  it('keeps vizIds from the server copy', () => {
    const out = reconcileTabs([], [server('a', 'SELECT 1', 100, { vizIds: ['v1', 'v2'] })]);
    expect(out[0].vizIds).toEqual(['v1', 'v2']);
  });

  /**
   * A local tab written before updatedAt existed has none. Treating that as "very old"
   * rather than NaN means the server copy wins, which is the safer default: the server
   * copy was definitely promoted at some point, the undated local one may be a stale
   * leftover.
   */
  it('lets the server win against a local tab with no updatedAt', () => {
    const undated = { id: 'a', name: 'local a', sql: 'SELECT local' } as PersistedTab;
    const out = reconcileTabs([undated], [server('a', 'SELECT server', 1)]);
    expect(out[0].sql).toBe('SELECT server');
  });

  it('preserves local ordering and appends server-only tabs', () => {
    const out = reconcileTabs(
      [local('a', 'SELECT 1', 100), local('b', 'SELECT 2', 100)],
      [server('c', 'SELECT 3', 100)],
    );
    expect(out.map((t) => t.id)).toEqual(['a', 'b', 'c']);
  });

  it('handles both sides being empty', () => {
    expect(reconcileTabs([], [])).toEqual([]);
  });
});
