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
import type { PersistedTab } from './workspacePersistence';
import type { ServerTab } from '../api/tabs';

/**
 * ServerTab and PersistedTab disagree on the lock field name — `locked` against the
 * older `isLocked` — so the mapping has to be explicit or a lock disappears on the way
 * in, and a locked tab would look deletable.
 */
function fromServer(tab: ServerTab): PersistedTab {
  return {
    id: tab.id,
    name: tab.name,
    sql: tab.sql,
    defaultSchema: tab.defaultSchema,
    cacheId: tab.cacheId,
    vizIds: tab.vizIds,
    isLocked: tab.locked,
    lockReason: tab.lockReason,
    lockType: tab.lockType,
    hidden: tab.hidden,
    updatedAt: tab.updatedAt,
  };
}

/**
 * Merges the browser's tabs with the server's into the set the session should show.
 *
 * Union by id; where a tab exists on both sides the newer `updatedAt` wins. Local-only
 * tabs stay — they are unpromoted drafts that simply have no server copy yet.
 *
 * Local ordering is preserved and server-only tabs are appended, so a reload does not
 * reshuffle the tab strip the user was looking at.
 *
 * An empty server list therefore returns the local set untouched, which is what makes
 * an unreachable drillbit non-destructive: `listTabs` degrades to `[]` and the user's
 * working set survives.
 *
 * See docs/dev/TabPersistence.md.
 */
export function reconcileTabs(local: PersistedTab[], server: ServerTab[]): PersistedTab[] {
  const merged = new Map<string, PersistedTab>();
  for (const tab of local) {
    merged.set(tab.id, tab);
  }

  for (const tab of server) {
    const existing = merged.get(tab.id);
    if (!existing) {
      merged.set(tab.id, fromServer(tab));
      continue;
    }
    // A local tab written before updatedAt existed has none. Treat that as older than
    // any server copy: the server record was definitely promoted at some point, while
    // an undated local one may be a stale leftover.
    const localUpdatedAt = existing.updatedAt ?? -1;
    if (tab.updatedAt > localUpdatedAt) {
      merged.set(tab.id, fromServer(tab));
    }
  }

  return Array.from(merged.values());
}
