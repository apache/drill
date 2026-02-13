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

import type { QueryResult } from '../types';

const DEFAULT_TTL_MS = 15 * 60 * 1000; // 15 minutes

interface CachedResult {
  result: QueryResult;
  executionTime: number;
  cachedAt: number;
}

const cache = new Map<string, CachedResult>();

function cacheKey(tabId: string, projectId?: string): string {
  return `${projectId || 'global'}:${tabId}`;
}

export function cacheResults(
  tabId: string,
  result: QueryResult,
  executionTime: number,
  projectId?: string,
): void {
  cache.set(cacheKey(tabId, projectId), {
    result,
    executionTime,
    cachedAt: Date.now(),
  });
}

export function getCachedResults(
  tabId: string,
  projectId?: string,
  ttlMs: number = DEFAULT_TTL_MS,
): { result: QueryResult; executionTime: number } | null {
  const entry = cache.get(cacheKey(tabId, projectId));
  if (!entry) {
    return null;
  }
  if (Date.now() - entry.cachedAt > ttlMs) {
    cache.delete(cacheKey(tabId, projectId));
    return null;
  }
  return { result: entry.result, executionTime: entry.executionTime };
}

export function invalidateCache(tabId: string, projectId?: string): void {
  cache.delete(cacheKey(tabId, projectId));
}

export function clearProjectCache(projectId?: string): void {
  const prefix = `${projectId || 'global'}:`;
  for (const key of cache.keys()) {
    if (key.startsWith(prefix)) {
      cache.delete(key);
    }
  }
}
