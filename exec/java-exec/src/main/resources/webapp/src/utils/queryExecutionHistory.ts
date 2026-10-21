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

/**
 * Tracks execution history of queries with backend cache IDs.
 * Allows fast lookup of whether a query has cached results available.
 */

export interface ExecutedQueryEntry {
  sql: string;
  cacheId: string;
  executedAt: number;
  rowCount: number;
  duration: number;
  defaultSchema?: string;
}

const HISTORY_KEY = 'drill_query_execution_history';
const MAX_HISTORY_SIZE = 100; // Keep last 100 executed queries

/**
 * Store an executed query in history.
 */
export function recordQueryExecution(
  sql: string,
  cacheId: string,
  rowCount: number,
  duration: number,
  defaultSchema?: string
): void {
  try {
    const history = getExecutionHistory();
    const normalized = normalizeSql(sql);

    // Remove any previous entry for this exact query (avoid duplicates)
    const filtered = history.filter((e) => normalizeSql(e.sql) !== normalized);

    // Add new entry at the front (most recent first)
    const newHistory: ExecutedQueryEntry[] = [
      {
        sql,
        cacheId,
        executedAt: Date.now(),
        rowCount,
        duration,
        defaultSchema,
      },
      ...filtered,
    ];

    // Keep only the most recent N entries
    const trimmed = newHistory.slice(0, MAX_HISTORY_SIZE);

    localStorage.setItem(HISTORY_KEY, JSON.stringify(trimmed));
  } catch (error) {
    console.error('Failed to record query execution:', error);
  }
}

/**
 * Retrieve full execution history (most recent first).
 */
export function getExecutionHistory(): ExecutedQueryEntry[] {
  try {
    const stored = localStorage.getItem(HISTORY_KEY);
    return stored ? JSON.parse(stored) : [];
  } catch (error) {
    console.error('Failed to read query execution history:', error);
    return [];
  }
}

/**
 * Look up a cached result for a specific query.
 * Returns the most recent cache entry for the given SQL.
 */
export function lookupCacheForQuery(sql: string): ExecutedQueryEntry | null {
  const normalized = normalizeSql(sql);
  const history = getExecutionHistory();

  const match = history.find((e) => normalizeSql(e.sql) === normalized);
  return match || null;
}

/**
 * Check if a query has a cached result (without loading full history).
 */
export function hasCachedResult(sql: string): boolean {
  return lookupCacheForQuery(sql) !== null;
}

/**
 * Normalize SQL for comparison (trim, lowercase, single spaces).
 */
function normalizeSql(sql: string): string {
  return sql
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

/**
 * Clear all execution history.
 */
export function clearExecutionHistory(): void {
  try {
    localStorage.removeItem(HISTORY_KEY);
  } catch (error) {
    console.error('Failed to clear execution history:', error);
  }
}
