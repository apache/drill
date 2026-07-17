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

export type ViewMode = 'view' | 'materialized_view';

export interface ViewDdlOptions {
  mode: ViewMode;
  schema: string;
  name: string;
  sql: string;
  replace: boolean;
}

/**
 * Format a compound schema name for use in SQL queries.
 * Plugin name stays unquoted; workspace parts are backtick-quoted.
 * e.g. "dfs.test" → "dfs.`test`", "dfs" → "dfs"
 */
export function formatSchema(schema: string): string {
  const parts = schema.split('.');
  if (parts.length <= 1) {
    return schema;
  }
  return parts[0] + '.' + parts.slice(1).map((p) => `\`${p}\``).join('.');
}

/** Strip comments and leading whitespace/parens so the leading keyword can be read. */
function firstKeyword(sql: string): string {
  const stripped = sql
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/--[^\n]*/g, ' ')
    .replace(/^[\s(]+/, '');
  const match = stripped.match(/^[A-Za-z_]+/);
  return match ? match[0].toUpperCase() : '';
}

/**
 * Whether this SQL may become a view. Only SELECT and WITH qualify.
 *
 * Checking the leading keyword is sufficient rather than heuristic: INSERT/DELETE/
 * UPDATE/MERGE are siblings of OrderedQueryOrExpr in the grammar's SqlQueryOrDml,
 * reachable only as the leading production, and WITH is followed strictly by
 * LeafQueryOrExpr — Drill has no writable CTEs. So a statement starting with SELECT or
 * WITH cannot contain DML anywhere inside it.
 *
 * Deliberately does not scan the body: that would reject valid views such as
 * SELECT 'INSERT' AS action FROM audit_log, or SELECT update_time FROM logs.
 */
export function isCreatableAsView(sql: string): boolean {
  const keyword = firstKeyword(sql);
  return keyword === 'SELECT' || keyword === 'WITH';
}

/**
 * A view name is a path, not an identifier — ViewHandler calls removeLeadingSlash on it
 * and getViewPath resolves it against the workspace location, so slashes place the view
 * in a subdirectory. Backticks are rejected because they would break out of the quoting
 * buildViewDdl generates; ".." is rejected because it would escape the workspace.
 */
export function isValidViewName(name: string): boolean {
  if (!/^[A-Za-z0-9_\-./]+$/.test(name)) {
    return false;
  }
  return !name.split('/').includes('..');
}

export function buildViewDdl({ mode, schema, name, sql, replace }: ViewDdlOptions): string {
  const keyword = mode === 'materialized_view' ? 'MATERIALIZED VIEW' : 'VIEW';
  const orReplace = replace ? 'OR REPLACE ' : '';
  const target = `${formatSchema(schema)}.\`${name}\``;
  return `CREATE ${orReplace}${keyword} ${target} AS ${sql.replace(/;\s*$/, '')}`;
}
