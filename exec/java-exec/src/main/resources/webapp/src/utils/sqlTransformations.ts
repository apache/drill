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

import type { DashboardFilter } from '../types';

/**
 * Injects WHERE conditions directly into the original SQL for dashboard
 * cross-filtering.  This approach ensures the filter column is resolved
 * against the source table, so it works even when the column is not in the
 * SELECT list.
 *
 * - If the query already has a WHERE clause (at the outermost level), the
 *   conditions are appended with AND.
 * - Otherwise a new WHERE clause is inserted before GROUP BY / HAVING /
 *   ORDER BY / LIMIT / UNION (or at the end of the query).
 *
 * CAST(col AS VARCHAR) is used so the comparison works regardless of the
 * column's actual type (INT, DATE, TIMESTAMP, etc.).
 */
// Allowlist of valid numeric operators — anything else is rejected.
const VALID_NUMERIC_OPS = new Set(['=', '!=', '>', '>=', '<', '<=', 'between']);

// ISO date pattern: YYYY-MM-DD (with optional leading zeros)
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// Column names may only contain word characters, spaces, dots, and hyphens.
// This prevents injecting SQL through a crafted column name.
const SAFE_COLUMN_RE = /^[\w\s.-]+$/;

/**
 * Returns true when `val` is a finite number (integer or float, including
 * negative values and scientific notation like 1e3).
 */
function isNumericLiteral(val: string): boolean {
  return val !== '' && isFinite(Number(val));
}

export function applyDashboardFilters(
  sql: string,
  filters: DashboardFilter[]
): string {
  if (!filters || filters.length === 0) {
    return sql;
  }

  const cleanSql = sql.replace(/;\s*$/, '').trim();

  const conditions: string[] = [];
  for (const f of filters) {
    // -- Validate column name against allowlist pattern --
    if (!f.column || !SAFE_COLUMN_RE.test(f.column)) {
      continue; // skip filters with suspicious column names
    }

    const quotedCol = quoteColumnName(f.column);
    if (f.value === null || f.value === undefined || f.value === 'null') {
      conditions.push(`${quotedCol} IS NULL`);
      continue;
    }

    // Temporal range filter — values must be valid ISO dates
    if (f.isTemporal && f.rangeStart && f.rangeEnd) {
      if (!ISO_DATE_RE.test(f.rangeStart) || !ISO_DATE_RE.test(f.rangeEnd)) {
        continue; // skip filters with non-date temporal values
      }
      const start = f.rangeStart;
      const end = f.rangeEnd;
      if (start === end) {
        conditions.push(`CAST(${quotedCol} AS DATE) = DATE '${start}'`);
      } else {
        conditions.push(`CAST(${quotedCol} AS DATE) BETWEEN DATE '${start}' AND DATE '${end}'`);
      }
      continue;
    }

    // Numeric filter with operator — values must be valid numbers, operator must be allowlisted
    if (f.isNumeric && f.numericOp) {
      if (!VALID_NUMERIC_OPS.has(f.numericOp)) {
        continue; // skip filters with unknown operators
      }
      if (!isNumericLiteral(String(f.value))) {
        continue; // skip filters with non-numeric values
      }
      const num = Number(f.value);
      if (f.numericOp === 'between' && f.numericEnd != null) {
        if (!isNumericLiteral(String(f.numericEnd))) {
          continue;
        }
        const numEnd = Number(f.numericEnd);
        conditions.push(`${quotedCol} BETWEEN ${num} AND ${numEnd}`);
      } else {
        const op = f.numericOp === '!=' ? '<>' : f.numericOp;
        conditions.push(`${quotedCol} ${op} ${num}`);
      }
      continue;
    }

    const escapedValue = String(f.value).replace(/'/g, "''");
    conditions.push(`CAST(${quotedCol} AS VARCHAR) = '${escapedValue}'`);
  }

  // If all filters were invalid / skipped, return the original SQL unchanged.
  if (conditions.length === 0) {
    return sql;
  }

  const filterExpr = conditions.join(' AND ');

  const { whereEnd, insertPos } = findWhereInsertionPoint(cleanSql);

  if (whereEnd !== -1) {
    // Existing WHERE clause — append AND before the next major keyword
    const beforeWhere = cleanSql.slice(0, whereEnd).trimEnd();
    const afterWhere = cleanSql.slice(whereEnd).trimStart();
    return beforeWhere + ' AND ' + filterExpr + (afterWhere ? ' ' + afterWhere : '');
  }
  // No WHERE clause — insert one
  const before = cleanSql.slice(0, insertPos).trimEnd();
  const after = cleanSql.slice(insertPos).trimStart();
  return before + ' WHERE ' + filterExpr + (after ? ' ' + after : '');
}

/**
 * Returns true when `keyword` appears at position `pos` in `sql` (case-
 * insensitive) and is surrounded by word boundaries.
 */
function matchKeywordAt(sql: string, pos: number, keyword: string): boolean {
  if (pos + keyword.length > sql.length) {
    return false;
  }
  if (sql.slice(pos, pos + keyword.length).toUpperCase() !== keyword) {
    return false;
  }
  if (pos > 0 && /\w/.test(sql[pos - 1])) {
    return false;
  }
  const after = pos + keyword.length;
  if (after < sql.length && /\w/.test(sql[after])) {
    return false;
  }
  return true;
}

/**
 * Scans `sql` to locate the outermost WHERE clause (if any) and determine
 * where to insert/append filter conditions.
 *
 * `whereEnd`  – position just before the next major keyword after WHERE
 *               (-1 when no outermost WHERE exists).
 * `insertPos` – position where a new WHERE clause should be inserted when
 *               there is no existing one.
 */
function findWhereInsertionPoint(sql: string): { whereEnd: number; insertPos: number } {
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inBacktick = false;

  let foundWhere = false;
  let whereClauseEnd = -1;
  let insertPos = sql.length;
  let foundInsert = false;

  const clauseKeywords = ['GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT', 'FETCH'];

  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];

    if (ch === "'" && !inDoubleQuote && !inBacktick) { inSingleQuote = !inSingleQuote; continue; }
    if (ch === '"' && !inSingleQuote && !inBacktick) { inDoubleQuote = !inDoubleQuote; continue; }
    if (ch === '`' && !inSingleQuote && !inDoubleQuote) { inBacktick = !inBacktick; continue; }
    if (inSingleQuote || inDoubleQuote || inBacktick) { continue; }

    if (ch === '(') { depth++; continue; }
    if (ch === ')') { depth--; continue; }
    if (depth !== 0) { continue; }

    // Outermost level — look for keywords
    if (!foundWhere && matchKeywordAt(sql, i, 'WHERE')) {
      foundWhere = true;
      // Skip past the keyword itself so we don't match it as a clause end
      i += 4;
      continue;
    }

    for (const kw of clauseKeywords) {
      if (matchKeywordAt(sql, i, kw)) {
        if (foundWhere && whereClauseEnd === -1) {
          whereClauseEnd = i;
        }
        if (!foundInsert) {
          insertPos = i;
          foundInsert = true;
        }
        break;
      }
    }
  }

  // If WHERE was found but no subsequent clause keyword, conditions extend to end
  if (foundWhere && whereClauseEnd === -1) {
    whereClauseEnd = sql.length;
  }

  return { whereEnd: foundWhere ? whereClauseEnd : -1, insertPos };
}

export type TransformationType =
  | 'uppercase'
  | 'lowercase'
  | 'trim'
  | 'cast'
  | 'truncate'
  | 'substring';

export interface ColumnTransformation {
  type: TransformationType;
  columnName: string;
  targetType?: string; // For CAST (INTEGER, BIGINT, DOUBLE, VARCHAR, TIMESTAMP, DATE)
  length?: number; // For TRUNCATE/SUBSTRING
}

/**
 * Applies a SQL transformation to a column in a SELECT query.
 * Uses regex-based manipulation to find and wrap the column with a SQL function.
 *
 * @param sql - The original SQL query
 * @param transformation - The transformation to apply
 * @returns The transformed SQL, or null if transformation failed
 */
export function applySqlTransformation(
  sql: string,
  transformation: ColumnTransformation,
  resultColumns?: string[]
): string | null {
  // Extract SELECT clause
  const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/is);
  if (!selectMatch) {
    return null;
  }

  const selectClause = selectMatch[1];
  let columns = splitSelectColumns(selectClause);

  if (!columns || columns.length === 0) {
    return null;
  }

  // If the SELECT clause contains a bare *, expand it to explicit column names
  // so that we can find and transform the target column
  if (resultColumns && resultColumns.length > 0) {
    const starIndex = columns.findIndex((c) => c.trim() === '*');
    if (starIndex !== -1) {
      const quotedCols = resultColumns.map((c) => quoteColumnName(c));
      columns = [
        ...columns.slice(0, starIndex),
        ...quotedCols,
        ...columns.slice(starIndex + 1),
      ];
    }
  }

  // Find and transform the target column
  let transformed = false;
  const transformedColumns = columns.map((col) => {
    if (transformed) {
      return col;
    }

    if (matchesColumn(col, transformation.columnName)) {
      transformed = true;
      return applyTransformation(col, transformation);
    }
    return col;
  });

  if (!transformed) {
    return null;
  }

  const newSelectClause = transformedColumns.join(', ');
  const selectIndex = sql.search(/SELECT/i);
  const fromIndex = selectMatch.index! + selectMatch[0].length - 4; // 4 = "FROM".length

  return (
    sql.substring(0, selectIndex + 6) + // "SELECT"
    ' ' +
    newSelectClause +
    ' ' +
    sql.substring(fromIndex)
  );
}

/**
 * Quotes a column name with backticks. Always quotes to avoid issues with
 * SQL reserved words (date, time, order, group, etc.).
 */
export function quoteColumnName(name: string): string {
  return '`' + name.replace(/`/g, '``') + '`';
}

/**
 * Splits a SELECT clause into individual columns, respecting quotes, parentheses, and aliases
 */
function splitSelectColumns(selectClause: string): string[] {
  const columns: string[] = [];
  let current = '';
  let parenDepth = 0;
  let inDoubleQuote = false;
  let inBacktick = false;
  let inSingleQuote = false;

  for (let i = 0; i < selectClause.length; i++) {
    const char = selectClause[i];

    // Handle string/quote escaping
    if (char === '"' && !inBacktick && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      current += char;
    } else if (char === '`' && !inDoubleQuote && !inSingleQuote) {
      inBacktick = !inBacktick;
      current += char;
    } else if (char === "'" && !inDoubleQuote && !inBacktick) {
      inSingleQuote = !inSingleQuote;
      current += char;
    } else if (char === '(' && !inDoubleQuote && !inBacktick && !inSingleQuote) {
      parenDepth++;
      current += char;
    } else if (char === ')' && !inDoubleQuote && !inBacktick && !inSingleQuote) {
      parenDepth--;
      current += char;
    } else if (char === ',' && parenDepth === 0 && !inDoubleQuote && !inBacktick && !inSingleQuote) {
      // Found a column separator
      if (current.trim()) {
        columns.push(current.trim());
      }
      current = '';
    } else {
      current += char;
    }
  }

  // Add the last column
  if (current.trim()) {
    columns.push(current.trim());
  }

  return columns;
}

/**
 * Checks if a column definition matches the target column name
 */
function matchesColumn(colDef: string, targetName: string): boolean {
  // Remove trailing alias if present (everything after "AS" keyword)
  const withoutAlias = colDef.replace(/\s+AS\s+\w+$/i, '');

  // Extract just the column name (remove function calls, table prefixes, quotes)
  const columnPart = withoutAlias.trim();

  // Check exact match (including function calls like UPPER(col))
  if (columnPart.toLowerCase() === targetName.toLowerCase()) {
    return true;
  }

  // Check unquoted column name
  const unquoted = columnPart
    .replace(/`/g, '')
    .replace(/"/g, '')
    .toLowerCase();

  const targetUnquoted = targetName
    .replace(/`/g, '')
    .replace(/"/g, '')
    .toLowerCase();

  // Check simple column (no dots, no function)
  if (unquoted === targetUnquoted) {
    return true;
  }

  // Check with table prefix (e.g., "users.name" matches "name")
  if (unquoted.endsWith('.' + targetUnquoted)) {
    return true;
  }

  return false;
}

/**
 * Applies the transformation function to a column definition
 */
function applyTransformation(colDef: string, transformation: ColumnTransformation): string {
  // Parse alias if present
  const aliasMatch = colDef.match(/\s+AS\s+(\w+)$/i);
  const alias = aliasMatch ? aliasMatch[1] : null;

  const withoutAlias = alias
    ? colDef.substring(0, colDef.length - aliasMatch![0].length)
    : colDef;

  const columnName = withoutAlias.trim();

  switch (transformation.type) {
    case 'uppercase':
      return `UPPER(${columnName})${alias ? ` AS ${alias}` : ''}`;

    case 'lowercase':
      return `LOWER(${columnName})${alias ? ` AS ${alias}` : ''}`;

    case 'trim':
      return `TRIM(${columnName})${alias ? ` AS ${alias}` : ''}`;

    case 'cast': {
      if (!transformation.targetType) {
        return colDef;
      }
      const castAlias = alias || quoteColumnName(transformation.columnName);
      return `CAST(${columnName} AS ${transformation.targetType}) AS ${castAlias}`;
    }

    case 'truncate':
      if (transformation.length === undefined) {
        return colDef;
      }
      return `SUBSTRING(${columnName}, 1, ${transformation.length})${alias ? ` AS ${alias}` : ''}`;

    case 'substring':
      if (transformation.length === undefined) {
        return colDef;
      }
      return `SUBSTRING(${columnName}, 1, ${transformation.length})${alias ? ` AS ${alias}` : ''}`;

    default:
      return colDef;
  }
}

/**
 * Simple SQL pretty-printer. Adds line breaks before major keywords
 * and indents column lists and conditions.
 */
export function prettifySql(sql: string): string {
  if (!sql || !sql.trim()) {
    return sql;
  }

  // Normalize whitespace
  let formatted = sql.replace(/\s+/g, ' ').trim();

  // Keywords that start a new line (no indent)
  const majorKeywords = [
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING',
    'ORDER BY', 'LIMIT', 'OFFSET', 'UNION', 'UNION ALL',
    'INTERSECT', 'EXCEPT', 'INSERT INTO', 'UPDATE', 'DELETE FROM',
    'SET', 'VALUES',
  ];

  // Keywords that start a new line (with indent)
  const joinKeywords = [
    'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN',
    'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN',
    'INNER JOIN', 'CROSS JOIN', 'JOIN',
    'ON', 'AND', 'OR',
  ];

  // Replace major keywords (case-insensitive, whole word)
  for (const kw of majorKeywords) {
    const re = new RegExp(`\\b(${kw})\\b`, 'gi');
    formatted = formatted.replace(re, `\n$1`);
  }

  // Replace join keywords with indented lines
  for (const kw of joinKeywords) {
    const re = new RegExp(`\\b(${kw})\\b`, 'gi');
    formatted = formatted.replace(re, `\n  $1`);
  }

  // Indent column list: add newline+indent after SELECT and before FROM
  formatted = formatted.replace(/\nSELECT\s+/i, '\nSELECT\n  ');

  // Break comma-separated columns onto individual lines (only in SELECT clause)
  const selectMatch = formatted.match(/\nSELECT\n {2}([\s\S]*?)\nFROM/i);
  if (selectMatch) {
    const cols = selectMatch[1];
    // Split on commas not inside parentheses
    let depth = 0;
    let result = '';
    for (let i = 0; i < cols.length; i++) {
      const ch = cols[i];
      if (ch === '(') {
        depth++;
      }
      if (ch === ')') {
        depth--;
      }
      if (ch === ',' && depth === 0) {
        result += ',\n  ';
      } else {
        result += ch;
      }
    }
    formatted = formatted.replace(cols, result);
  }

  // Clean up: remove leading newline, collapse multiple blank lines
  formatted = formatted.replace(/^\n+/, '').replace(/\n{3,}/g, '\n\n');

  return formatted;
}

/**
 * Calculates statistics for a column based on displayed row data
 */
export interface ColumnStats {
  count: number;
  nullCount: number;
  distinctCount: number;
  min?: number;
  max?: number;
  avg?: number;
  nonNullPercentage: number;
  uniquenessPercentage: number;
}

export type TimeGrain = 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'QUARTER' | 'YEAR';
export type AggregationFunction = 'SUM' | 'AVG' | 'MIN' | 'MAX' | 'COUNT';

export interface TimeGrainConfig {
  grain: TimeGrain;
  temporalColumn: string;
  metricAggregations: Record<string, AggregationFunction>;
  dimensions?: string[];
}

export interface AggregationConfig {
  metricAggregations: Record<string, AggregationFunction>;
  groupByColumns: string[];
}

/**
 * Checks whether a column type is a temporal type (DATE, TIMESTAMP, TIME).
 */
export function isTemporalType(type: string): boolean {
  const temporalTypes = ['date', 'timestamp', 'time'];
  return temporalTypes.some((t) => type.toLowerCase().includes(t));
}

/**
 * Wraps the original SQL as a subquery with DATE_TRUNC and GROUP BY for time grain aggregation.
 * Returns null if config is incomplete.
 */
export function buildTimeGrainQuery(
  originalSql: string,
  config: TimeGrainConfig
): string | null {
  if (!config.grain || !config.temporalColumn) {
    return null;
  }

  const metricEntries = Object.entries(config.metricAggregations);
  if (metricEntries.length === 0) {
    return null;
  }

  // Strip trailing semicolons/whitespace — they would be invalid inside the subquery
  const cleanSql = originalSql.replace(/;\s*$/, '').trim();

  const quotedTemporal = quoteColumnName(config.temporalColumn);
  // Qualify column references with the subquery alias to avoid ambiguity
  // (e.g. when the inner query has `to_date(date) AS date`, both the source
  // column and alias are named "date")
  const qualifiedTemporal = `_t.${quotedTemporal}`;
  const dateTruncExpr = `DATE_TRUNC('${config.grain}', ${qualifiedTemporal})`;

  const dimensionParts = (config.dimensions || []).map((d) => quoteColumnName(d));
  const qualifiedDimensionSelects = dimensionParts.map((d) => `_t.${d} AS ${d}`);
  const qualifiedDimensionGroupBy = dimensionParts.map((d) => `_t.${d}`);

  const selectParts = [
    `${dateTruncExpr} AS ${quotedTemporal}`,
    ...qualifiedDimensionSelects,
    ...metricEntries.map(([col, agg]) => {
      const quotedCol = quoteColumnName(col);
      return `${agg}(_t.${quotedCol}) AS ${quotedCol}`;
    }),
  ];

  // Drill supports aliases in GROUP BY, so reference the column alias
  // instead of repeating the DATE_TRUNC expression
  const groupByParts = [quotedTemporal, ...qualifiedDimensionGroupBy];

  return [
    `SELECT ${selectParts.join(', ')}`,
    `FROM (${cleanSql}) AS _t`,
    `GROUP BY ${groupByParts.join(', ')}`,
    `ORDER BY 1`,
  ].join('\n');
}

/**
 * Returns true only when timeGrain is set AND every metric has a corresponding aggregation.
 */
export function hasCompleteTimeGrainConfig(
  chartOptions: Record<string, unknown> | undefined,
  metrics: string[] | undefined
): boolean {
  if (!chartOptions || !chartOptions.timeGrain || !metrics || metrics.length === 0) {
    return false;
  }
  const aggregations = chartOptions.metricAggregations as Record<string, string> | undefined;
  if (!aggregations) {
    return false;
  }
  return metrics.every((m) => !!aggregations[m]);
}

/**
 * Returns true when at least one metric has an aggregation function set.
 * Does NOT require timeGrain.
 */
export function hasCompleteAggregationConfig(
  chartOptions: Record<string, unknown> | undefined,
  metrics: string[] | undefined
): boolean {
  if (!chartOptions || !metrics || metrics.length === 0) {
    return false;
  }
  const aggs = chartOptions.metricAggregations as Record<string, string> | undefined;
  if (!aggs) {
    return false;
  }
  return metrics.some((m) => !!aggs[m]);
}

/**
 * Wraps original SQL as a subquery with GROUP BY and metric aggregations.
 * Time grain (DATE_TRUNC) is handled separately by the backend Python/sqlglot service.
 */
export function buildAggregationQuery(
  originalSql: string,
  config: AggregationConfig
): string | null {
  const metricEntries = Object.entries(config.metricAggregations);
  if (metricEntries.length === 0) {
    return null;
  }

  // Strip trailing semicolons/whitespace — they would be invalid inside the subquery
  const cleanSql = originalSql.replace(/;\s*$/, '').trim();

  const selectParts: string[] = [];
  const groupByParts: string[] = [];

  for (const col of config.groupByColumns) {
    const quoted = quoteColumnName(col);
    const qualified = `_t.${quoted}`;
    selectParts.push(`${qualified} AS ${quoted}`);
    groupByParts.push(qualified);
  }

  for (const [col, agg] of metricEntries) {
    const quoted = quoteColumnName(col);
    selectParts.push(`${agg}(_t.${quoted}) AS ${quoted}`);
  }

  if (selectParts.length === 0) {
    return null;
  }

  const parts = [
    `SELECT ${selectParts.join(', ')}`,
    `FROM (${cleanSql}) AS _t`,
  ];
  if (groupByParts.length > 0) {
    parts.push(`GROUP BY ${groupByParts.join(', ')}`);
  }
  parts.push('ORDER BY 1');
  return parts.join('\n');
}

export type EffectiveQueryConfig = {
  xAxis?: string;
  metrics?: string[];
  dimensions?: string[];
  chartOptions?: Record<string, unknown>;
};

/**
 * Synchronous version of getEffectiveQuery. Computes the effective SQL query
 * for a visualization, wrapping the original SQL with aggregation/time grain
 * when the config requires it.
 *
 * When time grain is set, data is aggregated with DATE_TRUNC + GROUP BY.
 * If the user hasn't set explicit aggregation functions, SUM is used as the
 * default for each metric so the chart shows meaningful grouped results.
 *
 * All transformations are done client-side (no backend API dependency).
 *
 * Prefer this over getEffectiveQuery in useMemo / render paths to avoid
 * unnecessary async state transitions.
 */
export function computeEffectiveQuery(
  originalSql: string,
  cfg: EffectiveQueryConfig
): string {
  const timeGrain = cfg.chartOptions?.timeGrain as TimeGrain | undefined;
  const hasExplicitAgg = hasCompleteAggregationConfig(cfg.chartOptions, cfg.metrics);

  // Determine effective aggregation functions
  let aggregations: Record<string, AggregationFunction> | undefined;
  if (hasExplicitAgg) {
    aggregations = cfg.chartOptions!.metricAggregations as Record<string, AggregationFunction>;
  } else if (timeGrain && cfg.xAxis && cfg.metrics && cfg.metrics.length > 0) {
    // Time grain without explicit aggregation → default to SUM
    aggregations = {};
    for (const m of cfg.metrics) {
      aggregations[m] = 'SUM';
    }
  }

  // Nothing to transform
  if (!aggregations) {
    return originalSql;
  }

  // Time grain path: DATE_TRUNC + GROUP BY + aggregation
  if (timeGrain && cfg.xAxis) {
    const tgQuery = buildTimeGrainQuery(originalSql, {
      grain: timeGrain,
      temporalColumn: cfg.xAxis,
      metricAggregations: aggregations,
      dimensions: cfg.dimensions,
    });
    return tgQuery || originalSql;
  }

  // Aggregation-only path (no time grain)
  const groupByColumns: string[] = [];
  if (cfg.xAxis) {
    groupByColumns.push(cfg.xAxis);
  }
  if (cfg.dimensions) {
    groupByColumns.push(...cfg.dimensions);
  }
  const wrapped = buildAggregationQuery(originalSql, {
    metricAggregations: aggregations,
    groupByColumns,
  });
  return wrapped || originalSql;
}

/**
 * Async wrapper around computeEffectiveQuery for use in async contexts
 * (useEffect chains, event handlers, etc.).
 */
export async function getEffectiveQuery(
  originalSql: string,
  cfg: EffectiveQueryConfig
): Promise<string> {
  return computeEffectiveQuery(originalSql, cfg);
}

export function calculateColumnStats(
  rowData: Record<string, unknown>[],
  columnName: string
): ColumnStats {
  const values = rowData
    .map((row) => row[columnName])
    .filter((v) => v != null);

  const numericValues = values
    .map((v) => Number(v))
    .filter((v) => !isNaN(v));

  const totalRows = rowData.length;
  const distinctSet = new Set(values.map((v) => String(v)));

  return {
    count: values.length,
    nullCount: totalRows - values.length,
    distinctCount: distinctSet.size,
    min: numericValues.length > 0 ? Math.min(...numericValues) : undefined,
    max: numericValues.length > 0 ? Math.max(...numericValues) : undefined,
    avg:
      numericValues.length > 0
        ? numericValues.reduce((a, b) => a + b, 0) / numericValues.length
        : undefined,
    nonNullPercentage: values.length > 0 ? (values.length / totalRows) * 100 : 0,
    uniquenessPercentage: (distinctSet.size / values.length) * 100,
  };
}

/**
 * Normalize SQL for comparison — handles whitespace, case, and comments.
 * Useful for matching visualizations to tabs by SQL equality.
 */
export function normalizeSql(sql: string): string {
  return sql
    .toLowerCase()
    .replace(/--[^\n]*/g, '') // Remove single-line comments
    .replace(/\/\*[\s\S]*?\*\//g, '') // Remove block comments
    .replace(/\s+/g, ' ') // Collapse whitespace
    .trim();
}
