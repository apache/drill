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
  transformation: ColumnTransformation
): string | null {
  // Extract SELECT clause
  const selectMatch = sql.match(/SELECT\s+(.*?)\s+FROM/is);
  if (!selectMatch) {
    return null;
  }

  const selectClause = selectMatch[1];
  const columns = splitSelectColumns(selectClause);

  if (!columns || columns.length === 0) {
    return null;
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
  const fromIndex = selectMatch.index! + selectMatch[0].length - selectMatch[1].length - 5; // 5 = "FROM".length

  return (
    sql.substring(0, selectIndex + 6) + // "SELECT"
    ' ' +
    newSelectClause +
    ' ' +
    sql.substring(fromIndex)
  );
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

    case 'cast':
      if (!transformation.targetType) {
        return colDef;
      }
      return `CAST(${columnName} AS ${transformation.targetType})${alias ? ` AS ${alias}` : ''}`;

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
