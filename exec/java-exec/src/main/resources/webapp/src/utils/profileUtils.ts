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
import type { ColumnDataType } from '../types/profile';

/**
 * Convert a source (table reference or SQL query) into a SELECT statement with a LIMIT.
 * Handles both bare table names and full SQL queries.
 */
export function sourceToSelectSql(source: string, limit: number): string {
  const cleanSrc = source.replace(/;\s*$/, '');
  if (/^\s*SELECT\s/i.test(cleanSrc)) {
    // Wrap as subquery to avoid double LIMIT if source already has one
    return `SELECT * FROM (${cleanSrc}) __src LIMIT ${limit}`;
  }
  return `SELECT * FROM ${cleanSrc} LIMIT ${limit}`;
}

/** Classify a Drill metadata type string into a simplified category. */
export function classifyColumnType(drillType: string): ColumnDataType {
  const t = (drillType || '').toUpperCase();
  if (['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
       'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE', 'DECIMAL', 'NUMERIC',
       'MONEY'].some((n) => t.includes(n))) {
    return 'numeric';
  }
  if (['DATE', 'TIME', 'TIMESTAMP', 'INTERVAL'].some((n) => t.includes(n))) {
    return 'temporal';
  }
  if (t === 'BOOLEAN' || t === 'BIT') {
    return 'boolean';
  }
  if (['VARCHAR', 'CHAR', 'TEXT', 'STRING', 'BINARY', 'VARBINARY'].some((n) => t.includes(n))) {
    return 'string';
  }
  return 'other';
}

function q(col: string): string {
  return `\`${col.replace(/`/g, '``')}\``;
}

/**
 * Build a SQL query that computes aggregate stats for all columns.
 * Returns one row with aliased results like cnt__col, dcnt__col, min__col, etc.
 */
export function buildAggregateQuery(
  source: string,
  columns: { name: string; dataType: ColumnDataType }[],
  maxRows: number,
): string {
  const parts: string[] = [`COUNT(*) AS total_rows`];

  for (const col of columns) {
    const c = q(col.name);
    const alias = col.name.replace(/[^a-zA-Z0-9_]/g, '_');
    parts.push(`COUNT(${c}) AS cnt__${alias}`);
    parts.push(`COUNT(DISTINCT ${c}) AS dcnt__${alias}`);
    parts.push(`MIN(${c}) AS min__${alias}`);
    parts.push(`MAX(${c}) AS max__${alias}`);
    if (col.dataType === 'numeric') {
      parts.push(`AVG(${c}) AS avg__${alias}`);
      parts.push(`STDDEV_POP(${c}) AS std__${alias}`);
    }
    if (col.dataType === 'string') {
      parts.push(`MIN(LENGTH(${c})) AS minlen__${alias}`);
      parts.push(`MAX(LENGTH(${c})) AS maxlen__${alias}`);
      parts.push(`AVG(LENGTH(${c})) AS avglen__${alias}`);
    }
  }

  const isQuery = /^\s*SELECT\s/i.test(source);
  const cleanSrc = source.replace(/;\s*$/, '');
  const subquery = isQuery
    ? `(SELECT * FROM (${cleanSrc}) __inner LIMIT ${maxRows})`
    : `(SELECT * FROM ${cleanSrc} LIMIT ${maxRows})`;
  return `SELECT ${parts.join(',\n       ')}\nFROM ${subquery} __profile_src`;
}

/** Compute a histogram from numeric values with equal-width bins. */
export function computeHistogram(
  values: number[],
  binCount: number,
): { bin: string; count: number }[] {
  const sorted = values.filter((v) => v != null && isFinite(v)).sort((a, b) => a - b);
  if (sorted.length === 0) {
    return [];
  }
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  if (min === max) {
    return [{ bin: String(min), count: sorted.length }];
  }

  const width = (max - min) / binCount;
  const bins: { bin: string; count: number }[] = [];
  for (let i = 0; i < binCount; i++) {
    const lo = min + i * width;
    const hi = i === binCount - 1 ? max + 1 : min + (i + 1) * width;
    const label = `${lo.toPrecision(4)}`;
    const count = sorted.filter((v) => v >= lo && v < hi).length;
    bins.push({ bin: label, count });
  }
  // Include max value in last bin
  if (bins.length > 0) {
    bins[bins.length - 1].count += sorted.filter((v) => v === max).length
      - bins.reduce((s, b) => s + b.count, 0) + sorted.length;
    // Simpler: just rebuild properly
  }
  // Actually, let's do this properly with a single pass
  const result: number[] = new Array(binCount).fill(0);
  for (const v of sorted) {
    let idx = Math.floor((v - min) / width);
    if (idx >= binCount) {
      idx = binCount - 1;
    }
    result[idx]++;
  }
  return result.map((count, i) => ({
    bin: (min + i * width).toPrecision(4),
    count,
  }));
}

/** Compute a frequency table of the top K most common values. */
export function computeFrequencyTable(
  values: (string | null | undefined)[],
  topK: number,
  totalCount: number,
): { value: string; count: number; pct: number }[] {
  const freq = new Map<string, number>();
  for (const v of values) {
    const key = v == null ? '(null)' : String(v);
    freq.set(key, (freq.get(key) || 0) + 1);
  }
  return Array.from(freq.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, topK)
    .map(([value, count]) => ({
      value,
      count,
      pct: totalCount > 0 ? (count / totalCount) * 100 : 0,
    }));
}

/** Sort-based percentile calculation with linear interpolation. */
export function computePercentiles(
  values: number[],
  percentiles: number[],
): Record<number, number> {
  const sorted = values.filter((v) => v != null && isFinite(v)).sort((a, b) => a - b);
  if (sorted.length === 0) {
    return {};
  }
  const result: Record<number, number> = {};
  for (const p of percentiles) {
    const idx = (p / 100) * (sorted.length - 1);
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) {
      result[p] = sorted[lo];
    } else {
      result[p] = sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
    }
  }
  return result;
}

/** Compute Pearson correlation matrix for numeric columns. */
export function computeCorrelationMatrix(
  rows: Record<string, unknown>[],
  numericCols: string[],
): { columns: string[]; values: number[][] } {
  const n = rows.length;
  if (n < 2) {
    return { columns: numericCols, values: numericCols.map(() => numericCols.map(() => 0)) };
  }

  // Precompute column value arrays
  const colValues: number[][] = numericCols.map((col) =>
    rows.map((r) => {
      const v = Number(r[col]);
      return isFinite(v) ? v : NaN;
    }),
  );

  // Precompute means
  const means = colValues.map((vals) => {
    let sum = 0;
    let count = 0;
    for (const v of vals) {
      if (!isNaN(v)) {
        sum += v;
        count++;
      }
    }
    return count > 0 ? sum / count : 0;
  });

  const matrix: number[][] = numericCols.map(() => new Array(numericCols.length).fill(0));

  for (let i = 0; i < numericCols.length; i++) {
    matrix[i][i] = 1;
    for (let j = i + 1; j < numericCols.length; j++) {
      let sumXY = 0;
      let sumX2 = 0;
      let sumY2 = 0;
      for (let k = 0; k < n; k++) {
        const x = colValues[i][k];
        const y = colValues[j][k];
        if (!isNaN(x) && !isNaN(y)) {
          const dx = x - means[i];
          const dy = y - means[j];
          sumXY += dx * dy;
          sumX2 += dx * dx;
          sumY2 += dy * dy;
        }
      }
      const denom = Math.sqrt(sumX2 * sumY2);
      const r = denom > 0 ? sumXY / denom : 0;
      matrix[i][j] = r;
      matrix[j][i] = r;
    }
  }

  return { columns: numericCols, values: matrix };
}

/** Rough memory estimate for the dataset. */
export function estimateMemorySize(
  rowCount: number,
  columns: { dataType: ColumnDataType }[],
): number {
  let bytesPerRow = 0;
  for (const col of columns) {
    switch (col.dataType) {
      case 'numeric': bytesPerRow += 8; break;
      case 'boolean': bytesPerRow += 1; break;
      case 'temporal': bytesPerRow += 12; break;
      case 'string': bytesPerRow += 50; break;
      default: bytesPerRow += 32; break;
    }
  }
  return rowCount * bytesPerRow;
}
