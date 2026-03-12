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
import { useState, useRef, useCallback } from 'react';
import { executeQuery } from '../api/queries';
import { getCacheRows } from '../api/resultCache';
import { getProfileConfig } from '../api/profile';
import type { ProfileConfig, DataProfileResult, ColumnProfileData } from '../types/profile';
import {
  classifyColumnType,
  buildAggregateQuery,
  computeHistogram,
  computeFrequencyTable,
  computePercentiles,
  computeCorrelationMatrix,
  estimateMemorySize,
  sourceToSelectSql,
} from '../utils/profileUtils';

export type ProfilePhase = 'idle' | 'aggregates' | 'sample' | 'computing' | 'done' | 'error';

export interface UseDataProfileOptions {
  cacheId?: string;
  columns?: string[];
  metadata?: string[];
  totalRows?: number;
  sql?: string;
  schemaName?: string;
  tableName?: string;
}

export interface UseDataProfileReturn {
  profile: DataProfileResult | null;
  isLoading: boolean;
  phase: ProfilePhase;
  error: string | null;
  startProfile: () => void;
  cancel: () => void;
}

const DEFAULT_CONFIG: ProfileConfig = {
  correlationMaxColumns: 20,
  profileMaxRows: 50000,
  profileSampleSize: 10000,
  histogramBins: 20,
  topKValues: 10,
};

export function useDataProfile(opts: UseDataProfileOptions): UseDataProfileReturn {
  const [profile, setProfile] = useState<DataProfileResult | null>(null);
  const [phase, setPhase] = useState<ProfilePhase>('idle');
  const [error, setError] = useState<string | null>(null);
  const cancelledRef = useRef(false);
  const configRef = useRef<ProfileConfig>(DEFAULT_CONFIG);

  const getSource = useCallback((): string => {
    if (opts.sql) {
      return opts.sql;
    }
    if (opts.schemaName && opts.tableName) {
      const parts = opts.schemaName.split('.');
      const formatted = parts.length <= 1
        ? opts.schemaName
        : parts[0] + '.' + parts.slice(1).map((p) => `\`${p}\``).join('.');
      return `${formatted}.\`${opts.tableName}\``;
    }
    return '';
  }, [opts.sql, opts.schemaName, opts.tableName]);

  const getTableName = useCallback((): string => {
    if (opts.tableName) {
      return opts.tableName;
    }
    if (opts.sql) {
      return 'Query Result';
    }
    return 'Unknown';
  }, [opts.tableName, opts.sql]);

  const startProfile = useCallback(async () => {
    cancelledRef.current = false;
    setError(null);
    setProfile(null);

    try {
      // Load config
      configRef.current = await getProfileConfig();
    } catch {
      configRef.current = DEFAULT_CONFIG;
    }

    const config = configRef.current;
    const source = getSource();
    if (!source && !opts.cacheId) {
      setError('No data source specified');
      setPhase('error');
      return;
    }

    // Determine columns and types
    let colNames = opts.columns || [];
    let colTypes = opts.metadata || [];

    // If we don't have column info, fetch it
    if (colNames.length === 0 && source) {
      setPhase('aggregates');
      try {
        const schemaResult = await executeQuery({
          query: sourceToSelectSql(source, 1),
          queryType: 'SQL',
        });
        colNames = schemaResult.columns || [];
        colTypes = schemaResult.metadata || [];
      } catch (e) {
        setError(`Failed to fetch schema: ${e instanceof Error ? e.message : String(e)}`);
        setPhase('error');
        return;
      }
    }

    if (cancelledRef.current) { return; }

    const columnDefs = colNames.map((name, i) => ({
      name,
      type: colTypes[i] || 'VARCHAR',
      dataType: classifyColumnType(colTypes[i] || 'VARCHAR'),
    }));

    // ---- Phase 1: Aggregate stats via SQL ----
    setPhase('aggregates');

    const profileCols: ColumnProfileData[] = columnDefs.map((col) => ({
      name: col.name,
      type: col.type,
      dataType: col.dataType,
      count: 0,
      distinct: 0,
      missing: 0,
      missingPct: 0,
    }));

    let totalRows = opts.totalRows || 0;

    if (source) {
      try {
        const aggSql = buildAggregateQuery(source, columnDefs, config.profileMaxRows);
        const aggResult = await executeQuery({ query: aggSql, queryType: 'SQL' });

        if (cancelledRef.current) { return; }

        if (aggResult.rows && aggResult.rows.length > 0) {
          const row = aggResult.rows[0];
          totalRows = Number(row['total_rows']) || totalRows;

          for (let i = 0; i < columnDefs.length; i++) {
            const alias = columnDefs[i].name.replace(/[^a-zA-Z0-9_]/g, '_');
            const pc = profileCols[i];
            pc.count = Number(row[`cnt__${alias}`]) || 0;
            pc.distinct = Number(row[`dcnt__${alias}`]) || 0;
            pc.missing = totalRows - pc.count;
            pc.missingPct = totalRows > 0 ? (pc.missing / totalRows) * 100 : 0;

            const minVal = row[`min__${alias}`];
            const maxVal = row[`max__${alias}`];
            if (minVal != null) { pc.min = Number(minVal); }
            if (maxVal != null) { pc.max = Number(maxVal); }

            if (columnDefs[i].dataType === 'numeric') {
              const avg = row[`avg__${alias}`];
              const std = row[`std__${alias}`];
              if (avg != null) { pc.mean = Number(avg); }
              if (std != null) { pc.stddev = Number(std); }
            }

            if (columnDefs[i].dataType === 'string') {
              const ml = row[`minlen__${alias}`];
              const xl = row[`maxlen__${alias}`];
              const al = row[`avglen__${alias}`];
              if (ml != null) { pc.minLength = Number(ml); }
              if (xl != null) { pc.maxLength = Number(xl); }
              if (al != null) { pc.avgLength = Number(al); }
            }
          }
        }
      } catch (e) {
        setError(`Aggregate query failed: ${e instanceof Error ? e.message : String(e)}`);
        setPhase('error');
        return;
      }
    }

    if (cancelledRef.current) { return; }

    // ---- Phase 2: Fetch sample for distributions ----
    setPhase('sample');

    let sampleRows: Record<string, unknown>[] = [];
    const sampleSize = config.profileSampleSize;

    try {
      if (opts.cacheId) {
        const cached = await getCacheRows(opts.cacheId, 0, sampleSize);
        if (cached) {
          sampleRows = cached.rows;
        }
      } else if (source) {
        const sampleResult = await executeQuery({
          query: sourceToSelectSql(source, sampleSize),
          queryType: 'SQL',
        });
        sampleRows = sampleResult.rows || [];
      }
    } catch (e) {
      // Sample fetch is best-effort — continue with aggregate stats only
      console.warn('Sample fetch failed, distributions unavailable:', e);
    }

    if (cancelledRef.current) { return; }

    // ---- Phase 3: Client-side computations ----
    setPhase('computing');

    if (sampleRows.length > 0) {
      for (let i = 0; i < columnDefs.length; i++) {
        const col = columnDefs[i];
        const pc = profileCols[i];
        const rawValues = sampleRows.map((r) => r[col.name]);

        if (col.dataType === 'numeric') {
          const numericValues = rawValues
            .map((v) => Number(v))
            .filter((v) => isFinite(v));

          pc.histogram = computeHistogram(numericValues, config.histogramBins);

          const pctls = computePercentiles(numericValues, [5, 25, 50, 75, 95]);
          pc.p5 = pctls[5];
          pc.p25 = pctls[25];
          pc.median = pctls[50];
          pc.p75 = pctls[75];
          pc.p95 = pctls[95];

          pc.zeros = numericValues.filter((v) => v === 0).length;
          pc.negative = numericValues.filter((v) => v < 0).length;
        } else {
          const stringValues = rawValues.map((v) => (v == null ? null : String(v)));
          pc.topValues = computeFrequencyTable(stringValues, config.topKValues, pc.count);

          if (col.dataType === 'string') {
            pc.emptyCount = stringValues.filter((v) => v === '' || v === null).length;
          }
        }
      }
    }

    if (cancelledRef.current) { return; }

    // ---- Correlation matrix ----
    const numericCols = columnDefs
      .filter((c) => c.dataType === 'numeric')
      .map((c) => c.name)
      .slice(0, config.correlationMaxColumns);

    let correlationMatrix;
    if (numericCols.length >= 2 && sampleRows.length >= 2) {
      correlationMatrix = computeCorrelationMatrix(sampleRows, numericCols);
    }

    // ---- Assemble final result ----
    const missingCells = profileCols.reduce((sum, c) => sum + c.missing, 0);

    const result: DataProfileResult = {
      tableName: getTableName(),
      rowCount: totalRows,
      columnCount: columnDefs.length,
      missingCells,
      missingCellsPct: totalRows * columnDefs.length > 0
        ? (missingCells / (totalRows * columnDefs.length)) * 100
        : 0,
      memorySizeEstimate: estimateMemorySize(totalRows, columnDefs),
      columns: profileCols,
      correlationMatrix,
      profiledAt: Date.now(),
      sampleSize: sampleRows.length,
    };

    setProfile(result);
    setPhase('done');
  }, [opts.cacheId, opts.columns, opts.metadata, opts.totalRows, getSource, getTableName]);

  const cancel = useCallback(() => {
    cancelledRef.current = true;
    setPhase('idle');
  }, []);

  return {
    profile,
    isLoading: phase !== 'idle' && phase !== 'done' && phase !== 'error',
    phase,
    error,
    startProfile,
    cancel,
  };
}
