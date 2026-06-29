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

export interface ProfileConfig {
  correlationMaxColumns: number;
  profileMaxRows: number;
  profileSampleSize: number;
  histogramBins: number;
  topKValues: number;
}

export type ColumnDataType = 'numeric' | 'string' | 'temporal' | 'boolean' | 'other';

export interface ColumnProfileData {
  name: string;
  type: string;
  dataType: ColumnDataType;
  count: number;
  distinct: number;
  missing: number;
  missingPct: number;
  // numeric
  mean?: number;
  stddev?: number;
  min?: number;
  max?: number;
  zeros?: number;
  negative?: number;
  p5?: number;
  p25?: number;
  median?: number;
  p75?: number;
  p95?: number;
  // string
  minLength?: number;
  maxLength?: number;
  avgLength?: number;
  emptyCount?: number;
  // distributions
  histogram?: { bin: string; count: number }[];
  topValues?: { value: string; count: number; pct: number }[];
}

export interface CorrelationMatrix {
  columns: string[];
  values: number[][];
}

export interface DataProfileResult {
  tableName: string;
  rowCount: number;
  columnCount: number;
  missingCells: number;
  missingCellsPct: number;
  memorySizeEstimate: number;
  columns: ColumnProfileData[];
  correlationMatrix?: CorrelationMatrix;
  profiledAt: number;
  sampleSize: number;
}
