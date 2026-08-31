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

/** How a result set's columns map onto geographic coordinates. */
export type GeoMapping =
  | { kind: 'latlon'; latColumn: string; lonColumn: string }
  | { kind: 'geometry'; geomColumn: string };

export interface GeoColumnCandidate {
  mapping: GeoMapping;
  /** True when matched by name; false when the caller chose it. */
  inferred: boolean;
}

// Ordered most specific first, so "longitude" wins over a bare "lon" in a table that
// carries both. Matched whole, never as substrings: "latency_ms" and "plate_id" are not
// coordinates, and substring matching misfires constantly on real schemas.
const LAT_NAMES = ['latitude', 'lat', 'y'];
const LON_NAMES = ['longitude', 'lon', 'lng', 'long', 'x'];
const GEOM_NAMES = ['geom', 'geometry', 'wkt', 'shape'];

const NUMERIC_TYPE = /INT|DOUBLE|FLOAT|DECIMAL|NUMERIC|BIGINT|SMALLINT/i;

/**
 * "x" and "y" alone are far more likely to be something else — a chart axis, a
 * coordinate in some unrelated space — so they only count as a pair.
 */
const WEAK_NAMES = new Set(['x', 'y']);

function findByName(
  columns: string[],
  candidates: string[],
  accept: (column: string) => boolean,
): string | null {
  for (const candidate of candidates) {
    const match = columns.find((c) => c.toLowerCase() === candidate && accept(c));
    if (match) {
      return match;
    }
  }
  return null;
}

/**
 * Resolves a result set's columns to coordinates, by name.
 *
 * Returns null when nothing matches — the caller shows the manual picker rather than an
 * empty map. Detection is only ever a guess, so the result is always overridable.
 *
 * A lat/lon pair beats a geometry column: it supports the bounding-box prefilter and
 * the projected distance path, neither of which works on WKT text.
 *
 * See docs/dev/GeoFilterBuilder.md.
 */
export function detectGeoColumns(
  columns: string[],
  metadata?: string[],
): GeoColumnCandidate | null {
  const isNumeric = (column: string): boolean => {
    if (!metadata) {
      return true;
    }
    const type = metadata[columns.indexOf(column)];
    return !type || NUMERIC_TYPE.test(type);
  };

  const latColumn = findByName(columns, LAT_NAMES, isNumeric);
  const lonColumn = findByName(columns, LON_NAMES, isNumeric);

  if (latColumn && lonColumn) {
    // Both weak or both strong: an "x" paired with a "latitude" is coincidence, not a
    // coordinate pair, so require the weak names to appear together.
    const latIsWeak = WEAK_NAMES.has(latColumn.toLowerCase());
    const lonIsWeak = WEAK_NAMES.has(lonColumn.toLowerCase());
    if (latIsWeak === lonIsWeak) {
      return { mapping: { kind: 'latlon', latColumn, lonColumn }, inferred: true };
    }
  }

  const geomColumn = findByName(columns, GEOM_NAMES, () => true);
  if (geomColumn) {
    return { mapping: { kind: 'geometry', geomColumn }, inferred: true };
  }

  return null;
}
