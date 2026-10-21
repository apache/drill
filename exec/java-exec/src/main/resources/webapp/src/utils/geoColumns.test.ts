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
import { describe, it, expect } from 'vitest';
import { detectGeoColumns } from './geoColumns';

const NUM = 'DOUBLE';
const STR = 'VARCHAR';

describe('detectGeoColumns', () => {
  it('finds a lat/lon pair by name', () => {
    const found = detectGeoColumns(['id', 'lat', 'lon'], [STR, NUM, NUM]);
    expect(found?.mapping).toEqual({ kind: 'latlon', latColumn: 'lat', lonColumn: 'lon' });
    expect(found?.inferred).toBe(true);
  });

  it('accepts the long spellings', () => {
    const found = detectGeoColumns(['latitude', 'longitude'], [NUM, NUM]);
    expect(found?.mapping).toEqual({
      kind: 'latlon', latColumn: 'latitude', lonColumn: 'longitude',
    });
  });

  it('is case-insensitive', () => {
    const found = detectGeoColumns(['LAT', 'LNG'], [NUM, NUM]);
    expect(found?.mapping).toEqual({ kind: 'latlon', latColumn: 'LAT', lonColumn: 'LNG' });
  });

  /** A VARCHAR named "lat" is a label, not a coordinate. */
  it('rejects a lat/lon pair that is not numeric', () => {
    expect(detectGeoColumns(['lat', 'lon'], [STR, STR])).toBeNull();
  });

  it('finds a geometry column by name', () => {
    const found = detectGeoColumns(['id', 'geom'], [STR, STR]);
    expect(found?.mapping).toEqual({ kind: 'geometry', geomColumn: 'geom' });
  });

  it('prefers a lat/lon pair over a geometry column', () => {
    const found = detectGeoColumns(['geom', 'lat', 'lon'], [STR, NUM, NUM]);
    expect(found?.mapping.kind).toBe('latlon');
  });

  it('returns null when nothing looks geographic', () => {
    expect(detectGeoColumns(['id', 'name', 'total'], [STR, STR, NUM])).toBeNull();
  });

  /** Half a pair is not a location. */
  it('returns null for a latitude with no longitude', () => {
    expect(detectGeoColumns(['id', 'lat'], [STR, NUM])).toBeNull();
  });

  it('works without type metadata', () => {
    expect(detectGeoColumns(['lat', 'lon'])?.mapping.kind).toBe('latlon');
  });

  /** x/y are weak signals, so they only count when both are present. */
  it('accepts x and y together but not alone', () => {
    expect(detectGeoColumns(['x', 'y'], [NUM, NUM])?.mapping).toEqual({
      kind: 'latlon', latColumn: 'y', lonColumn: 'x',
    });
    expect(detectGeoColumns(['id', 'x'], [STR, NUM])).toBeNull();
  });

  /** "longitude" should win over a bare "lon" when a table carries both. */
  it('prefers the more specific spelling', () => {
    const found = detectGeoColumns(['lon', 'longitude', 'lat'], [NUM, NUM, NUM]);
    expect(found?.mapping).toEqual({
      kind: 'latlon', latColumn: 'lat', lonColumn: 'longitude',
    });
  });

  /**
   * A column merely containing "lat" — "plate_id", "latency_ms" — is not a coordinate.
   * Matching on substrings here would misfire constantly on real schemas.
   */
  it('does not match columns that merely contain a coordinate word', () => {
    expect(detectGeoColumns(['latency_ms', 'longest_run'], [NUM, NUM])).toBeNull();
  });

  it('handles an empty column list', () => {
    expect(detectGeoColumns([], [])).toBeNull();
  });
});
