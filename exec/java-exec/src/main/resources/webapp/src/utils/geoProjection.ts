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

/** A geographic coordinate, always [longitude, latitude] in EPSG:4326. */
export type LonLat = [number, number];

/**
 * EPSG code for the UTM zone containing a point.
 *
 * Distances in a spatial filter must be measured in a projected coordinate system, or
 * the number means degrees: `ST_DWithin(..., 500)` against raw lon/lat asks for 500
 * degrees and matches everything on Earth.
 *
 * UTM is used rather than Web Mercator (3857) because Mercator's "metres" are stretched
 * by 1/cos(latitude). A 500 m radius would really be about 675 m in Boston and roughly
 * a kilometre in Reykjavik — wrong by a factor that grows with latitude, and close
 * enough near the equator to survive testing.
 *
 * See docs/dev/GeoFilterBuilder.md.
 */
export function utmSridFor(lon: number, lat: number): number {
  // Zone 61 does not exist; a longitude of exactly 180 belongs to zone 60.
  const zone = Math.min(60, Math.max(1, Math.floor((lon + 180) / 6) + 1));
  // Northern and southern hemispheres use separate EPSG bands for the same zone. The
  // equator itself is treated as northern, matching the usual convention.
  return (lat >= 0 ? 32600 : 32700) + zone;
}

/** Arithmetic mean of the coordinates — good enough to pick a UTM zone. */
export function centroidOf(coords: LonLat[]): LonLat {
  const sum = coords.reduce<LonLat>(
    ([lonSum, latSum], [lon, lat]) => [lonSum + lon, latSum + lat],
    [0, 0],
  );
  return [sum[0] / coords.length, sum[1] / coords.length];
}

/**
 * True when the geometry reaches beyond the UTM zone chosen for its centroid.
 *
 * A single UTM zone is only accurate near itself, so a shape spanning several of them
 * would be measured against a projection that does not fit it. The caller warns rather
 * than silently returning a distorted answer.
 *
 * Crossing the equator counts: the two hemispheres use different EPSG codes even for
 * the same zone number.
 */
export function spansMultipleUtmZones(coords: LonLat[]): boolean {
  if (coords.length < 2) {
    return false;
  }
  const first = utmSridFor(coords[0][0], coords[0][1]);
  return coords.some(([lon, lat]) => utmSridFor(lon, lat) !== first);
}
