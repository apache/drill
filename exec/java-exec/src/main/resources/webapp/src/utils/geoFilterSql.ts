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
import type { GeoMapping } from './geoColumns';
import { utmSridFor, centroidOf } from './geoProjection';
import type { LonLat } from './geoProjection';

/** Geometry drawn on the map, always EPSG:4326 lon/lat. */
export type DrawnGeometry =
  | { kind: 'polygon'; ring: LonLat[] }
  | { kind: 'path'; coords: LonLat[]; distanceMetres: number }
  | { kind: 'point'; coord: LonLat; distanceMetres: number };

export interface BoundingBox {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}

/** Rough metres per degree of latitude. Constant enough for widening a bounding box. */
const METRES_PER_DEGREE_LAT = 111320;

/** Identifiers Drill will accept without quoting, and that cannot escape a string. */
const SAFE_COLUMN = /^[A-Za-z_][A-Za-z0-9_]*$/;

function coordsOf(geometry: DrawnGeometry): LonLat[] {
  switch (geometry.kind) {
    case 'polygon':
      return geometry.ring;
    case 'path':
      return geometry.coords;
    case 'point':
      return [geometry.coord];
  }
}

function distanceOf(geometry: DrawnGeometry): number {
  return geometry.kind === 'polygon' ? 0 : geometry.distanceMetres;
}

/** Formats a number without trailing zeros, so 42.40 becomes "42.4". */
function num(value: number): string {
  return String(value);
}

function pair([lon, lat]: LonLat): string {
  return `${num(lon)} ${num(lat)}`;
}

/**
 * Well-Known Text for the drawn geometry, in the order `ST_GeomFromText` expects:
 * longitude before latitude throughout.
 */
export function toWkt(geometry: DrawnGeometry): string {
  switch (geometry.kind) {
    case 'polygon': {
      const ring = [...geometry.ring];
      // WKT polygons must close. Callers that drew interactively usually have this
      // already; ones that built a ring by hand often do not.
      const [first] = ring;
      const last = ring[ring.length - 1];
      if (first && last && (first[0] !== last[0] || first[1] !== last[1])) {
        ring.push(first);
      }
      return `POLYGON((${ring.map(pair).join(', ')}))`;
    }
    case 'path':
      return `LINESTRING(${geometry.coords.map(pair).join(', ')})`;
    case 'point':
      return `POINT(${pair(geometry.coord)})`;
  }
}

/**
 * Envelope of the geometry, widened by its distance.
 *
 * The widening is what keeps the prefilter honest: without it, a path's box would clip
 * exactly the rows the distance test was meant to include, and the "optimisation" would
 * change the answer. The metres-to-degrees conversion is approximate on purpose and
 * only ever widens, so it can never exclude a matching row.
 */
export function boundingBoxOf(geometry: DrawnGeometry): BoundingBox {
  const coords = coordsOf(geometry);
  const lons = coords.map(([lon]) => lon);
  const lats = coords.map(([, lat]) => lat);

  const box: BoundingBox = {
    minLon: Math.min(...lons),
    maxLon: Math.max(...lons),
    minLat: Math.min(...lats),
    maxLat: Math.max(...lats),
  };

  const metres = distanceOf(geometry);
  if (metres <= 0) {
    return box;
  }

  const latPad = metres / METRES_PER_DEGREE_LAT;
  // Degrees of longitude shrink towards the poles. Use the latitude furthest from the
  // equator so the padding is generous rather than tight.
  const worstLat = Math.max(Math.abs(box.minLat), Math.abs(box.maxLat));
  const lonScale = Math.max(Math.cos((worstLat * Math.PI) / 180), 0.01);
  const lonPad = metres / (METRES_PER_DEGREE_LAT * lonScale);

  return {
    minLon: box.minLon - lonPad,
    maxLon: box.maxLon + lonPad,
    minLat: box.minLat - latPad,
    maxLat: box.maxLat + latPad,
  };
}

function assertSafeColumn(name: string): void {
  if (!SAFE_COLUMN.test(name)) {
    throw new Error(
      `Unsupported column name for a geo filter: "${name}". Column names must be plain `
      + 'identifiers.',
    );
  }
}

/**
 * Wraps a query in a spatial filter built from drawn geometry.
 *
 * Wrapping rather than editing means no SQL parsing, and the predicate composes with
 * whatever WHERE, GROUP BY or LIMIT the original already had.
 *
 * A polygon becomes `ST_Within`. A path or point becomes `ST_DWithin` with both sides
 * projected into the UTM zone of the geometry's centroid, because the distance argument
 * is in the units of the coordinate system — against raw lon/lat, "500" would mean 500
 * degrees.
 *
 * See docs/dev/GeoFilterBuilder.md.
 */
export function buildGeoFilterSql(
  originalSql: string,
  mapping: GeoMapping,
  geometry: DrawnGeometry,
): string {
  const metres = distanceOf(geometry);

  if (mapping.kind === 'geometry' && metres > 0) {
    throw new Error(
      'Distance filters are not supported on a geometry column yet. Use a polygon, or '
      + 'map latitude and longitude columns instead.',
    );
  }

  const wkt = toWkt(geometry);
  const inner = originalSql.trim().replace(/;\s*$/, '');

  let subject: string;
  let bbox: string[] = [];

  if (mapping.kind === 'latlon') {
    assertSafeColumn(mapping.latColumn);
    assertSafeColumn(mapping.lonColumn);
    subject = `ST_Point(t.${mapping.lonColumn}, t.${mapping.latColumn})`;

    // Cheap numeric comparisons first: Drill has no spatial index, so without this
    // every row pays a UDF call. These may also push down to the source.
    const box = boundingBoxOf(geometry);
    bbox = [
      `t.${mapping.lonColumn} BETWEEN ${num(box.minLon)} AND ${num(box.maxLon)}`,
      `t.${mapping.latColumn} BETWEEN ${num(box.minLat)} AND ${num(box.maxLat)}`,
    ];
  } else {
    assertSafeColumn(mapping.geomColumn);
    subject = `ST_GeomFromText(t.${mapping.geomColumn})`;
  }

  let predicate: string;
  if (metres > 0) {
    const srid = utmSridFor(...centroidOf(coordsOf(geometry)));
    predicate =
      `ST_DWithin(ST_Transform(${subject}, 4326, ${srid}), `
      + `ST_Transform(ST_GeomFromText('${wkt}'), 4326, ${srid}), ${num(metres)})`;
  } else {
    predicate = `ST_Within(${subject}, ST_GeomFromText('${wkt}'))`;
  }

  const conditions = [...bbox, predicate];
  return `SELECT * FROM (${inner}) t\nWHERE ${conditions.join('\n  AND ')}`;
}
