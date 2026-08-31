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
import { boundingBoxOf, toWkt, buildGeoFilterSql } from './geoFilterSql';
import type { DrawnGeometry } from './geoFilterSql';

const BOSTON_BOX: DrawnGeometry = {
  kind: 'polygon',
  ring: [
    [-71.19, 42.28], [-70.99, 42.28], [-70.99, 42.40], [-71.19, 42.40], [-71.19, 42.28],
  ],
};
const ROUTE: DrawnGeometry = {
  kind: 'path',
  coords: [[-71.10, 42.35], [-71.05, 42.36]],
  distanceMetres: 500,
};
const PIN: DrawnGeometry = {
  kind: 'point', coord: [-71.06, 42.36], distanceMetres: 1000,
};
const LATLON = { kind: 'latlon', latColumn: 'lat', lonColumn: 'lon' } as const;
const GEOM = { kind: 'geometry', geomColumn: 'shape' } as const;

describe('toWkt', () => {
  it('emits a closed POLYGON with lon before lat', () => {
    expect(toWkt(BOSTON_BOX)).toBe(
      'POLYGON((-71.19 42.28, -70.99 42.28, -70.99 42.4, -71.19 42.4, -71.19 42.28))',
    );
  });

  it('closes a ring the caller left open', () => {
    expect(toWkt({ kind: 'polygon', ring: [[0, 0], [1, 0], [1, 1]] }))
      .toBe('POLYGON((0 0, 1 0, 1 1, 0 0))');
  });

  it('emits a LINESTRING for a path', () => {
    expect(toWkt(ROUTE)).toBe('LINESTRING(-71.1 42.35, -71.05 42.36)');
  });

  it('emits a POINT', () => {
    expect(toWkt(PIN)).toBe('POINT(-71.06 42.36)');
  });
});

describe('boundingBoxOf', () => {
  it('takes the extremes of a polygon ring', () => {
    expect(boundingBoxOf(BOSTON_BOX)).toEqual({
      minLon: -71.19, maxLon: -70.99, minLat: 42.28, maxLat: 42.40,
    });
  });

  /**
   * A path's box must be widened by the distance, or the cheap prefilter would exclude
   * rows the spatial predicate would have matched — turning an optimisation into a
   * wrong answer.
   */
  it('expands a path box by the distance', () => {
    const box = boundingBoxOf(ROUTE);
    expect(box.minLon).toBeLessThan(-71.10);
    expect(box.maxLat).toBeGreaterThan(42.36);
  });

  it('expands a point box by the distance', () => {
    const box = boundingBoxOf(PIN);
    expect(box.minLat).toBeLessThan(42.36);
    expect(box.maxLon).toBeGreaterThan(-71.06);
  });

  /** 1 km is roughly 0.009 degrees of latitude — a sanity check on the conversion. */
  it('expands by roughly the right number of degrees', () => {
    const box = boundingBoxOf(PIN);
    expect(box.maxLat - 42.36).toBeGreaterThan(0.005);
    expect(box.maxLat - 42.36).toBeLessThan(0.02);
  });
});

describe('buildGeoFilterSql', () => {
  /**
   * ST_Point takes longitude FIRST. Reversing it silently returns nothing instead of
   * erroring, so this is the assertion that matters most in the file.
   */
  it('passes longitude before latitude to ST_Point', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON_BOX);
    expect(sql).toContain('ST_Point(t.lon, t.lat)');
    expect(sql).not.toContain('ST_Point(t.lat, t.lon)');
  });

  it('wraps the original query rather than editing it', () => {
    const sql = buildGeoFilterSql('SELECT a, b FROM t WHERE a > 1', LATLON, BOSTON_BOX);
    expect(sql).toContain('FROM (SELECT a, b FROM t WHERE a > 1) t');
  });

  it('emits a bounding-box prefilter before the spatial predicate', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON_BOX);
    expect(sql.indexOf('BETWEEN')).toBeLessThan(sql.indexOf('ST_Within'));
  });

  it('uses ST_Within for a polygon and no distance', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON_BOX);
    expect(sql).toContain('ST_Within');
    expect(sql).not.toContain('ST_DWithin');
    expect(sql).not.toContain('ST_Transform');
  });

  /**
   * The whole point of the projection work: a metre distance is only meaningful in a
   * projected CRS. Passing 500 against unprojected lon/lat would mean 500 degrees.
   */
  it('projects both geometries into UTM before measuring a path distance', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, ROUTE);
    expect(sql).toContain('ST_DWithin(');
    expect(sql).toContain('ST_Transform(ST_Point(t.lon, t.lat), 4326, 32619)');
    expect(sql).toContain('4326, 32619)');
    expect(sql).toContain(', 500)');
  });

  it('projects a point radius the same way', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, PIN);
    expect(sql).toContain('ST_DWithin(');
    expect(sql).toContain('32619');
    expect(sql).toContain(', 1000)');
  });

  /** A southern-hemisphere shape must land in the 327xx band, not 326xx. */
  it('picks the southern UTM band below the equator', () => {
    const sydney: DrawnGeometry = {
      kind: 'point', coord: [151.21, -33.87], distanceMetres: 250,
    };
    expect(buildGeoFilterSql('SELECT * FROM t', LATLON, sydney)).toContain('32756');
  });

  it('filters a geometry column through ST_GeomFromText', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', GEOM, BOSTON_BOX);
    expect(sql).toContain('ST_Within(ST_GeomFromText(t.shape)');
    expect(sql).not.toContain('ST_Point');
  });

  /** No raw numeric columns exist to compare, so no prefilter is possible. */
  it('omits the bounding box for a geometry column', () => {
    expect(buildGeoFilterSql('SELECT * FROM t', GEOM, BOSTON_BOX)).not.toContain('BETWEEN');
  });

  /** Distance on a geometry column is out of scope for the first version. */
  it('refuses a distance filter on a geometry column', () => {
    expect(() => buildGeoFilterSql('SELECT * FROM t', GEOM, PIN)).toThrow(/geometry column/i);
  });

  it('strips a trailing semicolon from the original query', () => {
    expect(buildGeoFilterSql('SELECT * FROM t;', LATLON, BOSTON_BOX)).not.toContain(';) t');
  });

  /** A quote in a column name would otherwise break out of the generated SQL. */
  it('rejects a column name containing a quote', () => {
    expect(() => buildGeoFilterSql(
      'SELECT * FROM t',
      { kind: 'latlon', latColumn: "lat'; DROP TABLE x --", lonColumn: 'lon' },
      BOSTON_BOX,
    )).toThrow(/column name/i);
  });
});
