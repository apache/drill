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
import { utmSridFor, centroidOf, spansMultipleUtmZones } from './geoProjection';

describe('utmSridFor', () => {
  it('maps Boston to UTM zone 19 north', () => {
    expect(utmSridFor(-71.06, 42.36)).toBe(32619);
  });

  it('maps Sydney to UTM zone 56 south', () => {
    expect(utmSridFor(151.21, -33.87)).toBe(32756);
  });

  it('uses the 32700 band below the equator', () => {
    expect(utmSridFor(0, -0.001)).toBe(32731);
    expect(utmSridFor(0, 0.001)).toBe(32631);
  });

  it('handles the antimeridian edges', () => {
    expect(utmSridFor(-180, 0)).toBe(32601);
    expect(utmSridFor(179.9, 0)).toBe(32660);
  });

  /** Longitude exactly 180 would compute zone 61, which does not exist. */
  it('clamps a longitude of exactly 180 to zone 60', () => {
    expect(utmSridFor(180, 0)).toBe(32660);
  });

  it('treats the equator itself as northern', () => {
    expect(utmSridFor(0, 0)).toBe(32631);
  });
});

describe('centroidOf', () => {
  it('averages the coordinates', () => {
    expect(centroidOf([[0, 0], [2, 0], [2, 2], [0, 2]])).toEqual([1, 1]);
  });

  it('returns the point itself for a single coordinate', () => {
    expect(centroidOf([[-71.06, 42.36]])).toEqual([-71.06, 42.36]);
  });
});

describe('spansMultipleUtmZones', () => {
  /**
   * UTM is only valid near its own zone, so a wide shape must warn rather than lie.
   * Zones are 6 degrees wide; zone 19 runs from -72 to -66, so -71 and -70 share it
   * while -73 falls into zone 18.
   */
  it('detects a shape crossing a zone boundary', () => {
    expect(spansMultipleUtmZones([[-71, 42], [-70, 42]])).toBe(false);
    expect(spansMultipleUtmZones([[-73, 42], [-71, 42]])).toBe(true);
    expect(spansMultipleUtmZones([[-73, 42], [-59, 42]])).toBe(true);
  });

  it('is false for a single point', () => {
    expect(spansMultipleUtmZones([[-71.06, 42.36]])).toBe(false);
  });

  /** A shape straddling the equator uses different EPSG bands for the same zone. */
  it('detects a shape crossing the equator', () => {
    expect(spansMultipleUtmZones([[0, -1], [0, 1]])).toBe(true);
  });
});
