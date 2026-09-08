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
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';

// jsdom gives every element zero size and has no canvas, so OpenLayers cannot actually
// render. Mock the map itself and assert on the wiring — what this component gets wrong
// will be projections and layer setup, not whether OL paints pixels.
const addLayer = vi.hoisted(() => vi.fn());
const addInteraction = vi.hoisted(() => vi.fn());
const removeInteraction = vi.hoisted(() => vi.fn());
const setTarget = vi.hoisted(() => vi.fn());

vi.mock('ol/Map', () => ({
  default: vi.fn(() => ({
    addLayer,
    removeLayer: vi.fn(),
    addInteraction,
    removeInteraction,
    setTarget,
    dispose: vi.fn(),
    getView: () => ({ fit: vi.fn(), setCenter: vi.fn(), setZoom: vi.fn() }),
  })),
}));

import GeoMapCanvas from './GeoMapCanvas';
import { fromLonLat, toLonLat } from 'ol/proj';

const CONFIG = { tileUrl: '', attribution: '' };
const base = {
  config: CONFIG,
  geometry: null,
  onGeometryChange: vi.fn(),
  drawMode: 'none' as const,
  distanceMetres: 500,
};

describe('GeoMapCanvas', () => {
  beforeEach(() => vi.clearAllMocks());

  it('renders a map container', () => {
    const { container } = render(<GeoMapCanvas {...base} />);
    expect(container.querySelector('.geo-map-canvas')).not.toBeNull();
  });

  /** No tile URL configured means bundled vectors only — nothing leaves the network. */
  it('adds no raster layer when no tile url is configured', () => {
    render(<GeoMapCanvas {...base} />);
    const layerNames = addLayer.mock.calls.map(([layer]) => layer?.constructor?.name);
    expect(layerNames).not.toContain('TileLayer');
  });

  it('adds a raster layer when a tile url is configured', () => {
    render(<GeoMapCanvas {...base}
      config={{ tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: '© T' }} />);
    const layerNames = addLayer.mock.calls.map(([layer]) => layer?.constructor?.name);
    expect(layerNames).toContain('TileLayer');
  });

  /** Modify is always attached so a drawn shape stays editable; Draw is not. */
  const drawCalls = () =>
    addInteraction.mock.calls.filter(([i]) => i?.constructor?.name === 'Draw');

  it('adds no draw interaction when no mode is active', () => {
    render(<GeoMapCanvas {...base} />);
    expect(drawCalls()).toHaveLength(0);
  });

  it('always attaches Modify so a drawn shape can be adjusted', () => {
    render(<GeoMapCanvas {...base} />);
    const names = addInteraction.mock.calls.map(([i]) => i?.constructor?.name);
    expect(names).toContain('Modify');
  });

  it.each(['rectangle', 'polygon', 'path', 'point'] as const)(
    'adds a draw interaction for %s', (mode) => {
      render(<GeoMapCanvas {...base} drawMode={mode} />);
      expect(drawCalls()).toHaveLength(1);
    });

  /** Switching modes must not leave the previous interaction attached. */
  it('removes the previous draw interaction when the mode changes', () => {
    const { rerender } = render(<GeoMapCanvas {...base} drawMode="polygon" />);
    rerender(<GeoMapCanvas {...base} drawMode="path" />);
    expect(removeInteraction).toHaveBeenCalled();
  });

  /** OpenLayers holds DOM nodes and listeners that leak across tab switches. */
  it('detaches the map on unmount', () => {
    const { unmount } = render(<GeoMapCanvas {...base} />);
    unmount();
    expect(setTarget).toHaveBeenCalledWith(undefined);
  });
});

/**
 * Pins the projection contract the component depends on. The map view is Web Mercator
 * but everything crossing this component's boundary is EPSG:4326 lon/lat, and getting
 * that backwards produces coordinates that are numerically plausible and
 * geographically wrong.
 */
describe('projection contract', () => {
  it('fromLonLat takes lon first and returns Web Mercator metres', () => {
    const [x, y] = fromLonLat([-71.06, 42.36]);
    expect(x).toBeCloseTo(-7910240, -3);
    expect(y).toBeCloseTo(5214932, -3);
  });

  it('round-trips back to the original lon/lat', () => {
    const [lon, lat] = toLonLat(fromLonLat([-71.06, 42.36]));
    expect(lon).toBeCloseTo(-71.06, 6);
    expect(lat).toBeCloseTo(42.36, 6);
  });
});
