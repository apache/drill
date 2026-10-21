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
import { useEffect, useRef } from 'react';
import Map from 'ol/Map';
import View from 'ol/View';
import TileLayer from 'ol/layer/Tile';
import VectorLayer from 'ol/layer/Vector';
import VectorSource from 'ol/source/Vector';
import XYZ from 'ol/source/XYZ';
import GeoJSON from 'ol/format/GeoJSON';
import Draw, { createBox } from 'ol/interaction/Draw';
import Modify from 'ol/interaction/Modify';
import { Style, Stroke, Fill, Circle as CircleStyle } from 'ol/style';
import { fromLonLat, toLonLat } from 'ol/proj';
import type { Type as GeometryType } from 'ol/geom/Geometry';
import type { Coordinate } from 'ol/coordinate';
import type { MapConfig } from '../../api/mapConfig';
import type { DrawnGeometry } from '../../utils/geoFilterSql';
import type { LonLat } from '../../utils/geoProjection';

export type DrawMode = 'rectangle' | 'polygon' | 'path' | 'point' | 'none';

interface GeoMapCanvasProps {
  config: MapConfig;
  geometry: DrawnGeometry | null;
  onGeometryChange: (geometry: DrawnGeometry | null) => void;
  drawMode: DrawMode;
  /** Metres. Applies to path and point geometry only. */
  distanceMetres: number;
}

/** Bundled basemap, already shipped for the ECharts geo charts. */
const BASEMAP_URL = '/geojson/world.json';

const DRAW_TYPES: Record<Exclude<DrawMode, 'none'>, GeometryType> = {
  rectangle: 'Circle', // with createBox(), OpenLayers draws a box from a Circle type
  polygon: 'Polygon',
  path: 'LineString',
  point: 'Point',
};

const DRAWN_STYLE = new Style({
  stroke: new Stroke({ color: '#1f9bff', width: 2 }),
  fill: new Fill({ color: 'rgba(31, 155, 255, 0.15)' }),
  image: new CircleStyle({
    radius: 5,
    fill: new Fill({ color: '#1f9bff' }),
    stroke: new Stroke({ color: '#fff', width: 1.5 }),
  }),
});

const BASEMAP_STYLE = new Style({
  stroke: new Stroke({ color: 'rgba(140, 140, 150, 0.8)', width: 0.8 }),
  fill: new Fill({ color: 'rgba(190, 195, 200, 0.25)' }),
});

/** Web Mercator coordinates to the EPSG:4326 lon/lat everything outside here uses. */
function toLonLatPairs(coords: Coordinate[]): LonLat[] {
  return coords.map((coord) => {
    const [lon, lat] = toLonLat(coord);
    return [lon, lat] as LonLat;
  });
}

/**
 * The drawing surface for the geospatial filter builder.
 *
 * Query results are deliberately not plotted: this is where geometry is authored, not
 * where data is viewed.
 *
 * The map view is Web Mercator because that is what basemaps and tile servers use, but
 * every coordinate crossing this component's boundary is EPSG:4326 lon/lat. The
 * conversion happens in exactly one place each way — `fromLonLat` on the way in,
 * `toLonLat` on the way out — because getting it wrong does not throw. It produces
 * numbers that look like coordinates and point somewhere else entirely.
 *
 * See docs/dev/GeoFilterBuilder.md.
 */
export default function GeoMapCanvas({
  config,
  geometry,
  onGeometryChange,
  drawMode,
  distanceMetres,
}: GeoMapCanvasProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const drawSourceRef = useRef<VectorSource | null>(null);
  const drawInteractionRef = useRef<Draw | null>(null);

  // Kept in a ref so changing the distance does not rebuild the draw interaction
  // mid-gesture, which would cancel whatever the analyst is drawing.
  const distanceRef = useRef(distanceMetres);
  distanceRef.current = distanceMetres;
  const onChangeRef = useRef(onGeometryChange);
  onChangeRef.current = onGeometryChange;

  // Build the map once.
  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }

    const drawSource = new VectorSource();
    drawSourceRef.current = drawSource;

    const map = new Map({
      target: containerRef.current,
      view: new View({ center: fromLonLat([0, 20]), zoom: 2 }),
      layers: [],
    });
    mapRef.current = map;

    // Bundled vector basemap. Fetched rather than imported so it stays out of the
    // main bundle, exactly as ChartPreview does for the ECharts maps.
    fetch(BASEMAP_URL)
      .then((response) => response.json())
      .then((geoJson) => {
        const source = new VectorSource({
          features: new GeoJSON().readFeatures(geoJson, {
            featureProjection: 'EPSG:3857',
          }),
        });
        map.addLayer(new VectorLayer({ source, style: BASEMAP_STYLE }));
      })
      .catch(() => {
        // A missing basemap leaves an empty canvas that can still be drawn on, which
        // is worse-looking but entirely functional.
      });

    map.addLayer(new VectorLayer({ source: drawSource, style: DRAWN_STYLE }));

    map.addInteraction(new Modify({ source: drawSource }));

    return () => {
      // OpenLayers keeps DOM nodes and listeners alive otherwise, and this component
      // mounts and unmounts every time the results tab changes.
      map.setTarget(undefined);
      map.dispose();
      mapRef.current = null;
    };
  }, []);

  // Raster tiles, when an administrator has configured a server.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !config.tileUrl) {
      return;
    }
    const layer = new TileLayer({
      source: new XYZ({ url: config.tileUrl, attributions: config.attribution }),
    });
    // Beneath everything else: basemap context, not the subject.
    map.addLayer(layer);
    layer.setZIndex(-1);

    return () => {
      map.removeLayer(layer);
    };
  }, [config.tileUrl, config.attribution]);

  // Swap the draw interaction when the mode changes.
  useEffect(() => {
    const map = mapRef.current;
    const source = drawSourceRef.current;
    if (!map || !source) {
      return;
    }

    if (drawInteractionRef.current) {
      map.removeInteraction(drawInteractionRef.current);
      drawInteractionRef.current = null;
    }

    if (drawMode === 'none') {
      return;
    }

    const draw = new Draw({
      source,
      type: DRAW_TYPES[drawMode],
      geometryFunction: drawMode === 'rectangle' ? createBox() : undefined,
    });

    draw.on('drawstart', () => {
      // One shape at a time. Multi-shape filters need ST_Union and are deferred.
      source.clear();
    });

    draw.on('drawend', (event) => {
      const geom = event.feature.getGeometry();
      if (!geom) {
        return;
      }
      const metres = distanceRef.current;

      if (drawMode === 'point') {
        const [lon, lat] = toLonLat((geom as unknown as { getCoordinates(): Coordinate })
          .getCoordinates());
        onChangeRef.current({ kind: 'point', coord: [lon, lat], distanceMetres: metres });
        return;
      }

      if (drawMode === 'path') {
        const coords = (geom as unknown as { getCoordinates(): Coordinate[] })
          .getCoordinates();
        onChangeRef.current({
          kind: 'path', coords: toLonLatPairs(coords), distanceMetres: metres,
        });
        return;
      }

      // Rectangle and polygon both produce a Polygon; take the outer ring.
      const rings = (geom as unknown as { getCoordinates(): Coordinate[][] })
        .getCoordinates();
      onChangeRef.current({ kind: 'polygon', ring: toLonLatPairs(rings[0] ?? []) });
    });

    map.addInteraction(draw);
    drawInteractionRef.current = draw;

    return () => {
      map.removeInteraction(draw);
    };
  }, [drawMode]);

  // Clearing the geometry from outside clears the canvas.
  useEffect(() => {
    if (!geometry) {
      drawSourceRef.current?.clear();
    }
  }, [geometry]);

  return <div className="geo-map-canvas" ref={containerRef} />;
}
