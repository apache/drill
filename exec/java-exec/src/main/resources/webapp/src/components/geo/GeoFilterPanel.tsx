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
import { useEffect, useMemo, useState } from 'react';
import { Alert, Button, Empty, InputNumber, Select, Space, Tooltip, Typography } from 'antd';
import {
  BorderOutlined,
  DeleteOutlined,
  EnvironmentOutlined,
  FilterOutlined,
  LineOutlined,
  RadiusSettingOutlined,
} from '@ant-design/icons';
import GeoMapCanvas from './GeoMapCanvas';
import type { DrawMode } from './GeoMapCanvas';
import { getMapConfig } from '../../api/mapConfig';
import type { MapConfig } from '../../api/mapConfig';
import { detectGeoColumns } from '../../utils/geoColumns';
import type { GeoMapping } from '../../utils/geoColumns';
import { buildGeoFilterSql } from '../../utils/geoFilterSql';
import type { DrawnGeometry } from '../../utils/geoFilterSql';
import { spansMultipleUtmZones } from '../../utils/geoProjection';

const { Text, Paragraph } = Typography;

interface GeoFilterPanelProps {
  /** Null until a query has returned. */
  results: { columns: string[]; metadata?: string[] } | null;
  sql: string;
  /** Replaces the editor's SQL with the filtered query and re-runs it. */
  onApplyFilter: (sql: string) => void;
}

const NO_TILES: MapConfig = { tileUrl: '', attribution: '' };

/** Distance is meaningless for geometry that already encloses an area. */
const NEEDS_DISTANCE: DrawMode[] = ['path', 'point'];

function coordsOf(geometry: DrawnGeometry): [number, number][] {
  switch (geometry.kind) {
    case 'polygon':
      return geometry.ring;
    case 'path':
      return geometry.coords;
    case 'point':
      return [geometry.coord];
  }
}

/**
 * Builds a spatial filter by drawing on a map.
 *
 * Enabled only once a query has returned: the result set is how the coordinate columns
 * are known, and there has to be a query to wrap. Results are not plotted — this is a
 * surface for authoring geometry, not for viewing data.
 *
 * See docs/dev/GeoFilterBuilder.md.
 */
export default function GeoFilterPanel({ results, sql, onApplyFilter }: GeoFilterPanelProps) {
  const [config, setConfig] = useState<MapConfig>(NO_TILES);
  const [drawMode, setDrawMode] = useState<DrawMode>('polygon');
  const [geometry, setGeometry] = useState<DrawnGeometry | null>(null);
  const [distanceMetres, setDistanceMetres] = useState(500);
  const [override, setOverride] = useState<GeoMapping | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMapConfig().then((loaded) => {
      if (!cancelled) {
        setConfig(loaded);
      }
    });
    return () => { cancelled = true; };
  }, []);

  const detected = useMemo(
    () => (results ? detectGeoColumns(results.columns, results.metadata) : null),
    [results],
  );
  const mapping = override ?? detected?.mapping ?? null;

  // A new result set invalidates a geometry drawn against the previous one.
  useEffect(() => {
    setGeometry(null);
    setOverride(null);
  }, [results]);

  const filterSql = useMemo(() => {
    if (!mapping || !geometry) {
      return null;
    }
    try {
      return buildGeoFilterSql(sql, mapping, geometry);
    } catch {
      // Unsupported combinations — a distance on a geometry column, an exotic column
      // name — surface as a disabled Apply rather than a crash.
      return null;
    }
  }, [sql, mapping, geometry]);

  const spansZones = geometry ? spansMultipleUtmZones(coordsOf(geometry)) : false;
  const usesDistance = NEEDS_DISTANCE.includes(drawMode);

  if (!results) {
    return (
      <div className="geo-filter-empty">
        <Empty description="Run a query to build a geographic filter from its results." />
      </div>
    );
  }

  const numericColumns = results.columns.filter((_, i) => {
    const type = results.metadata?.[i];
    return !type || /INT|DOUBLE|FLOAT|DECIMAL|NUMERIC/i.test(type);
  });

  return (
    <div className="geo-filter-panel">
      <div className="geo-filter-toolbar">
        <Space size="small" wrap>
          <Tooltip title="Draw a rectangle">
            <Button size="small" icon={<BorderOutlined />}
              type={drawMode === 'rectangle' ? 'primary' : 'default'}
              onClick={() => setDrawMode('rectangle')}>Rectangle</Button>
          </Tooltip>
          <Tooltip title="Draw a polygon">
            <Button size="small" icon={<FilterOutlined />}
              type={drawMode === 'polygon' ? 'primary' : 'default'}
              onClick={() => setDrawMode('polygon')}>Polygon</Button>
          </Tooltip>
          <Tooltip title="Draw a path and filter within a distance of it">
            <Button size="small" icon={<LineOutlined />}
              type={drawMode === 'path' ? 'primary' : 'default'}
              onClick={() => setDrawMode('path')}>Path</Button>
          </Tooltip>
          <Tooltip title="Place a point and filter within a distance of it">
            <Button size="small" icon={<EnvironmentOutlined />}
              type={drawMode === 'point' ? 'primary' : 'default'}
              onClick={() => setDrawMode('point')}>Point</Button>
          </Tooltip>

          {usesDistance && (
            <Space size={4}>
              <RadiusSettingOutlined />
              <label htmlFor="geo-distance">Distance</label>
              <InputNumber id="geo-distance" size="small" min={1} max={1000000}
                value={distanceMetres} onChange={(v) => setDistanceMetres(v ?? 500)}
                addonAfter="m" style={{ width: 130 }} />
            </Space>
          )}

          <Button size="small" icon={<DeleteOutlined />} disabled={!geometry}
            onClick={() => setGeometry(null)}>Clear</Button>

          <Button size="small" type="primary" disabled={!filterSql}
            onClick={() => filterSql && onApplyFilter(filterSql)}>Apply filter</Button>
        </Space>
      </div>

      {!mapping && (
        <Alert
          type="info"
          showIcon
          message="No coordinate columns found"
          description={
            <>
              <Paragraph>
                Looked for a numeric latitude and longitude pair (latitude, lat, y and
                longitude, lon, lng, x) or a geometry column holding WKT. Pick them
                below if they are named something else.
              </Paragraph>
              <Space>
                <label htmlFor="geo-lat">Latitude column</label>
                <Select id="geo-lat" size="small" style={{ width: 160 }}
                  options={numericColumns.map((c) => ({ value: c, label: c }))}
                  onChange={(lat) => setOverride((prev) => ({
                    kind: 'latlon',
                    latColumn: lat,
                    lonColumn: prev?.kind === 'latlon' ? prev.lonColumn : '',
                  }))} />
                <label htmlFor="geo-lon">Longitude column</label>
                <Select id="geo-lon" size="small" style={{ width: 160 }}
                  options={numericColumns.map((c) => ({ value: c, label: c }))}
                  onChange={(lon) => setOverride((prev) => ({
                    kind: 'latlon',
                    latColumn: prev?.kind === 'latlon' ? prev.latColumn : '',
                    lonColumn: lon,
                  }))} />
              </Space>
            </>
          }
        />
      )}

      {spansZones && (
        <Alert
          type="warning"
          showIcon
          message="This shape spans more than one UTM zone"
          description={
            'Distances are measured in a single projected zone chosen from the shape '
            + 'centre, so they lose accuracy towards the edges. Draw a smaller shape for '
            + 'an exact distance filter.'
          }
        />
      )}

      {mapping && (
        <div className="geo-filter-map">
          <GeoMapCanvas
            config={config}
            geometry={geometry}
            onGeometryChange={setGeometry}
            drawMode={drawMode}
            distanceMetres={distanceMetres}
          />
        </div>
      )}

      {filterSql && (
        <div className="geo-filter-preview">
          <Text type="secondary">Filter to apply</Text>
          <pre>{filterSql}</pre>
        </div>
      )}
    </div>
  );
}
