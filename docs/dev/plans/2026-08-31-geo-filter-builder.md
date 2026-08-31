# Geospatial Filter Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an analyst draw geometry on a map — rectangle, polygon, path or point — and have it pushed into the current query as a Drill `ST_` predicate. The map is a filter authoring surface, not a results viewer: nothing is plotted on it.

**Architecture:** A Map tab in the SQL Lab results panel, enabled only once a query has returned results — the result set supplies the coordinate columns, and there is a query to wrap. OpenLayers draws over the GeoJSON basemaps the app already bundles. Drawn geometry is serialised to WKT and composed into a wrapping query — `SELECT * FROM (<original>) t WHERE <bbox> AND <predicate>` — which replaces the editor's SQL and re-runs. Detection, projection maths and SQL generation are pure functions, testable without a map.

**Tech Stack:** OpenLayers 9 (BSD-2), React 18, Redux Toolkit, TypeScript, antd, Vitest on the frontend; Java / JAX-RS + Drill `PersistentStore` for the tile config.

**Spec:** [`../GeoFilterBuilder.md`](../GeoFilterBuilder.md)

## Global Constraints

- After modifying anything in `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. Braces on every `if`; no unused imports.
- Apache 2.0 license header on every new source file (Java, TS, TSX, CSS).
- Do not add Claude as a git co-author. Imperative commit messages.
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`.
- **`ST_Point` takes longitude first, then latitude.** Getting this backwards returns zero rows instead of erroring, so every task that emits or asserts on generated SQL must have the order pinned by a test.
- **Never pass a metre distance to `ST_DWithin` on unprojected geometry.** Its distance argument is in the units of the coordinate system — degrees for raw lon/lat — so `500` would mean 500 degrees and match everything. Distances go through `ST_Transform` into the UTM zone of the shape's centroid (Task 2). Web Mercator is not an acceptable substitute: its metres are stretched by `1/cos(latitude)`.
- OpenLayers renders into a DOM element it measures on creation. In tests, jsdom reports zero size for everything, so the map must be constructed behind a guard or mocked — see Task 5.

---

## Phase 1 — Detection and SQL generation

Both are pure functions with no map, no network and no React, because they are where a
silent wrong answer would come from.

### Task 1: Detect coordinate columns in a result set

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/geoColumns.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/geoColumns.test.ts`

**Interfaces:**
- Consumes: nothing.
- Produces:

```ts
export type GeoMapping =
  | { kind: 'latlon'; latColumn: string; lonColumn: string }
  | { kind: 'geometry'; geomColumn: string };

export interface GeoColumnCandidate {
  mapping: GeoMapping;
  /** True when matched by name; false when the caller picked it. */
  inferred: boolean;
}

export function detectGeoColumns(
  columns: string[],
  metadata?: string[],
): GeoColumnCandidate | null;
```

`metadata` is the per-column type array Drill already returns alongside `columns` in a
`QueryResult`. When present, a lat/lon pair is only accepted on numeric columns.

- [ ] **Step 1: Write the failing test**

```ts
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
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/geoColumns.test.ts`
Expected: FAIL — cannot resolve `./geoColumns`.

- [ ] **Step 3: Write the implementation**

Match names case-insensitively against ordered candidate lists so `latitude` wins over
`lat` when both exist:

```ts
const LAT_NAMES = ['latitude', 'lat', 'y'];
const LON_NAMES = ['longitude', 'lon', 'lng', 'long', 'x'];
const GEOM_NAMES = ['geom', 'geometry', 'wkt', 'shape'];
const NUMERIC = /INT|DOUBLE|FLOAT|DECIMAL|NUMERIC|BIGINT|SMALLINT/i;
```

Treat `x`/`y` as valid only as a pair, since a column called `x` alone is far more
likely to be something else.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/geoColumns.test.ts`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/geoColumns.ts src/utils/geoColumns.test.ts
git commit -m "Detect coordinate columns in query results"
```

---

### Task 2: Projection maths

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/geoProjection.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/geoProjection.test.ts`

Split from the SQL builder because this is the part that is wrong-but-plausible if it
drifts, and it is worth being able to check in isolation.

**Interfaces:**
- Produces:

```ts
/** EPSG code for the UTM zone containing this point. */
export function utmSridFor(lon: number, lat: number): number;
export function centroidOf(coords: [number, number][]): [number, number];
/** True when the coordinates span more than one UTM zone. */
export function spansMultipleUtmZones(coords: [number, number][]): boolean;
```

- [ ] **Step 1: Write the failing test**

```ts
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
});

describe('centroidOf', () => {
  it('averages the coordinates', () => {
    expect(centroidOf([[0, 0], [2, 0], [2, 2], [0, 2]])).toEqual([1, 1]);
  });
});

describe('spansMultipleUtmZones', () => {
  /** UTM is only valid near its zone, so a wide shape must warn rather than lie. */
  it('detects a shape crossing a zone boundary', () => {
    expect(spansMultipleUtmZones([[-73, 42], [-71, 42]])).toBe(false);
    expect(spansMultipleUtmZones([[-73, 42], [-59, 42]])).toBe(true);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/geoProjection.test.ts`
Expected: FAIL — cannot resolve `./geoProjection`.

- [ ] **Step 3: Write the implementation**

```ts
export function utmSridFor(lon: number, lat: number): number {
  const zone = Math.floor((lon + 180) / 6) + 1;
  return (lat >= 0 ? 32600 : 32700) + zone;
}
```

Clamp the zone to 1..60 so a longitude of exactly 180 does not produce zone 61.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/geoProjection.test.ts`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/geoProjection.ts src/utils/geoProjection.test.ts
git commit -m "Add UTM zone selection for metric distance filters"
```

---

### Task 3: Generate the spatial filter SQL

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/geoFilterSql.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/geoFilterSql.test.ts`

**Interfaces:**
- Consumes: `GeoMapping` (Task 1), `utmSridFor` / `centroidOf` (Task 2).
- Produces:

```ts
/** Drawn geometry, always EPSG:4326 lon/lat. */
export type DrawnGeometry =
  | { kind: 'polygon'; ring: [number, number][] }
  | { kind: 'path'; coords: [number, number][]; distanceMetres: number }
  | { kind: 'point'; coord: [number, number]; distanceMetres: number };

export interface BoundingBox {
  minLon: number; maxLon: number; minLat: number; maxLat: number;
}

export function boundingBoxOf(geometry: DrawnGeometry): BoundingBox;
export function toWkt(geometry: DrawnGeometry): string;
export function buildGeoFilterSql(
  originalSql: string,
  mapping: GeoMapping,
  geometry: DrawnGeometry,
): string;
```

- [ ] **Step 1: Write the failing test**

```ts
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
   * A path's box must be widened by the distance or the prefilter would exclude rows
   * the spatial predicate would have matched — turning an optimisation into a wrong
   * answer.
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
   * The whole point of Task 2: a metre distance is only meaningful in a projected CRS.
   * Passing 500 against unprojected lon/lat would mean 500 degrees and match the planet.
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

  it('filters a geometry column through ST_GeomFromText', () => {
    const sql = buildGeoFilterSql(
      'SELECT * FROM t', { kind: 'geometry', geomColumn: 'shape' }, BOSTON_BOX,
    );
    expect(sql).toContain('ST_Within(ST_GeomFromText(t.shape)');
    expect(sql).not.toContain('ST_Point');
  });

  /** No raw numeric columns exist to compare, so no prefilter is possible. */
  it('omits the bounding box for a geometry column', () => {
    const sql = buildGeoFilterSql(
      'SELECT * FROM t', { kind: 'geometry', geomColumn: 'shape' }, BOSTON_BOX,
    );
    expect(sql).not.toContain('BETWEEN');
  });

  /** Distance on a geometry column is out of scope for the first version. */
  it('refuses a distance filter on a geometry column', () => {
    expect(() => buildGeoFilterSql(
      'SELECT * FROM t', { kind: 'geometry', geomColumn: 'shape' }, PIN,
    )).toThrow(/geometry column/i);
  });

  it('strips a trailing semicolon from the original query', () => {
    expect(buildGeoFilterSql('SELECT * FROM t;', LATLON, BOSTON_BOX)).not.toContain(';) t');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/geoFilterSql.test.ts`
Expected: FAIL — cannot resolve `./geoFilterSql`.

- [ ] **Step 3: Write the implementation**

Format coordinates with `String(n)` so `42.40` renders as `42.4`. For the distance box
expansion, convert metres to degrees with `metres / 111320` for latitude and
`metres / (111320 * Math.cos(lat * Math.PI / 180))` for longitude — approximate on
purpose, and only ever widening the box, so it can never exclude a matching row.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/geoFilterSql.test.ts`
Expected: PASS (15 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/geoFilterSql.ts src/utils/geoFilterSql.test.ts
git commit -m "Generate spatial filter SQL from drawn geometry"
```

---

## Phase 2 — Tile configuration

### Task 4: Map config store and endpoints

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MapConfigResources.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/DrillRestServer.java` — `register(MapConfigResources.class)`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMapConfigResources.java`

**Interfaces:**
- Produces: `GET`/`POST` `/api/v1/map/config` returning `{ tileUrl, attribution }`.

Modelled on `ProfileConfigResources`, which is the smallest existing example of a
config-in-`PersistentStore` resource. Store name `drill.sqllab.map_config`, single
well-known key. Resource registration is required — a JAX-RS class that is not
registered simply 404s.

- [ ] **Step 1: Write the failing test**

```java
@Test
public void testDefaultsToNoTileServer() throws Exception {
  JsonNode config = get("/api/v1/map/config");
  assertEquals("", config.get("tileUrl").asText());
}

@Test
public void testTileUrlRoundTrips() throws Exception {
  post("/api/v1/map/config",
      "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\","
          + "\"attribution\":\"© Example\"}");

  JsonNode config = get("/api/v1/map/config");
  assertEquals("https://tiles.example.com/{z}/{x}/{y}.png", config.get("tileUrl").asText());
  assertEquals("© Example", config.get("attribution").asText());
}

/**
 * Most tile providers require credit as a condition of use, so a URL without an
 * attribution is a licence problem waiting to happen. Rejected rather than defaulted:
 * only the admin knows what the provider requires.
 */
@Test
public void testTileUrlWithoutAttributionIsRejected() throws Exception {
  RequestBody body = RequestBody.create(
      "{\"tileUrl\":\"https://tiles.example.com/{z}/{x}/{y}.png\"}", JSON);
  Request request = new Request.Builder().url(url("/api/v1/map/config")).post(body).build();
  try (Response response = httpClient.newCall(request).execute()) {
    assertEquals(400, response.code());
  }
}

/** Clearing the URL returns to bundled vectors and needs no attribution. */
@Test
public void testClearingTheTileUrlIsAllowed() throws Exception {
  post("/api/v1/map/config", "{\"tileUrl\":\"\",\"attribution\":\"\"}");
  assertEquals("", get("/api/v1/map/config").get("tileUrl").asText());
}
```

Copy the `url`/`get`/`post` helpers from `TestProjectSchemaCacheEndpoints`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestMapConfigResources`
Expected: FAIL — 404 from every call.

- [ ] **Step 3: Write the implementation**

```java
public static class MapConfig {
  @JsonProperty private String tileUrl;      // "" means bundled vector basemap only
  @JsonProperty private String attribution;  // required whenever tileUrl is set
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl exec/java-exec test -Dtest=TestMapConfigResources`
Expected: PASS (4 tests)

- [ ] **Step 5: Checkstyle and commit**

```bash
mvn -o checkstyle:check -pl exec/java-exec
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MapConfigResources.java \
        exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/DrillRestServer.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMapConfigResources.java
git commit -m "Add map tile configuration"
```

---

### Task 5: Map config API client

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/api/mapConfig.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/api/mapConfig.test.ts`

**Interfaces:**
- Produces:

```ts
export interface MapConfig { tileUrl: string; attribution: string; }
export function getMapConfig(): Promise<MapConfig>;
```

- [ ] **Step 1: Write the failing test**

```ts
const mockGet = vi.hoisted(() => vi.fn());
vi.mock('./client', () => ({ default: { get: mockGet } }));
import { getMapConfig } from './mapConfig';

it('returns the configured tile url', async () => {
  mockGet.mockResolvedValue({ data: { tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: 'c' } });
  await expect(getMapConfig()).resolves.toMatchObject({ tileUrl: 'https://t/{z}/{x}/{y}.png' });
});

/** An unreachable config must fall back to the bundled basemap, not a broken map. */
it('falls back to no tile server when the request fails', async () => {
  mockGet.mockRejectedValue(new Error('network'));
  await expect(getMapConfig()).resolves.toEqual({ tileUrl: '', attribution: '' });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/api/mapConfig.test.ts`
Expected: FAIL — cannot resolve `./mapConfig`.

- [ ] **Step 3: Write the implementation**

Follow `src/api/projects.ts`; catch and return the empty config.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run src/api/mapConfig.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/api/mapConfig.ts src/api/mapConfig.test.ts
git commit -m "Add map config API client"
```

---

## Phase 3 — The map

### Task 6: Map canvas with draw tools

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/package.json` — add `ol`
- Create: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoMapCanvas.tsx`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoMapCanvas.test.tsx`

**Interfaces:**
- Consumes: `MapConfig` (Task 5), `DrawnGeometry` (Task 3).
- Produces:

```ts
interface GeoMapCanvasProps {
  config: MapConfig;
  geometry: DrawnGeometry | null;
  onGeometryChange: (geometry: DrawnGeometry | null) => void;
  drawMode: 'rectangle' | 'polygon' | 'path' | 'point' | 'none';
  /** Metres, applied to path and point geometry. */
  distanceMetres: number;
}
export default function GeoMapCanvas(props: GeoMapCanvasProps): JSX.Element;
```

Query results are **not** plotted — this is a drawing surface. The only vector data is
the bundled basemap and whatever the analyst has drawn.

The map view is Web Mercator (EPSG:3857) because that is what the basemap and tiles use,
but everything crossing this component's boundary is EPSG:4326 lon/lat. Convert with
`ol/proj`'s `fromLonLat` on the way in and `toLonLat` on the way out. Getting this wrong
does not throw — it produces coordinates that are numerically plausible and
geographically wrong, which is why the conversion happens in exactly one place each way.

Install: `npm install ol` (BSD-2, ASF Category A).

- [ ] **Step 1: Write the failing test**

jsdom gives every element zero size and has no canvas, so OpenLayers cannot render. Mock
the library and assert on the wiring — what this component gets wrong will be
projections and prop plumbing, not whether OL draws pixels.

```tsx
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';

const addLayer = vi.hoisted(() => vi.fn());
const addInteraction = vi.hoisted(() => vi.fn());
vi.mock('ol/Map', () => ({
  default: vi.fn(() => ({
    addLayer, addInteraction, setTarget: vi.fn(), dispose: vi.fn(),
    getView: () => ({ fit: vi.fn() }),
  })),
}));

import GeoMapCanvas from './GeoMapCanvas';
import { fromLonLat } from 'ol/proj';

const CONFIG = { tileUrl: '', attribution: '' };

describe('GeoMapCanvas', () => {
  beforeEach(() => vi.clearAllMocks());

  const base = {
    config: CONFIG, geometry: null, onGeometryChange: vi.fn(),
    drawMode: 'none' as const, distanceMetres: 500,
  };

  it('renders a map container', () => {
    const { container } = render(<GeoMapCanvas {...base} />);
    expect(container.querySelector('.geo-map-canvas')).not.toBeNull();
  });

  /** No tile URL configured means bundled vectors only — nothing leaves the network. */
  it('adds no raster layer when no tile url is configured', () => {
    render(<GeoMapCanvas {...base} />);
    const layerTypes = addLayer.mock.calls.map(([l]) => l?.constructor?.name);
    expect(layerTypes).not.toContain('TileLayer');
  });

  it('adds a raster layer when a tile url is configured', () => {
    render(<GeoMapCanvas {...base}
      config={{ tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: '© T' }} />);
    expect(addLayer).toHaveBeenCalled();
  });

  it('adds a draw interaction only when a draw mode is active', () => {
    const { rerender } = render(<GeoMapCanvas {...base} />);
    const before = addInteraction.mock.calls.length;
    rerender(<GeoMapCanvas {...base} drawMode="polygon" />);
    expect(addInteraction.mock.calls.length).toBeGreaterThan(before);
  });

  it.each(['rectangle', 'polygon', 'path', 'point'] as const)(
    'supports drawing a %s', (mode) => {
      const { rerender } = render(<GeoMapCanvas {...base} />);
      const before = addInteraction.mock.calls.length;
      rerender(<GeoMapCanvas {...base} drawMode={mode} />);
      expect(addInteraction.mock.calls.length).toBeGreaterThan(before);
    });
});

/** Pins the projection contract this component depends on. */
describe('projection', () => {
  it('fromLonLat takes lon first and returns Web Mercator metres', () => {
    const [x, y] = fromLonLat([-71.06, 42.36]);
    expect(x).toBeCloseTo(-7910240, -3);
    expect(y).toBeCloseTo(5214932, -3);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/geo/GeoMapCanvas.test.tsx`
Expected: FAIL — cannot resolve `ol/Map` or `./GeoMapCanvas`.

- [ ] **Step 3: Write the implementation**

Build the map in a `useEffect` keyed on the container ref. Layers, in order: bundled
GeoJSON vector basemap (fetched from `/geojson/world.json`, as `ChartPreview` already
does), optional `TileLayer` when `config.tileUrl` is set, and the drawn-geometry layer.

`ol/interaction/Draw` types map to the modes: `Polygon`, `Polygon` with
`geometryFunction: createBox()`, `LineString`, and `Point`. Add `ol/interaction/Modify`
so drawn geometry can be adjusted rather than redrawn — that iteration is the reason
OpenLayers was chosen over ECharts brush.

For path and point modes, render the distance as a translucent buffer around the drawn
geometry so the analyst can see what they are selecting. Compute it for display only;
the authoritative distance goes to Drill in the SQL.

Dispose the map on unmount — OpenLayers holds DOM nodes and event listeners that leak
across tab switches otherwise.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/geo/GeoMapCanvas.test.tsx`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add package.json package-lock.json src/components/geo/GeoMapCanvas.tsx \
        src/components/geo/GeoMapCanvas.test.tsx
git commit -m "Add an OpenLayers map canvas with draw tools"
```

---

### Task 7: The Map tab

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoFilterPanel.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx` — add the tab at the results `items` array (alongside `key: 'results'` and `key: 'notebook'`)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoFilterPanel.test.tsx`

**Interfaces:**
- Consumes: `detectGeoColumns` (Task 1), `buildGeoFilterSql` (Task 3), `getMapConfig` (Task 5), `GeoMapCanvas` (Task 6).
- Produces:

```ts
interface GeoFilterPanelProps {
  /** Null until a query has returned; the tab is disabled before then. */
  results: { columns: string[]; metadata?: string[] } | null;
  sql: string;
  /** Replaces the editor's SQL with the filtered query and re-runs it. */
  onApplyFilter: (sql: string) => void;
}
```

Rows are deliberately absent: nothing is plotted. The panel needs the result *shape* to
resolve coordinate columns, not the data.

`GeoMapCanvas` is mocked here — the panel's job is gating, detection, the column
override and producing the right SQL, none of which need a real map.

- [ ] **Step 1: Write the failing test**

```tsx
vi.mock('./GeoMapCanvas', () => ({ default: () => <div data-testid="map" /> }));
vi.mock('../../api/mapConfig', () => ({
  getMapConfig: vi.fn(() => Promise.resolve({ tileUrl: '', attribution: '' })),
}));

import GeoFilterPanel from './GeoFilterPanel';

const GEO_RESULTS = {
  columns: ['id', 'lat', 'lon'],
  metadata: ['VARCHAR', 'DOUBLE', 'DOUBLE'],
};

/** The builder filters a query, so there has to be one that has run. */
it('explains itself when no query has run yet', () => {
  render(<GeoFilterPanel results={null} sql="" onApplyFilter={vi.fn()} />);
  expect(screen.getByText(/run a query/i)).toBeInTheDocument();
  expect(screen.queryByTestId('map')).not.toBeInTheDocument();
});

it('renders the map when coordinates are detected', async () => {
  render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
    onApplyFilter={vi.fn()} />);
  expect(await screen.findByTestId('map')).toBeInTheDocument();
});

/** An empty map with no explanation is the worst version of this. */
it('explains itself when no coordinate columns are found', () => {
  render(<GeoFilterPanel results={{ columns: ['id', 'total'], metadata: ['VARCHAR', 'DOUBLE'] }}
    sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  expect(screen.getByText(/latitude/i)).toBeInTheDocument();
  expect(screen.queryByTestId('map')).not.toBeInTheDocument();
});

it('lets the user pick columns when detection was wrong', async () => {
  render(<GeoFilterPanel
    results={{ columns: ['id', 'a', 'b'], metadata: ['VARCHAR', 'DOUBLE', 'DOUBLE'] }}
    sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  expect(screen.getByLabelText(/latitude column/i)).toBeInTheDocument();
  expect(screen.getByLabelText(/longitude column/i)).toBeInTheDocument();
});

/** Apply before drawing would produce invalid SQL. */
it('disables Apply until geometry exists', async () => {
  render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
    onApplyFilter={vi.fn()} />);
  await screen.findByTestId('map');
  expect(screen.getByRole('button', { name: /apply filter/i })).toBeDisabled();
});

/** Distance only appears for the geometry types it means something for. */
it('shows a distance control for path and point only', async () => {
  render(<GeoFilterPanel results={GEO_RESULTS} sql="SELECT * FROM t"
    onApplyFilter={vi.fn()} />);
  await screen.findByTestId('map');

  fireEvent.click(screen.getByRole('button', { name: /polygon/i }));
  expect(screen.queryByLabelText(/distance/i)).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole('button', { name: /path/i }));
  expect(screen.getByLabelText(/distance/i)).toBeInTheDocument();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/geo/GeoFilterPanel.test.tsx`
Expected: FAIL — cannot resolve `./GeoFilterPanel`.

- [ ] **Step 3: Write the implementation**

Gate in three stages, each with its own message: no query has run, a query ran but no
coordinate columns were found, or ready to draw. The middle case names the columns it
looked for and offers the manual override, since "we could not find latitude" with no
way to correct it is a dead end.

Distance is metres, entered in metres or kilometres. Only render the control for path
and point modes — attaching a distance to a polygon would suggest a buffer this version
does not build.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/geo/GeoFilterPanel.test.tsx`
Expected: PASS (6 tests)

- [ ] **Step 5: Wire the tab, then commit**

Add to the results panel `items` array in `SqlLabPage.tsx`, after `notebook`:

```tsx
{
  key: 'geo',
  label: <span><GlobalOutlined /> Map</span>,
  children: (
    <GeoFilterPanel
      results={results ? { columns: results.columns, metadata: results.metadata } : null}
      sql={sql}
      onApplyFilter={(filtered) => {
        updateSql(filtered);
        handleExecute();
      }}
    />
  ),
}
```

```bash
npx tsc --noEmit && npx eslint src && npx vitest run && npm run build
git add src/components/geo/GeoFilterPanel.tsx src/components/geo/GeoFilterPanel.test.tsx \
        src/pages/SqlLabPage.tsx
git commit -m "Add a Map tab with a geospatial filter builder"
```

---

## Final verification

- [ ] `mvn -o checkstyle:check -pl exec/java-exec`
- [ ] `mvn -o -pl exec/java-exec test -Dtest=TestMapConfigResources`
- [ ] `npx tsc --noEmit && npx eslint src && npx vitest run && npm run build`
- [ ] Confirm the Map tab is lazy-loaded — check that `ol` lands in its own chunk and is absent from the initial bundle
- [ ] Update `docs/dev/ui/pages/sql-lab.md` with the Map tab, and the doc table in `CLAUDE.md`
- [ ] Manual, and worth doing carefully — the failure mode here is a plausible wrong answer, not an error:
  - Run a query with lat/lon columns, draw a rectangle over a city you know is in the data, apply, and confirm the returned rows fall inside it.
  - Draw the same box in the opposite hemisphere and confirm zero rows. If it returns data, longitude and latitude are swapped somewhere.
  - Draw a path with a 500 m distance and confirm the matched rows are genuinely within about 500 m, not 500 km. Check at a high latitude too, where a Web Mercator mistake would show up worst.
  - Confirm the Map tab is disabled before any query has run.

## Deferred

- **Plotting query results** on the map. This is a drawing surface; showing the
  auto-limited sample would imply the filter applies only to what is displayed.
- **Multiple shapes** in one filter, via `ST_Union` / `ST_Difference`.
- **Saved shapes** reusable across queries.
- **Distance on geometry columns** — no raw numeric columns to prefilter on, and the
  transform path needs more thought.
- **Shapes spanning several UTM zones**, which Task 2's `spansMultipleUtmZones` detects
  so the UI can warn rather than quietly returning a distorted answer.
