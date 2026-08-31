# Geospatial Filter Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an analyst draw a rectangle or polygon on a map of their query results and have that shape pushed into the query as a Drill `ST_` predicate, so the filter is evaluated over the whole dataset rather than the returned rows.

**Architecture:** A Map tab in the SQL Lab results panel, rendering results with OpenLayers over the GeoJSON basemaps the app already bundles. Drawn geometry is serialised to WKT and composed into a wrapping query — `SELECT * FROM (<original>) t WHERE <bbox> AND ST_Within(...)` — which replaces the editor's SQL and re-runs. Pure functions do the detection and SQL generation so both are testable without a map.

**Tech Stack:** OpenLayers 9 (BSD-2), React 18, Redux Toolkit, TypeScript, antd, Vitest on the frontend; Java / JAX-RS + Drill `PersistentStore` for the tile config.

**Spec:** [`../GeoFilterBuilder.md`](../GeoFilterBuilder.md)

## Global Constraints

- After modifying anything in `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. Braces on every `if`; no unused imports.
- Apache 2.0 license header on every new source file (Java, TS, TSX, CSS).
- Do not add Claude as a git co-author. Imperative commit messages.
- Frontend commands run from `exec/java-exec/src/main/resources/webapp`.
- **`ST_Point` takes longitude first, then latitude.** Getting this backwards returns zero rows instead of erroring, so every task that emits or asserts on generated SQL must have the order pinned by a test.
- **Do not emit `ST_DWithin` or any radius filter.** Its distance argument is in degrees for lon/lat data, and a "5 km" control passing `5` is silently wrong. Deferred until it goes through `ST_Transform`.
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

### Task 2: Generate the spatial filter SQL

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/utils/geoFilterSql.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/utils/geoFilterSql.test.ts`

**Interfaces:**
- Consumes: `GeoMapping` from Task 1.
- Produces:

```ts
/** A drawn shape, as EPSG:4326 lon/lat rings. Outer ring first. */
export interface DrawnShape {
  /** [lon, lat] pairs; first and last coordinate must match. */
  ring: [number, number][];
}

export interface BoundingBox {
  minLon: number; maxLon: number; minLat: number; maxLat: number;
}

export function boundingBoxOf(shape: DrawnShape): BoundingBox;
export function toWkt(shape: DrawnShape): string;
export function buildGeoFilterSql(
  originalSql: string,
  mapping: GeoMapping,
  shape: DrawnShape,
): string;
```

- [ ] **Step 1: Write the failing test**

```ts
import { describe, it, expect } from 'vitest';
import { boundingBoxOf, toWkt, buildGeoFilterSql } from './geoFilterSql';
import type { DrawnShape } from './geoFilterSql';

// A small box around Boston, counter-clockwise, explicitly closed.
const BOSTON: DrawnShape = {
  ring: [
    [-71.19, 42.28], [-70.99, 42.28], [-70.99, 42.40], [-71.19, 42.40], [-71.19, 42.28],
  ],
};
const LATLON = { kind: 'latlon', latColumn: 'lat', lonColumn: 'lon' } as const;

describe('boundingBoxOf', () => {
  it('takes the extremes of the ring', () => {
    expect(boundingBoxOf(BOSTON)).toEqual({
      minLon: -71.19, maxLon: -70.99, minLat: 42.28, maxLat: 42.40,
    });
  });

  it('handles a ring that crosses the equator and prime meridian', () => {
    expect(boundingBoxOf({ ring: [[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]] })).toEqual({
      minLon: -1, maxLon: 1, minLat: -1, maxLat: 1,
    });
  });
});

describe('toWkt', () => {
  it('emits a closed POLYGON with lon before lat', () => {
    expect(toWkt(BOSTON)).toBe(
      'POLYGON((-71.19 42.28, -70.99 42.28, -70.99 42.4, -71.19 42.4, -71.19 42.28))',
    );
  });

  it('closes a ring the caller left open', () => {
    const open: DrawnShape = { ring: [[0, 0], [1, 0], [1, 1]] };
    expect(toWkt(open)).toBe('POLYGON((0 0, 1 0, 1 1, 0 0))');
  });
});

describe('buildGeoFilterSql', () => {
  /**
   * ST_Point takes longitude FIRST. Reversing it silently returns nothing instead of
   * erroring, so this assertion is the one that matters most in the file.
   */
  it('passes longitude before latitude to ST_Point', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON);
    expect(sql).toContain('ST_Point(t.lon, t.lat)');
    expect(sql).not.toContain('ST_Point(t.lat, t.lon)');
  });

  it('wraps the original query rather than editing it', () => {
    const sql = buildGeoFilterSql('SELECT a, b FROM t WHERE a > 1', LATLON, BOSTON);
    expect(sql).toContain('FROM (SELECT a, b FROM t WHERE a > 1) t');
  });

  /** The bbox is what keeps this from being a full scan through a UDF. */
  it('emits a bounding-box prefilter before the spatial predicate', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON);
    expect(sql.indexOf('BETWEEN')).toBeLessThan(sql.indexOf('ST_Within'));
    expect(sql).toContain('t.lon BETWEEN -71.19 AND -70.99');
    expect(sql).toContain('t.lat BETWEEN 42.28 AND 42.4');
  });

  it('filters a geometry column through ST_GeomFromText', () => {
    const sql = buildGeoFilterSql(
      'SELECT * FROM t', { kind: 'geometry', geomColumn: 'shape' }, BOSTON,
    );
    expect(sql).toContain('ST_Within(ST_GeomFromText(t.shape)');
    expect(sql).not.toContain('ST_Point');
  });

  /** No bbox is possible without raw numeric columns to compare. */
  it('omits the bounding box for a geometry column', () => {
    const sql = buildGeoFilterSql(
      'SELECT * FROM t', { kind: 'geometry', geomColumn: 'shape' }, BOSTON,
    );
    expect(sql).not.toContain('BETWEEN');
  });

  it('strips a trailing semicolon from the original query', () => {
    const sql = buildGeoFilterSql('SELECT * FROM t;', LATLON, BOSTON);
    expect(sql).not.toContain(';) t');
  });

  /** Radius filters are deferred; nothing should emit ST_DWithin yet. */
  it('never emits ST_DWithin', () => {
    expect(buildGeoFilterSql('SELECT * FROM t', LATLON, BOSTON)).not.toContain('ST_DWithin');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/utils/geoFilterSql.test.ts`
Expected: FAIL — cannot resolve `./geoFilterSql`.

- [ ] **Step 3: Write the implementation**

Format coordinates with `String(n)` so `42.40` renders as `42.4`, matching the tests and
avoiding trailing-zero noise in the generated SQL. Close the ring in `toWkt` when the
caller has not.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/utils/geoFilterSql.test.ts`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils/geoFilterSql.ts src/utils/geoFilterSql.test.ts
git commit -m "Generate spatial filter SQL from a drawn shape"
```

---

## Phase 2 — Tile configuration

### Task 3: Map config store and endpoints

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

### Task 4: Map config API client

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

### Task 5: Map canvas with draw tools

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/package.json` — add `ol`
- Create: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoMapCanvas.tsx`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoMapCanvas.test.tsx`

**Interfaces:**
- Consumes: `MapConfig` (Task 4), `DrawnShape` (Task 2).
- Produces:

```ts
interface GeoMapCanvasProps {
  points: { lon: number; lat: number }[];
  config: MapConfig;
  shape: DrawnShape | null;
  onShapeChange: (shape: DrawnShape | null) => void;
  drawMode: 'rectangle' | 'polygon' | 'none';
}
export default function GeoMapCanvas(props: GeoMapCanvasProps): JSX.Element;
```

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

  it('renders a map container', () => {
    const { container } = render(
      <GeoMapCanvas points={[]} config={CONFIG} shape={null}
        onShapeChange={vi.fn()} drawMode="none" />);
    expect(container.querySelector('.geo-map-canvas')).not.toBeNull();
  });

  /** No tile URL configured means bundled vectors only — nothing leaves the network. */
  it('adds no raster layer when no tile url is configured', () => {
    render(<GeoMapCanvas points={[]} config={CONFIG} shape={null}
      onShapeChange={vi.fn()} drawMode="none" />);
    const layerTypes = addLayer.mock.calls.map(([l]) => l?.constructor?.name);
    expect(layerTypes).not.toContain('TileLayer');
  });

  it('adds a raster layer when a tile url is configured', () => {
    render(<GeoMapCanvas points={[]} shape={null} onShapeChange={vi.fn()} drawMode="none"
      config={{ tileUrl: 'https://t/{z}/{x}/{y}.png', attribution: '© T' }} />);
    expect(addLayer).toHaveBeenCalled();
  });

  it('adds a draw interaction only when a draw mode is active', () => {
    const { rerender } = render(
      <GeoMapCanvas points={[]} config={CONFIG} shape={null}
        onShapeChange={vi.fn()} drawMode="none" />);
    const before = addInteraction.mock.calls.length;

    rerender(<GeoMapCanvas points={[]} config={CONFIG} shape={null}
      onShapeChange={vi.fn()} drawMode="polygon" />);
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
does), optional `TileLayer` when `config.tileUrl` is set, the points layer, and the drawn
shape layer. Add `ol/interaction/Draw` with `type: 'Polygon'` or
`createBox()` for rectangles, plus `ol/interaction/Modify` so a drawn shape can be
adjusted rather than redrawn. Dispose the map on unmount — OpenLayers holds DOM and
event listeners that leak across tab switches otherwise.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/geo/GeoMapCanvas.test.tsx`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add package.json package-lock.json src/components/geo/GeoMapCanvas.tsx \
        src/components/geo/GeoMapCanvas.test.tsx
git commit -m "Add an OpenLayers map canvas with draw tools"
```

---

### Task 6: The Map tab

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoFilterPanel.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx` — add the tab at the results `items` array (alongside `key: 'results'` and `key: 'notebook'`)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/geo/GeoFilterPanel.test.tsx`

**Interfaces:**
- Consumes: `detectGeoColumns` (Task 1), `buildGeoFilterSql` (Task 2), `getMapConfig` (Task 4), `GeoMapCanvas` (Task 5).
- Produces:

```ts
interface GeoFilterPanelProps {
  columns: string[];
  metadata?: string[];
  rows: Record<string, unknown>[];
  sql: string;
  /** Replaces the editor's SQL with the filtered query and re-runs it. */
  onApplyFilter: (sql: string) => void;
}
```

`GeoMapCanvas` is mocked in this test — the panel's job is detection, the column
override, and producing the right SQL, none of which need a real map.

- [ ] **Step 1: Write the failing test**

```tsx
vi.mock('./GeoMapCanvas', () => ({ default: () => <div data-testid="map" /> }));
vi.mock('../../api/mapConfig', () => ({
  getMapConfig: vi.fn(() => Promise.resolve({ tileUrl: '', attribution: '' })),
}));

import GeoFilterPanel from './GeoFilterPanel';

const GEO_COLUMNS = ['id', 'lat', 'lon'];
const GEO_TYPES = ['VARCHAR', 'DOUBLE', 'DOUBLE'];
const ROWS = [{ id: 'a', lat: 42.36, lon: -71.06 }];

it('renders the map when coordinates are detected', async () => {
  render(<GeoFilterPanel columns={GEO_COLUMNS} metadata={GEO_TYPES} rows={ROWS}
    sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  expect(await screen.findByTestId('map')).toBeInTheDocument();
});

/** An empty map with no explanation is the worst version of this. */
it('explains itself when no coordinate columns are found', () => {
  render(<GeoFilterPanel columns={['id', 'total']} metadata={['VARCHAR', 'DOUBLE']}
    rows={[]} sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  expect(screen.getByText(/latitude/i)).toBeInTheDocument();
  expect(screen.queryByTestId('map')).not.toBeInTheDocument();
});

it('lets the user pick columns when detection was wrong', async () => {
  render(<GeoFilterPanel columns={['id', 'a', 'b']} metadata={['VARCHAR', 'DOUBLE', 'DOUBLE']}
    rows={[]} sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  expect(screen.getByLabelText(/latitude column/i)).toBeInTheDocument();
  expect(screen.getByLabelText(/longitude column/i)).toBeInTheDocument();
});

it('applies a filter built from the drawn shape', async () => {
  const onApplyFilter = vi.fn();
  render(<GeoFilterPanel columns={GEO_COLUMNS} metadata={GEO_TYPES} rows={ROWS}
    sql="SELECT * FROM t" onApplyFilter={onApplyFilter} />);

  await screen.findByTestId('map');
  fireEvent.click(screen.getByRole('button', { name: /apply filter/i }));
  expect(onApplyFilter).not.toHaveBeenCalled(); // nothing drawn yet
});

/** Apply must stay disabled until there is a shape, or it produces invalid SQL. */
it('disables Apply until a shape exists', async () => {
  render(<GeoFilterPanel columns={GEO_COLUMNS} metadata={GEO_TYPES} rows={ROWS}
    sql="SELECT * FROM t" onApplyFilter={vi.fn()} />);
  await screen.findByTestId('map');
  expect(screen.getByRole('button', { name: /apply filter/i })).toBeDisabled();
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run src/components/geo/GeoFilterPanel.test.tsx`
Expected: FAIL — cannot resolve `./GeoFilterPanel`.

- [ ] **Step 3: Write the implementation**

Rows become points by reading the mapped columns and dropping any row where either value
is not a finite number — null coordinates are common in real data and would otherwise
plot at (0, 0), an actual place in the Atlantic that analysts will notice and mistrust.

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run src/components/geo/GeoFilterPanel.test.tsx`
Expected: PASS (5 tests)

- [ ] **Step 5: Wire the tab, then commit**

Add to the results panel `items` array in `SqlLabPage.tsx`, after `notebook`:

```tsx
{
  key: 'geo',
  label: <span><GlobalOutlined /> Map</span>,
  children: (
    <GeoFilterPanel
      columns={results?.columns ?? []}
      metadata={results?.metadata}
      rows={results?.rows ?? []}
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
- [ ] Manual: run a query with lat/lon columns, confirm points plot in the right hemisphere, draw a rectangle, apply, and check the returned rows fall inside the drawn box. Then draw a polygon over an area you know is empty and confirm zero rows rather than an error.

## Deferred

- **Radius / `ST_DWithin`**, pending the metres-versus-degrees decision. `ST_Transform` is the route.
- **Multiple shapes** via `ST_Union` / `ST_Difference`.
- **Saved shapes** reusable across queries.
- **Point sampling** — the map plots the returned rows, which the auto-limit caps at 1000 by default, so analysts are drawing over a sample of what the filter will actually apply to. See the open question in [`../GeoFilterBuilder.md`](../GeoFilterBuilder.md).
