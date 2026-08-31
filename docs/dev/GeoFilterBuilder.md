# Geospatial Filter Builder

Design notes for drawing geographic filters on a map and pushing them into the query as
SQL. The implementation plan is
[`plans/2026-08-31-geo-filter-builder.md`](plans/2026-08-31-geo-filter-builder.md).

Status: **implemented** (7 tasks, see
[`plans/2026-08-31-geo-filter-builder.md`](plans/2026-08-31-geo-filter-builder.md)).
Not yet exercised against a running drillbit: no drawn shape has produced a real query.

## What it is

A **Map** tab beside Results in SQL Lab. It is a tool for *authoring* a filter, not for
viewing data: the analyst draws geometry — a rectangle, a polygon, a path, a point — and
that geometry becomes a Drill spatial predicate wrapped around the current query, which
then re-runs.

The tab is enabled **only once a query has returned results**. Two reasons: the result
set is how the app knows which columns hold coordinates, and there is a query to wrap.
Before that the tab explains why it is unavailable rather than showing a dead map.

**Query results are not plotted.** The map is a drawing surface over a basemap. Plotting
the returned rows is a possible later addition, but it is not the point of the feature,
and it would show only the auto-limited sample rather than the data being filtered.

## The generated SQL

Drill ships 28 `ST_` functions in `contrib/udfs`, which covers everything here.

The filter wraps the user's query rather than editing it:

```sql
SELECT * FROM (<original query>) t
WHERE t.lon BETWEEN -71.19 AND -70.99
  AND t.lat BETWEEN  42.28 AND  42.40
  AND ST_Within(ST_Point(t.lon, t.lat),
                ST_GeomFromText('POLYGON((-71.19 42.28, ...))'))
```

Wrapping avoids parsing SQL, composes with any existing `WHERE`, `GROUP BY` or `LIMIT`,
and works for queries the app did not generate.

### Geometry types and the predicate each produces

| Drawn | WKT | Predicate |
|---|---|---|
| Rectangle | `POLYGON` | `ST_Within` |
| Polygon | `POLYGON` | `ST_Within` |
| Path | `LINESTRING` | `ST_DWithin` with a distance |
| Point | `POINT` | `ST_DWithin` with a distance |

A path or a point means nothing on its own — "near this route" is only a filter once a
distance is attached. So distance is a first-class part of the feature, not an optional
extra, and that forces the units problem below to be solved rather than avoided.

### Distance must go through a projected coordinate system

`ST_DWithin`'s third argument is expressed in **the units of the geometry's coordinate
system**. For raw lon/lat that is degrees. A control reading "500 m" that passes `500`
would filter to within 500 *degrees* — the entire planet — and return everything, which
looks like a working feature returning a lot of data rather than like a bug.

`ST_Transform(geom, srcSrid, tgtSrid)` (proj4j, EPSG codes) is the way out. Both
geometries are transformed into a metric CRS before the distance test:

```sql
ST_DWithin(
  ST_Transform(ST_Point(t.lon, t.lat), 4326, 32619),
  ST_Transform(ST_GeomFromText('LINESTRING(...)'), 4326, 32619),
  500)                                    -- 500 real metres
```

The target is the **UTM zone of the drawn geometry's centroid**, computed client-side:
`zone = floor((lon + 180) / 6) + 1`, then `32600 + zone` in the northern hemisphere or
`32700 + zone` in the southern.

Web Mercator (3857) would be easier and wrong: its metres are stretched by
`1 / cos(latitude)`, so a "500 m" radius in Boston would really be about 675 m, and in
Reykjavík about 1 km. Wrong by a factor that grows with latitude is the worst kind of
wrong, because it looks plausible in testing near the equator.

UTM is itself only valid near its zone, so a shape spanning many zones is out of scope;
the UI warns rather than silently producing a bad answer.

### Two traps in the function signatures

**`ST_Point` takes longitude first.** `STPointFunc.java` declares `lonParam` then
`latParam`, the reverse of how people say "lat/long". Swapping them puts Boston in
Antarctica and returns zero rows rather than an error.

**`ST_Transform` needs both source and target SRID.** Source is always 4326 here, since
the drawn geometry and the raw lon/lat columns are both unprojected degrees.

### The bounding-box prefilter is not an optimisation

Drill has no spatial index, so every row pays `ST_Point` plus the predicate — and these
queries run over whole tables, not over a returned sample. The `BETWEEN` clauses on the
raw numeric columns are cheap, may push down to the source (JDBC predicate pushdown,
Parquet row-group pruning), and cut the rows reaching the expensive test.

The bbox comes from the shape's envelope, expanded by the distance for a path or point,
so it never changes the result — only the work.

## Detection

Coordinate columns are resolved from the result set, which exists by the time the tab is
usable:

1. **Latitude/longitude pair** — matched by name (`lat`, `latitude`, `y`; `lon`, `lng`,
   `long`, `longitude`, `x`) on numeric columns.
2. **Geometry column** — a VARCHAR holding WKT, filtered with
   `ST_Within(ST_GeomFromText(t.geom), ...)`.

Detection is a guess, so the mapping is always editable, following the pattern
`ColumnMapper.tsx` already uses to assign columns to chart roles. When nothing matches,
the tab says what it looked for.

A geometry column cannot have a bounding-box prefilter (there are no raw numeric columns
to compare), and cannot use the UTM distance path without an extra transform, so
distance filters are offered only for lat/lon mappings in the first version.

## Basemap

The app already bundles GeoJSON/TopoJSON basemaps in `public/geojson/` (including
`world.json`) and serves them through `fetchGeoJson` for the existing ECharts geo charts.
The Map tab renders the same data as an OpenLayers vector layer, so the default basemap
works with nothing leaving the network.

An admin may set a raster tile URL for street-level detail, stored in
`drill.sqllab.map_config` following the `ProfileConfigResources` pattern:

| Field | Meaning |
|---|---|
| `tileUrl` | XYZ template, e.g. `https://{a-c}.tile.openstreetmap.org/{z}/{x}/{y}.png`. Empty means bundled vectors only. |
| `attribution` | Required credit line, shown in the map's attribution control. |

`attribution` exists because most tile providers require credit as a condition of use —
OpenStreetMap's policy does. Shipping the URL field without it would put an admin in
breach without their noticing.

Drawing a path over a blank vector basemap is workable but harder than over streets, so
deployments that can reach a tile server will get noticeably more out of this feature.

## Why OpenLayers

ECharts is already present and its `brush` component can select over a `geo` coordinate
system, which would avoid a new dependency. It was rejected because brush is a one-shot
selection gesture: it cannot draw a path, cannot attach a distance, and cannot modify a
shape after drawing — and building a filter is iterative.

OpenLayers is BSD-2 (ASF Category A, permitted), and brings two things that would
otherwise be hand-rolled and silently wrong: `ol/format/WKT` serialises drawn geometry
straight into what `ST_GeomFromText` expects, and `ol/proj` handles reprojection between
the map's Web Mercator view and the EPSG:4326 the data is in.

The cost is a second mapping stack alongside ECharts geo. Accepted deliberately: the
chart maps render data, this one edits geometry.

## Out of scope for the first version

- **Plotting query results** on the map.
- **Multiple shapes** in one filter — union of polygons, or a shape with a hole.
  `ST_Union` and `ST_Difference` exist, so this is additive later.
- **Saving a shape** for reuse across queries.
- **Distance on geometry columns**, as noted above.
- **Shapes spanning several UTM zones**, which the UI warns about rather than
  approximating.
