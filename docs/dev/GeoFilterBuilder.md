# Geospatial Filter Builder

Design notes for drawing geographic filters on a map and pushing them into the query as
SQL. The implementation plan is
[`plans/2026-08-31-geo-filter-builder.md`](plans/2026-08-31-geo-filter-builder.md).

Status: **design agreed, not yet implemented**.

## What it is

A **Map** tab beside Results in SQL Lab. When a query returns something that can be
located — latitude/longitude columns, or a geometry column — the tab plots the rows and
lets an analyst draw a rectangle or polygon over them. Applying the shape rewrites the
query with a spatial predicate and re-runs it, so the filter is evaluated by Drill over
the whole dataset rather than over the rows that happen to have come back.

## The generated SQL

Drill ships 28 `ST_` functions in `contrib/udfs`, which is everything this needs.

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

### Two traps the code must handle

**`ST_Point` takes longitude first.** `STPointFunc.java` declares `lonParam` then
`latParam`, the reverse of how people say "lat/long". Swapping them puts Boston in
Antarctica and returns zero rows rather than an error.

**Distances are in the units of the coordinate system.** `ST_DWithin`'s third argument
is degrees when the geometry is raw lon/lat, not metres. A radius control that reads
"5 km" and passes `5` would be silently, badly wrong — which is why radius is deferred
until it goes through `ST_Transform`. Rectangle and polygon have no such problem.

### The bounding-box prefilter is not an optimisation

Drill has no spatial index, so every row pays `ST_Point` plus the predicate. The
`BETWEEN` clauses on the raw numeric columns are cheap, may push down to the source
(JDBC predicate pushdown, Parquet row-group pruning), and cut the rows reaching the
expensive test. The bbox is computed from the drawn shape's envelope, so it never
changes the result — only the work. It is emitted for every shape, including polygons.

## Detection

A query is geo-capable when its result columns can be resolved to coordinates:

1. **Latitude/longitude pair** — matched by name (`lat`, `latitude`, `y`; `lon`, `lng`,
   `long`, `longitude`, `x`) on numeric columns.
2. **Geometry column** — a VARCHAR holding WKT, filtered with
   `ST_Within(ST_GeomFromText(t.geom), ...)`.

Detection is a guess, so the mapping is always editable, following the pattern
`ColumnMapper.tsx` already uses to assign columns to chart roles. When nothing matches,
the Map tab explains what it looked for rather than rendering an empty map.

## Basemap

The app already bundles GeoJSON/TopoJSON basemaps in `public/geojson/` (including
`world.json`) and serves them through `fetchGeoJson`, for the existing ECharts geo
charts. The Map tab renders the same data as an OpenLayers vector layer, so the default
basemap works with nothing leaving the network.

An admin may set a raster tile URL to get street-level detail, stored in
`drill.sqllab.map_config` following the `ProfileConfigResources` pattern:

| Field | Meaning |
|---|---|
| `tileUrl` | XYZ template, e.g. `https://{a-c}.tile.openstreetmap.org/{z}/{x}/{y}.png`. Empty means bundled vectors only. |
| `attribution` | Required credit line, shown in the map's attribution control. |

`attribution` exists because most tile providers require credit as a condition of use —
OpenStreetMap's policy does. Shipping the URL field without it would put an admin in
breach without their noticing.

## Why OpenLayers

ECharts is already present and its `brush` component can select over a `geo` coordinate
system, which would avoid a new dependency. It was rejected because brush is a one-shot
selection gesture: it cannot modify a shape after drawing, and building a filter is an
iterative act — nudge a vertex, widen the box, re-run.

OpenLayers is BSD-2 (ASF Category A, permitted), and brings two things that would
otherwise be hand-rolled and silently wrong: `ol/format/WKT` serialises a drawn geometry
straight into what `ST_GeomFromText` expects, and `ol/proj` handles the reprojection
from the map's Web Mercator view to the EPSG:4326 lon/lat the data is in.

The cost is a second mapping stack alongside ECharts geo. Accepted deliberately: the
chart maps render data, this one edits geometry, and they have little in common beyond
the word "map".

## Out of scope for the first version

- **Radius / circle**, pending the units decision above.
- **Multi-shape filters** (union of several polygons, or subtract-a-hole). `ST_Union`
  and `ST_Difference` exist, so this is additive later.
- **Saving a shape** for reuse across queries.
- **Clustering or heatmaps** for very large point sets. The Map tab plots the returned
  rows only, which the auto-limit already caps; the *filter* applies to everything.

## Open questions

- **How many rows will analysts plot?** OpenLayers handles tens of thousands of vector
  features, but the Map tab is bounded by the query's auto-limit (default 1000), so
  points on screen are a sample of the data being filtered. Whether that sample needs
  to be representative — or drawn from a `TABLESAMPLE` rather than the first N rows —
  is unresolved and affects whether analysts trust what they are drawing over.
