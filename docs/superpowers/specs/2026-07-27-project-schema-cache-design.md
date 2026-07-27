# Project Schema Cache — Design

**Date:** 2026-07-27
**Status:** Approved, ready for implementation plan

## Problem

When Prospector (the AI assistant) is asked to generate SQL inside a project, it
must first run `INFORMATION_SCHEMA` queries at chat time to discover the schemas,
tables, and columns available in that project. This adds latency and tool-round
churn to every conversation. The schema tree and SQL autocomplete pay the same
live-query cost.

We want a server-side cache of the schemas that belong to a project, populated
when data sources are added and kept fresh seamlessly, so Prospector, the schema
tree, and autocomplete all read fast, pre-scanned metadata instead of querying
`INFORMATION_SCHEMA` on demand.

## Goals

- Cache **only the schemas explicitly added to a project** (its `datasets`).
- Cache **tables + columns (with types)**, not just schema names — that is what
  removes Prospector's live discovery calls.
- Serve the schema tree and SQL autocomplete from the cache too (read-through).
- Keep the cache fresh without user friction: on add, on access (background), and
  via an explicit refresh button.
- **Never scan HTTP or GoogleSheets plugins** (see Non-goals / constraints).

## Non-goals / hard constraints

- **HTTP and GoogleSheets plugins must not be scanned.** They have too many edge
  cases: HTTP "tables" are URL-tail arguments materialized only at query time and
  do not enumerate (`HttpApiConnectionSchema.getTableNames()` returns only tables
  already queried); GoogleSheets enumeration forces a network round-trip to the
  Google Drive API just to build the schema. Any schema whose plugin config class
  is `HttpStoragePluginConfig` or `GoogleSheetsStoragePluginConfig` is skipped —
  it falls through to today's live-query behavior, never cached.
- No background poller / TTL sweeper. Freshness is driven by add, by access, and
  by the refresh button only.
- No new Maven or npm dependencies.

## Architecture

### Storage

A new `PersistentStore` **`drill.sqllab.schema_cache`**, keyed by **schema path**
(the value of `DatasetRef.schema`, e.g. `"dfs.logs"`, `"mysql"`). The cache is
**global / shared across projects**: a schema added to two projects is scanned and
stored once, and every project referencing it reads the same entry.

Value (`CachedSchema`, persisted via the LP Jackson mapper like other stores):

```
CachedSchema {
  String  schemaPath;     // the store key, echoed for convenience
  long    scannedAt;      // 0 = placeholder, not yet scanned
  boolean scanning;       // an async scan is in flight
  boolean truncated;      // size cap hit (see below)
  List<CachedTable> tables;
}
CachedTable  { String schema; String name; String type; List<CachedColumn> columns; }
CachedColumn { String name; String type; }
```

A `plugin`-type dataset (e.g. `"mysql"`) is one entry, scanned with
`TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA LIKE 'mysql.%'`, so its sub-schemas land in
the same entry; each `CachedTable` keeps its own fully-qualified `schema`.

### The `SchemaCache` helper

One new class owns the store and all logic. It is injected into both
`MetadataResources` and `ProjectResources`.

```
read(schemaPath) -> CachedSchema | null
    // Returns the cached entry if present and scanned. If the entry is older than
    // STALE_INTERVAL_MS, fires a background refresh (serve-stale-then-refresh) and
    // still returns the current copy. Returns null for untracked schemas.

scan(schemaPath)            // synchronous scan -> writes the entry (used by async worker + refresh)
refreshAsync(schemaPath)    // fire-and-forget scan on a background thread
track(schemaPath)           // write a placeholder entry (scannedAt=0) if absent, then refreshAsync
untrackIfUnreferenced(schemaPath, projectStore)  // delete entry if no project references it
isScannable(schemaPath) -> boolean   // false for http / googlesheets / EXCLUDED_PLUGINS
```

`scan` runs a single `INFORMATION_SCHEMA.\`COLUMNS\`` query
(`SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.\`COLUMNS\` WHERE TABLE_SCHEMA = ? OR TABLE_SCHEMA LIKE ?`)
via the same `RestQueryRunner` machinery `MetadataResources` already uses, groups
rows into tables, and writes the entry. Table `type` comes from a companion
`INFORMATION_SCHEMA.\`TABLES\`` lookup (or is defaulted to `TABLE` if we choose to
skip the second query — decide in planning; columns are the essential part).

`isScannable` reuses the plugin-config-class-name check that
`MetadataResources.isHttpPlugin` already does, extended to also reject
`GoogleSheetsStoragePluginConfig`, plus the existing `EXCLUDED_PLUGINS` set
(`cp`, `sys`, `information_schema`).

**Size cap:** `scan` stops after `MAX_TABLES_PER_SCHEMA` tables (default ~500) and
sets `truncated = true`. Keeps the cache and the Prospector prompt bounded for
large JDBC/Hive sources. `ponytail:` tunable constant.

**Staleness:** `STALE_INTERVAL_MS` default 5 minutes. Access-triggered, not a
poller — a read only schedules a refresh if the entry is already older than this,
so typing/expanding does not rescan on every keystroke. `ponytail:` tunable.

Change detection on refresh is plain snapshot equality: if the freshly scanned
`tables` equal what is stored, do not re-`put` (avoids churn / needless `updatedAt`
bumps).

### Read-through for tree + autocomplete

`MetadataResources.getTables(schema)` and `getColumns(schema, table)` gain a cache
fast path:

1. If `schemaCache.read(schema)` returns a scanned entry, serve tables/columns from
   it (and it will have scheduled a background refresh if stale).
2. Otherwise fall through to today's live `INFORMATION_SCHEMA` query, unchanged.

No `projectId` plumbing is needed on these endpoints: **the existence of a tracked
entry is the gate.** Untracked schemas (anything never added to a project, plus all
http/googlesheets schemas which are never tracked) behave exactly as today.

`SchemaExplorer` (schema tree) and `useMonacoCompletion` (autocomplete) call these
same endpoints, so both transparently benefit — no frontend changes required for
the read path.

### Trigger points

| Event | Where | Action |
|---|---|---|
| Add dataset | `ProjectResources.addDataset` (`POST /{id}/datasets`) | if `isScannable(schema)`, `schemaCache.track(schema)` (placeholder + async scan). Non-blocking; the add response returns immediately. |
| Remove dataset | `ProjectResources.removeDataset` (`DELETE /{id}/datasets/{dsId}`) | `schemaCache.untrackIfUnreferenced(schema, projectStore)` — deletes the entry only if no other non-deleted project still references that schema path. |
| Read (tree / autocomplete / any `getTables`/`getColumns`) | `MetadataResources` | serve from cache if tracked; background refresh if stale. |
| Refresh | `POST /{id}/schema-cache/refresh` | re-scan every scannable schema referenced by the project's datasets. Owner-only, mirrors existing dataset-endpoint auth. |
| Inspect | `GET /{id}/schema-cache` | return the cached entries + `scannedAt` / `scanning` / `truncated` per schema, for the UI. |

### Prospector integration

`ProspectorResources`, when building the system prompt for a chat with
`ctx.projectId` (or `ctx.projectDatasets`), calls `schemaCache.read(schema)` for
each dataset schema and injects the cached tables + columns (capped, same pattern
and budget discipline as the existing project block at
`ProspectorResources` ~L693–711).

The current instruction (~L890–898) that says *"ALWAYS use `list_schemas` /
`get_schema_info` to discover data BEFORE writing SQL"* softens to: *"Use the
schema listed below. Only call `get_schema_info` if a table or column you need is
not listed (e.g. the cache is truncated or a source could not be scanned)."* Cache
miss / empty → the old live-discovery instruction still applies, so nothing breaks.

### Frontend

- **Refresh button** on the Project Data Sources page (`ProjectDataSourcesPage`),
  calling `POST /{id}/schema-cache/refresh`, with a "last scanned" indicator from
  `GET /{id}/schema-cache`. New client functions in `api/projects.ts`.
- No changes needed to `SchemaExplorer` or `useMonacoCompletion` for the read path
  (they already call the cached endpoints). Optional later: surface a "cached"
  affordance — out of scope for this spec.

## Data flow (happy path)

```
User adds "mysql" to project
  -> POST /{id}/datasets
  -> track("mysql"): placeholder entry written, async scan queued -> returns 200 immediately
  -> background: scan runs INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='mysql' OR LIKE 'mysql.%'
                 (mysql is scannable; not http/googlesheets) -> entry filled, scannedAt stamped

User opens SQL Lab in the project, types "SELECT * FROM mysql.store."
  -> autocomplete calls getColumns("mysql.store", ...)
  -> read("mysql.store") hits the "mysql" entry -> columns served from cache (instant)
  -> entry age < 5min -> no refresh

Later, user expands mysql in the tree (entry now stale)
  -> getTables("mysql") serves cached tables instantly
  -> read() sees age > 5min -> schedules background refresh -> entry updated if changed

User asks Prospector "top 10 customers by revenue"
  -> system prompt already contains mysql.store's tables+columns from cache
  -> model writes SQL without any get_schema_info round-trip
```

## Deliberate simplifications (`ponytail:`)

- **Read-through cache instead of a bespoke browse-merge path.** One mechanism
  (`read`/`scan`) feeds tree, autocomplete, and Prospector; drops the separate
  "merge a browsed sub-schema into a plugin entry" logic and a Prospector-specific
  injection query.
- **Serve-stale-then-refresh** rather than blocking reads on freshness. Access
  keeps the cache current asynchronously; the user never waits.
- **Snapshot equality** for change detection, not hashing.
- **Async scan on add is best-effort** (like `loadProject`): if it fails, the next
  read or the refresh button fills it in.
- Single size cap and single staleness interval, both tunable constants — no
  per-plugin policy, no config surface.

## Testing

- `SchemaCache` unit tests: `isScannable` rejects http/googlesheets/excluded and
  accepts a normal schema; size cap sets `truncated`; snapshot equality suppresses
  a no-op re-`put`; stale read schedules a refresh, fresh read does not.
- `MetadataResources`: tracked schema served from cache; untracked schema falls
  through to live query; http/googlesheets never served from cache.
- `ProjectResources`: add tracks a scannable schema and skips an http/googlesheets
  one; remove untracks only when unreferenced; refresh endpoint owner-only.

## Open items for the plan

- Whether to run the second `INFORMATION_SCHEMA.TABLES` query for accurate table
  `type`, or default to `TABLE` and skip it (columns are the essential payload).
- Background-thread mechanism for `refreshAsync` (reuse an existing executor on
  the Drillbit context vs. a small dedicated single-thread pool).
- Exact cap/interval defaults (`MAX_TABLES_PER_SCHEMA`, `STALE_INTERVAL_MS`).
