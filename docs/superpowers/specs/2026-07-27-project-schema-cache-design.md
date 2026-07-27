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

A new `PersistentStore` **`drill.sqllab.schema_cache`**, keyed by **project id**.
The cache is **per-project — deliberately not shared across projects.** Two reasons:
access control (see below), and simpler removal (no cross-project reverse lookup).
The same schema added to five projects is scanned and stored five times; that
redundancy is accepted in exchange for the cache never crossing a project's
authorization boundary.

Value (`ProjectSchemaCache`, persisted via the LP Jackson mapper like other stores):

```
ProjectSchemaCache {
  String projectId;                        // the store key, echoed for convenience
  Map<String, CachedSchema> schemas;       // keyed by DatasetRef.schema path, e.g. "dfs.logs", "mysql"
}
CachedSchema {
  String  schemaPath;
  long    scannedAt;      // 0 = placeholder, not yet scanned
  boolean scanning;       // an async scan is in flight
  boolean truncated;      // size cap hit (see below)
  List<CachedTable> tables;
}
CachedTable  { String schema; String name; String type; List<CachedColumn> columns; }
CachedColumn { String name; String type; }
```

**Access control.** The cache is only ever served to a requester who passes the
same `canRead(project)` check the project's own endpoints enforce (owner / public /
`sharedWith`), reusing `ProjectResources.canRead` rather than a second copy. A
schema cached under project X is never served through project Y, and never through
a metadata call that carries no project context. This closes the leak a global
cache would open: under impersonation / views, one user's cached table & column
names could otherwise be served to a user whose own `INFORMATION_SCHEMA` query
would not return them.

A `plugin`-type dataset (e.g. `"mysql"`) is one entry, scanned with
`TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA LIKE 'mysql.%'`, so its sub-schemas land in
the same entry; each `CachedTable` keeps its own fully-qualified `schema`.

### The `SchemaCache` helper

One new class owns the store and all logic. It is injected into both
`MetadataResources` and `ProjectResources`. All read/mutate operations are scoped
to a `projectId`.

```
read(projectId, schemaPath) -> CachedSchema | null
    // Returns the project's cached entry if present and scanned. If the entry is
    // older than STALE_INTERVAL_MS, fires a background refresh
    // (serve-stale-then-refresh) and still returns the current copy. Returns null
    // if the schema is not tracked in that project. Callers gate on canRead first.

scan(projectId, schemaPath)         // synchronous scan -> writes the entry (async worker + refresh)
refreshAsync(projectId, schemaPath) // fire-and-forget scan on a background thread
track(projectId, schemaPath)        // write a placeholder entry (scannedAt=0) if absent, then refreshAsync
untrack(projectId, schemaPath)      // remove the schema from that project's cache map
isScannable(schemaPath) -> boolean  // false for http / googlesheets / EXCLUDED_PLUGINS
```

`scan` runs two cheap `INFORMATION_SCHEMA` queries via the same `RestQueryRunner`
machinery `MetadataResources` already uses, both filtered by
`TABLE_SCHEMA = ? OR TABLE_SCHEMA LIKE ?` (the schema path and its `.%` prefix):
- `INFORMATION_SCHEMA.\`TABLES\`` → `TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE` (the type,
  so cached VIEW / MATERIALIZED VIEW / TABLE render with the same icons as today).
- `INFORMATION_SCHEMA.\`COLUMNS\`` → `TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE`.

It joins the two by (schema, table), groups into `CachedTable`s, and writes the
entry. Two small queries per schema, run only on scan/refresh (not per read).

`isScannable` reuses the plugin-config-class-name check that
`MetadataResources.isHttpPlugin` already does, extended to also reject
`GoogleSheetsStoragePluginConfig`, plus the existing `EXCLUDED_PLUGINS` set
(`cp`, `sys`, `information_schema`).

**Size cap:** `scan` stops after `MAX_TABLES_PER_SCHEMA` tables (default 500) and
sets `truncated = true`. Keeps the cache and the Prospector prompt bounded for
large JDBC/Hive sources.

**Staleness:** `STALE_INTERVAL_MS` default 5 minutes. Access-triggered, not a
poller — a read only schedules a refresh if the entry is already older than this,
so typing/expanding does not rescan on every keystroke.

**Configurability.** Both are compile-time constants, not runtime config — the
lazy default. They can be promoted to Drill boot-config options
(`drill.exec.sqllab.schema_cache.max_tables` / `.stale_interval_ms`) later if ops
tuning without a rebuild is ever needed; nothing in the design depends on their
being configurable. `ponytail:` constants, promote to boot config if a real need
appears.

Change detection on refresh is plain snapshot equality: if the freshly scanned
`tables` equal what is stored, do not re-`put` (avoids churn / needless `updatedAt`
bumps).

### Read-through for tree + autocomplete

`MetadataResources.getTables(schema)` and `getColumns(schema, table)` gain an
optional `?projectId=` query param and a cache fast path:

1. If `projectId` is present **and** the requester passes `canRead(project)` **and**
   `schemaCache.read(projectId, schema)` returns a scanned entry → serve
   tables/columns from it (a background refresh is scheduled if the entry is stale).
2. Otherwise fall through to today's live `INFORMATION_SCHEMA` query, unchanged.

The cache is only consulted inside an authorized project context; without a
`projectId` (or without read access, or for an untracked schema, or for
http/googlesheets which are never tracked) the endpoints behave exactly as today.

Frontend wiring for the read path:
- `SchemaExplorer` already knows `projectId` — pass it to its `getTables`/`getColumns`
  calls when in project context.
- `useMonacoCompletion` (autocomplete) takes an optional `projectId` (threaded from
  `SqlLabPage` when a project is open) and forwards it to `getTables`/`getColumns`.

Outside a project (global schema browser, non-project SQL Lab), no `projectId` is
sent and nothing is cached or served — unchanged behavior.

### Trigger points

| Event | Where | Action |
|---|---|---|
| Add dataset | `ProjectResources.addDataset` (`POST /{id}/datasets`) | if `isScannable(schema)`, `schemaCache.track(projectId, schema)` (placeholder + async scan). Non-blocking; the add response returns immediately. |
| Remove dataset | `ProjectResources.removeDataset` (`DELETE /{id}/datasets/{dsId}`) | `schemaCache.untrack(projectId, schema)` — remove the schema from this project's cache map (no cross-project lookup, since the cache is per-project). |
| Delete project | `ProjectResources.deleteProject` / `purge` | drop the project's whole cache entry. |
| Read (tree / autocomplete, in project context) | `MetadataResources` | if `canRead(project)` and schema tracked, serve from cache; background refresh if stale. |
| Refresh | `POST /{id}/schema-cache/refresh` | re-scan every scannable schema referenced by the project's datasets. Owner-only, mirrors existing dataset-endpoint auth. |
| Inspect | `GET /{id}/schema-cache` | return the project's cached entries + `scannedAt` / `scanning` / `truncated` per schema, for the UI. `canRead`-gated. |

### Prospector integration

`ProspectorResources` already loads and **authorizes** the project via `loadProject`
(which enforces `canRead`). For each dataset schema of that authorized project it
calls `schemaCache.read(projectId, schema)` and injects the cached tables + columns
(capped, same pattern and budget discipline as the existing project block at
`ProspectorResources` ~L693–711). Because the read is scoped to a project the caller
already passed `canRead` on, no additional auth check is needed here.

The current instruction (~L890–898) that says *"ALWAYS use `list_schemas` /
`get_schema_info` to discover data BEFORE writing SQL"* softens to: *"Use the
schema listed below. Only call `get_schema_info` if a table or column you need is
not listed (e.g. the cache is truncated or a source could not be scanned)."* Cache
miss / empty → the old live-discovery instruction still applies, so nothing breaks.

### Frontend

- **Refresh button** on the Project Data Sources page (`ProjectDataSourcesPage`),
  calling `POST /{id}/schema-cache/refresh`, with a "last scanned" indicator from
  `GET /{id}/schema-cache`. New client functions in `api/projects.ts`.
- `SchemaExplorer` and `useMonacoCompletion` pass `projectId` (when in project
  context) to their `getTables`/`getColumns` calls so the cache read path engages —
  a small, additive change to the existing metadata API client signatures. Optional
  later: surface a "cached" affordance — out of scope for this spec.

## Data flow (happy path)

```
User adds "mysql" to project
  -> POST /{id}/datasets
  -> track("mysql"): placeholder entry written, async scan queued -> returns 200 immediately
  -> background: scan runs INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='mysql' OR LIKE 'mysql.%'
                 (mysql is scannable; not http/googlesheets) -> entry filled, scannedAt stamped

User opens SQL Lab in the project, types "SELECT * FROM mysql.store."
  -> autocomplete calls getColumns("mysql.store", ..., projectId=P)
  -> canRead(P) ok, read(P, "mysql.store") hits the "mysql" entry -> served from cache (instant)
  -> entry age < 5min -> no refresh

Later, user expands mysql in the tree (entry now stale)
  -> getTables("mysql", projectId=P) serves cached tables instantly
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
- **Per-project cache, not shared** — accepts scanning the same schema once per
  project so the cache never crosses an authorization boundary and removal needs no
  cross-project lookup. Chosen over a global cache specifically for access control.
- **Serve-stale-then-refresh** rather than blocking reads on freshness. Access
  keeps the cache current asynchronously; the user never waits.
- **Snapshot equality** for change detection, not hashing.
- **Async scan on add is best-effort** (like `loadProject`): if it fails, the next
  read or the refresh button fills it in.
- Single size cap and single staleness interval, both compile-time constants — no
  per-plugin policy, no config surface (promotable to boot config later).

## Testing

- `SchemaCache` unit tests: `isScannable` rejects http/googlesheets/excluded and
  accepts a normal schema; size cap sets `truncated`; snapshot equality suppresses
  a no-op re-`put`; stale read schedules a refresh, fresh read does not.
- `MetadataResources`: with `projectId` + `canRead`, a tracked schema is served from
  cache; without `projectId`, or a schema not tracked in that project, falls through
  to the live query; http/googlesheets never served from cache.
- **Access control:** a user who cannot `canRead` project P is never served P's
  cached metadata (via `getTables`/`getColumns?projectId=P`, `GET /P/schema-cache`,
  or Prospector), and a schema cached under P is not served through project Q.
- `ProjectResources`: add tracks a scannable schema and skips an http/googlesheets
  one; remove/delete untracks; refresh endpoint owner-only.

## Open items for the plan

- Background-thread mechanism for `refreshAsync` (reuse an existing executor on
  the Drillbit context vs. a small dedicated single-thread pool).
- Confirm the `INFORMATION_SCHEMA.\`TABLES\`` + `COLUMNS` join handles dynamic-schema
  sources (Splunk/Mongo `**` marker) the way `MetadataResources.fetchColumnsForTable`
  does today, or that such schemas simply cache what INFORMATION_SCHEMA returns.
