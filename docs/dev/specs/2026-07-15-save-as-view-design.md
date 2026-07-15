# Save as View / Materialized View — design

Status: approved, not yet implemented
Date: 2026-07-15

## Goal

In the SQL Lab query view, the Save button saves the editor's SQL as a *saved query* —
metadata in a `PersistentStore`, executed against nothing. This adds two more
destinations: save the SQL as a Drill **view**, or as a **materialized view**.

## Why this is mostly a frontend feature

Drill already has everything needed on the SQL side:

- `CREATE [OR REPLACE] VIEW` / `CREATE VIEW IF NOT EXISTS` — long-standing.
- `CREATE [OR REPLACE] MATERIALIZED VIEW`, `DROP MATERIALIZED VIEW`,
  `REFRESH MATERIALIZED VIEW` — DRILL-8543 (PR #3036, commit `8f06c306c7`), an
  ancestor of this branch. See [`docs/dev/MaterializedViews.md`](../MaterializedViews.md).
- `/query.json` (`QueryResources.java:49`) passes SQL straight to the parser with no
  statement-type whitelist. `ScheduleManager.java:455` already relies on this to run
  CTAS, INSERT, DROP and ALTER SESSION.

So there is no new execution machinery. The work is: one metadata flag, one small
store, and UI.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| UI shape | A `Save as` radio in the existing `SaveQueryDialog` | Save button unchanged; reuses the modal and form |
| Schema eligibility | New backend `supportsViews` flag on `SchemaInfo` | Authoritative; see below |
| Saved-query record for views | None — the `.view.drill` file is the artifact | One radio, one destination; avoids drift |
| Project linking | Auto-add to the active project when one is active | Matches the existing `addSavedQuery` behaviour |
| `DatasetRef.type` | Add `'view'` and `'materialized_view'` | Backend field is a free `String`; no Java change |
| Tree rendering | Distinct icons, flat list retained | Minimal churn in a 1252-line component |
| Name conflict | Unchecked `Replace if it already exists` → `OR REPLACE` | Clobbering is always deliberate; no error-string parsing |
| Descriptions | `drill.sqllab.view_descriptions` PersistentStore | Editable without re-running DDL |
| Non-SELECT SQL | Disable the View/MV radios | See "The SELECT guard" |
| Slashes in names | Allowed (subdirectory placement) | See "Nested views" |

## Backend

### 1. `MetadataResources.java` — `supportsViews` on `SchemaInfo`

```
supportsViews = (pluginConfig instanceof FileSystemConfig) && IS_MUTABLE = 'YES'
```

**Both halves are required.** Neither is sufficient:

- `IS_MUTABLE` is exactly `WorkspaceConfig.isWritable()`
  (`WorkspaceSchemaFactory.java:867-868`), so it is the right writability source — but
  `isMutable()` also returns true for Splunk, Kudu, JDBC and GoogleSheets, which accept
  CTAS and reject views.
- Only `WorkspaceSchema` overrides `createView` (`WorkspaceSchemaFactory.java:329`) and
  `createMaterializedView` (`:384`). `AbstractSchema` throws for both (`:160`, `:185`).
  So one rule covers views and materialized views alike.

Do **not** key on `PluginInfo.type == 'filesystem'`. That string is built by taking the
config class's simple name, stripping a `Config` suffix and lowercasing
(`MetadataResources.java:342-346`) — it is a display artifact, not the plugin's
registered type name (`FileSystemConfig.NAME` is `"file"`).

`getSchemas()` already runs an `INFORMATION_SCHEMA.SCHEMATA` query, so add `IS_MUTABLE`
to the existing `SELECT` rather than adding a round trip.

### 2. New `ViewDescriptionResources.java`

Store: `drill.sqllab.view_descriptions`, following the six existing `drill.sqllab.*`
stores (projects, saved_queries, visualizations, dashboards, schedules, ai_config).

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/view-descriptions?schema=<s>` | Batch per schema — the tree needs one call, not one per view |
| `PUT /api/v1/view-descriptions/{key}` | Create or edit |
| `DELETE /api/v1/view-descriptions/{key}` | Clear |

Value: `{ schema, name, description, updatedBy, updatedAt }`.

**Key encoding.** Schema and view names can contain arbitrary characters (backticks) and
slashes, and the store key becomes a znode/file name. `schema + "." + name` is ambiguous
because schema names contain dots. Use `base64url(schema + "|" + name)` — deterministic,
so reads need no scan, and path-safe.

**Authorization.** A view is a shared object with no owner in Drill's model, unlike a
saved query with its owner/`isPublic` fields. Rule: any authenticated user who can see
the schema may read and edit its descriptions. This is a deliberate choice, not an
oversight — revisit if view descriptions ever need to be restricted.

**Orphans.** Dropping a view outside the UI leaves its description behind. Accepted: a
view recreated under the same name almost certainly wants its description back.

## Frontend

| File | Change |
|---|---|
| `types/index.ts` | `SchemaInfo.supportsViews`; add `'MATERIALIZED VIEW'` to `TableInfo.type`; add `'view' \| 'materialized_view'` to `DatasetRef.type` |
| `api/viewDescriptions.ts` | New — get (batch per schema) / put / delete |
| `buildViewDdl.ts` | New — pure DDL builder + `isCreatableAsView` guard |
| `SaveQueryDialog.tsx` | Mode radio; schema/name/replace fields for the view modes |
| `TreeNodeBuilder.tsx` | Branch on `type` for icon + tooltip; render `.view.drill` files as view nodes |
| `SchemaExplorer.tsx` | Context menu: Edit Description; Refresh (MV only); refresh target schema after create |
| `ProjectDetailPage.tsx:203` | Real cases in the label chain |

`buildViewDdl` and `isCreatableAsView` are **pure functions in their own module**, not
inline in the modal. They hold the only real logic (quoting, `OR REPLACE`, statement
keyword, guard) and are the unit worth testing directly.

### Current state worth knowing

- **`TableInfo.type` is dead data.** `buildTableNodes` (`TreeNodeBuilder.tsx:246-344`)
  destructures only `table.name`; every table renders the same amber `TableOutlined`
  (`:339`). Nothing in the app branches on it. Views are already in the tree today,
  indistinguishable from tables.
- **`DatasetRef.type` falls through to `'Table'`** (`ProjectDetailPage.tsx:203-213`), so
  an unhandled `'view'` renders as "Table" silently rather than failing loudly. The new
  cases must be added, not relied upon to error.
- **The tree does not use react-query for tables.** `SchemaExplorer` keeps a hand-rolled
  `useState` cache (`:322`) and imports `getTables` directly, so invalidating
  `['tables', schema]` does nothing. The global refresh button collapses the whole tree;
  `handleRefreshNode` (`:847`) drops a single schema's cache entry and is the right hook
  after a create.

## Data flow — Save as View

1. Build the DDL, reusing `formatSchema` from `metadata.ts` for plugin/workspace quoting:

   ```sql
   CREATE [OR REPLACE] {VIEW | MATERIALIZED VIEW} dfs.`tmp`.`name` AS <sql>
   ```

2. `executeQuery` **without `autoLimitRowCount`**. `getFileColumns:193` documents why:
   auto-limit makes Drill cancel the query once N rows arrive, which would truncate an
   MV's materialization.
3. On success, in order:
   a. `PUT` the description, if non-empty.
   b. `addDataset(projectId, { type: 'view' | 'materialized_view', schema, table: name, label: name })`
      if a project is active.
   c. Refresh the target schema's tree node.
4. On failure, surface Drill's error and do nothing else.

### Partial failure

Steps 3a–3c can each fail *after* the view exists. Follow the existing precedent —
`useProspector.test.tsx:95` pins "reports a project-linking failure without losing the
created visualization". Same rule here: if the view was created but the description PUT
failed, report exactly that. Never report total failure for a view that exists.

## The SELECT guard

Views must be creatable only from `SELECT` (and CTEs). The check is on the **leading
statement keyword only**, and this is sufficient rather than merely heuristic:

- `SqlInsert` / `SqlDelete` / `SqlUpdate` / `SqlMerge` are siblings of
  `OrderedQueryOrExpr` in `SqlQueryOrDml` (generated `Parser.jj:2484-2498`) — reachable
  only as the leading production.
- `WITH` is followed strictly by `LeafQueryOrExpr` (`Parser.jj:4475`). Drill has no
  writable CTEs (no Postgres-style `WITH x AS (INSERT ... RETURNING)`).

So if the first keyword is `SELECT` or `WITH`, the statement cannot contain DML anywhere.
The guard cannot be bypassed.

**Do not scan the statement body** for `INSERT`/`UPDATE`/`DELETE`. It is unnecessary
given the above, and it produces false rejections of valid views:

```sql
SELECT 'INSERT' AS action FROM audit_log   -- string literal
SELECT update_time FROM logs               -- column name
SELECT * FROM deleted_users                -- table name
```

`isCreatableAsView(sql)`: strip `--` and `/* */` comments and leading whitespace/parens,
then require the first token to be `SELECT` or `WITH`, case-insensitive. Leading comments
and a parenthesised `(SELECT ... UNION SELECT ...)` must both pass.

When it returns false, **disable the View and Materialized View radios** with an
explanatory tooltip — not the Save button, since saving a non-SELECT as a plain saved
query remains valid.

### Name validation

The name is a **path, not an identifier** — `ViewHandler.java:64` calls
`removeLeadingSlash` on it, and `getViewPath` resolves it against the workspace location.
So slashes are allowed and an identifier regex would be wrong.

Validate: reject backticks (they would break out of the quoting we generate); reject `..`
path segments. Allow letters, digits, `_`, `-`, `.` and `/`.

Multi-statement injection is not a concern: `/query.json` is a single-statement parser,
so `SELECT 1; DROP TABLE x` fails to parse rather than executing.

## Nested views

This statement:

```sql
CREATE VIEW dfs.tmp.`subdir/myview` AS SELECT ...
```

writes `<location>/subdir/myview.view.drill`. It is queryable — `getViewPath` resolves
the path directly — but **invisible to `INFORMATION_SCHEMA`**:

- `getViews()` (`WorkspaceSchemaFactory.java:644-649`) globs `<location>/*.drill`,
  non-recursively (`DotDrillUtil.java:76`).
- `getTableNames()` = `rawTableNames() ∪ getViews() ∪ getMaterializedViews()` (`:677-678`).
- `RecordCollector.views()` (`:185-186`) is `getTablesByNames(schema.getTableNames())`
  filtered to `TableType.VIEW`; `materializedViews()` (`:198`) is identical.

So `TABLES`, `VIEWS` and `MATERIALIZED_VIEWS` are the same list filtered three ways.
`VIEWS` is **not** an independent scan and does not help here.

**Resolution:** the file browser (`getFiles`, `metadata.ts:178`) already lists these files
— it just shows the raw `myview.view.drill` filename, since `FileInfo` does no `.drill`
filtering. Detect the `.view.drill` / `.materialized_view.drill` suffix, strip it, and
render a proper view node whose click inserts `` dfs.`tmp`.`subdir/myview` ``.

Accepted trade-off: nested views remain absent from `INFORMATION_SCHEMA`, so sqlline,
JDBC clients and the Prospector's schema context will not see them. Making `getViews()`
recursive would fix this for all clients but would list an entire bucket on every
metadata query for a workspace like `dfs.root` — a separate PR needing benchmarks.

`VIEWS` is still worth reading for `VIEW_DEFINITION` (`InfoSchemaConstants.java:67`), the
natural source for showing a view's SQL in a tooltip or a future edit flow.

## Materialized view refresh

- Context-menu action on MV nodes, issuing ``REFRESH MATERIALIZED VIEW dfs.`tmp`.`name` ``.
- `INFORMATION_SCHEMA.MATERIALIZED_VIEWS` exposes `REFRESH_STATUS` and
  `LAST_REFRESH_TIME` (`InfoSchemaConstants.java:70-71`) — surface staleness in the tree
  tooltip rather than making users guess.
- Editing a description must **never** re-run the DDL.
  `CREATE OR REPLACE MATERIALIZED VIEW` re-executes the query and rewrites the whole
  Parquet dataset — this is precisely why descriptions live in a store rather than in
  the DDL.

## Testing

**Java**

- `supportsViews` is true for a writable file workspace; false for a non-writable one
  (`dfs.root`); **false for a mutable non-file plugin**. The third case is the entire
  point of the flag.
- `ViewDescriptionResources` CRUD, including key encoding for a schema containing dots
  and a view name containing slashes.

**Vitest**

- `buildViewDdl` across mode × replace flag × quoting, including a slashed name.
- `isCreatableAsView`: accepts `SELECT`, `WITH`, leading comments, leading parens;
  rejects `INSERT`/`UPDATE`/`DELETE`; **accepts** `SELECT 'INSERT' AS action FROM t` and
  `SELECT update_time FROM logs` (the false-positive cases).
- Partial-failure reporting when the view is created but a follow-up step fails.
- `TreeNodeBuilder` icon branch, including `.view.drill` files rendered as view nodes.

## Build order

Each step is independently shippable:

1. `supportsViews` + dialog mode radio + `buildViewDdl` + SELECT guard.
2. Tree icons + refresh-after-create.
3. Descriptions store + REST + edit UI.
4. MV refresh action + staleness display.
5. Nested-view rendering in the file browser.

## Accepted trade-offs

1. **Descriptions are web-UI-only.** They do not travel with the `.view.drill` file, so
   sqlline and JDBC clients never see them. Putting them in the file would need either a
   grammar change (making a typo fix re-materialize an MV) or REST-level workspace file
   writes that do not exist today.
2. **Nested views are absent from `INFORMATION_SCHEMA`.** Discoverable by browsing only.
3. **Description orphans** persist after a view is dropped outside the UI.
