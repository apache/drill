# SQL Lab page

**File:** `src/pages/SqlLabPage.tsx`
**Routes:** `/query` (cross-project), `/projects/:id/query` (project-scoped via `ProjectQueryPage` wrapper — see [`project-query.md`](project-query.md))

The most complex page in the app.

## Purpose

A full SQL IDE: Monaco editor with multi-tab support, schema browser, results grid with visualization builder, saved-query management, scheduling, AI features (chat via Prospector, transpile, optimize, explain), and an optional notebook panel for query narratives.

## Data sources

| API | Module | Used for |
|---|---|---|
| `POST /query.json` (`executeQuery`) | `api/queries.ts` | Run query |
| `GET /schema.json` (`getSchemaTree`, `useSchemas`) | `api/metadata.ts` | Schema browser tree |
| `GET /api/v1/savedQueries/:id` (`getSavedQuery`) | `api/savedQueries.ts` | Load saved query into a tab |
| `GET /api/v1/visualizations?queryId=…` (`getVisualizations`) | `api/visualizations.ts` | Attach saved visualizations to a query tab |
| `GET /api/v1/ai/status` (`getAiStatus`) | `api/ai.ts` | Gate AI features on Prospector being configured |
| `POST /api/v1/ai/chat` (`streamChat`, SSE) | `api/ai.ts` | Optimize / explain / suggest flows |
| `POST /api/v1/ai/transpile` (`transpileSql`) | `api/ai.ts` | Dialect transpilation |
| `POST /api/v1/ai/formatSql` (`formatSql`) | `api/ai.ts` | Format SQL button |
| Result cache (`resultCache.ts`) | `api/resultCache.ts` | Persist results across reloads |

## Child components

In `src/components/sqllab/`:

- `SqlEditor` — Monaco wrapper. Selection tracking, completion provider, validation, SQL-aware command palette.
- `ResultsGrid` — AG Grid table for results. Pagination, sort, column visibility, JSON-cell expander, CSV/clipboard export.
- `SchemaExplorer` — nested schema/table tree. Filter, drag-to-insert, peek-rows preview. Renders into the Left Rail.
- `QueryToolbar` — Run / Run Selection / Cancel buttons, schema/auto-limit dropdowns, format SQL.
- `VisualizationBuilder` — chart-config panel that drives ChartPreview. Lives next to ResultsGrid as the "Visualization" sub-tab.
- `NotebookPanel` — collapsible right-side markdown notebook for query narratives. Insertable Python code blocks from Prospector.
- `SaveQueryDialog` — save current tab as a new or existing saved query.
- `QueryHistoryModal` — past executions for the current user.
- `KeyboardShortcutsModal` — help overlay (triggered by `?`).
- `ProspectorPanel` — chat assistant rendered in the Right Inspector when the inspector is open.
- `ShareApiModal` — copyable cURL / API snippets for the active query.
- `VizTabIcon` — overlay icon shown on tabs that carry visualizations.

## Key state

### Redux (`querySlice`)

The tabs themselves live in Redux because they must survive route changes:

```ts
{
  tabs: Array<{
    id, name, sql, results, cacheId, isRunning, vizIds, isLocked
  }>,
  activeTabId: string,
}
```

### Redux (`uiSlice`)

- `editorHeight` — vertical split between editor and results, drag-resizable (150–600px).
- `selectedSchema`, `selectedTable` — schema browser focus.
- `resultsPanelTab` — `'results' | 'visualization' | 'history'`.

### Local (`useState`)

- `autoLimit` (localStorage `drill-sqllab-auto-limit`) — appends `LIMIT N` to bare SELECTs.
- `editorSettings`, `resultsSettings` (localStorage) — font size, line numbers, theme, pagination size.
- `editorInstanceRef`, `monacoInstanceRef` — Monaco handles for selection / validation.
- `isDragging` — splitter drag flag.
- `savedSqlRef` (per tab) — last-saved SQL, used to detect dirty state.

### Hooks

- `useTabPersistence` — writes tabs to localStorage (`drill-sqllab-tabs`) and restores on mount.
- `useWorkspacePersistence` — pushes results to the server-side result cache so tab content survives across machines / sessions if the cache is enabled.
- `useRestoreTabs` — rehydrates results from the cache on reload.
- `useQueryExecution` — wraps `executeQuery`, manages `isRunning` and abort.
- `useQueryHistory` — fetches and groups past executions.
- `useSchemas` — caches schema tree.
- `useProspector` — chat-assistant state, shared with the right-inspector panel.
- `useMonacoCompletion` — completion provider feeding off schema tree.
- `useSqlValidation` — inline error markers.

## Chrome

- Breadcrumb: `SQL Lab` (or project breadcrumb when wrapped by `ProjectQueryPage`).
- Toolbar actions: format, save, schedule, share, settings.
- Left rail: `SchemaExplorer`.
- Inspector tabs: `Prospector` (chat), `History`, `Validation`.

## Behavior

- **Multi-tab.** New tab is created untitled; rename, duplicate, pin, close from the tab context menu. Pinned tabs can't be closed.
- **Tab persistence.** Tabs persisted to localStorage; results to the server-side result cache (if enabled) so they survive reloads.
- **Run Selection.** When a selection exists, the Run button switches label and runs only the selected text.
- **Auto-limit.** When `autoLimit` is set, bare SELECT statements get `LIMIT N` appended. `extractSqlLimit()` strips comments before checking for an existing LIMIT.
- **Visualizations.** Building a chart adds it to the tab as a "Visualization" tab on the results panel and stores its id in `tab.vizIds`. Visualizations can be opened independently from the `/visualizations` page.
- **AI Optimize.** Selecting "Optimize" in the AI menu sends the current SQL through Prospector with the optimize prompt. The response streams into a modal; "Accept" runs the result through `transpileSql` and replaces the editor contents.
- **Notebook panel.** Collapsible markdown surface to the right of the editor. Renders saved markdown plus runnable Python cells.
- **Mobile / tablet.** Below the mobile breakpoint (`Grid.useBreakpoint()`), the schema explorer collapses and the layout switches to single-pane.

## Quirks

- **Heaviest chunk.** SQL Lab pulls in Monaco + AG Grid + ECharts (via the visualization builder) — the lazy import is essential. Adding new heavyweight deps here should go through `manualChunks` in `vite.config.ts`.
- **State boundary.** Tabs are in Redux; editor visual state (cursor, selection, scroll) lives on the Monaco instance via refs. Don't try to mirror Monaco state into Redux — it gets out of sync.
- **`isLocked`** prevents rename/close on saved-query tabs to make the saved-query→tab mapping unambiguous. Users save into a copy if they want to diverge.
- **Result cache** is best-effort. If the backend cache is disabled, tabs still work but results are lost on reload.
