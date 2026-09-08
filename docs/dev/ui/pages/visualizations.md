# Visualizations

Covers the visualization grid and the per-visualization editor.

## VisualizationsPage

**File:** `src/pages/VisualizationsPage.tsx`
**Routes:** `/visualizations`, `/projects/:id/visualizations` (project-scoped via `ProjectVisualizationsPage`)

### Purpose

Browse all visualizations with chart-type icons and a preview pane. Filter by type, owner, visibility. Bulk add to projects, bulk delete.

### Data sources

| API | Module |
|---|---|
| `GET /api/v1/visualizations` (`getVisualizations`) | `api/visualizations.ts` |
| `POST /query.json` (`executeQuery`) — for live preview | `api/queries.ts` |
| `GET /api/v1/projects` (`getProjects`) — for bulk add | `api/projects.ts` |

### Child components

- `VisualizationTile` — `ChartPreview` thumbnail + name + actions.
- `ChartPreview` — renders an ECharts chart from a config.
- `NewVisualizationTile` — create button (opens the SQL-first new viz flow).
- `AddToProjectModal`, `BulkAddToProjectModal`.

### Key state

- `searchText`
- `filter` — `'type' | 'owner' | 'public' | 'private'`
- `bulkSelected: Set<string>`
- `selectedPreviewId` — which viz to render in the larger right-side preview
- `useUndoableDelete`

### Behavior

- Chart-type icons (`AreaChartOutlined`, `LineChartOutlined`, etc.) come from a `chartIcons` map keyed by viz type.
- Clicking a tile navigates to [`VisualizationDetailPage`](#visualizationdetailpage).
- Bulk select via row-level checkboxes.

### Quirks

- Accepts optional props `filterIds?`, `projectId?`, `projectOwner?`. Project wrappers set them; cross-project leaves them undefined.

---

## VisualizationDetailPage

**File:** `src/pages/VisualizationDetailPage.tsx`
**Routes:** `/visualizations/:vizId`, `/projects/:id/visualizations/:vizId`

### Purpose

Full editor for a single visualization: inline SQL editing, chart-config UI, live preview, save / publish, add to dashboard.

### Data sources

| API | Module |
|---|---|
| `GET /api/v1/visualizations/:vizId` (`getVisualization`) | `api/visualizations.ts` |
| `PUT /api/v1/visualizations/:vizId` (`updateVisualization`) | `api/visualizations.ts` |
| `POST /query.json` (`executeQuery`) | `api/queries.ts` |
| `GET /api/v1/dashboards` (`getDashboards`) | `api/dashboards.ts` |
| `POST /api/v1/dashboards` (`createDashboard`) | `api/dashboards.ts` |
| `PUT /api/v1/dashboards/:id` (`updateDashboard`) | `api/dashboards.ts` |
| `POST /api/v1/savedQueries` (`createSavedQuery`), `PUT` (`updateSavedQuery`) | `api/savedQueries.ts` |
| `POST /api/v1/ai/formatSql` (`formatSql`) | `api/ai.ts` |

### Child components

- `SqlEditor` — Monaco editor (shared with SQL Lab).
- `ChartPreview` — live chart preview.
- `VisualizationEditor` — config UI (axes, series, palette, type-specific options).
- Dashboard picker / "Add to dashboard" modal.
- Alerts for query errors and column-dependency warnings.

### Key state

- `isEditing` — toggle between view and edit modes
- `editedSql` — staged SQL during edit
- `queryResult`, `queryError`, `queryLoading`
- `ranQueryRef` — last-executed SQL, so the chart shows stale-but-valid results until the user re-runs
- Column dependency: `findMissingColumnRefs`, `groupMissingByColumn`

### Behavior

- **Edit mode.** Entering it seeds `editedSql` from the saved viz. Chart shows the last successful query until the user re-runs.
- **Save.** Updates the viz; if it's used by any dashboards, those dashboards are also touched so their snapshots invalidate.
- **Add to dashboard.** Modal lets the user pick an existing dashboard or create a new one inline.
- **Column validation.** If the viz config references a column that's missing from the current result set, an inline warning lists the missing columns.
- **Copy.** Duplicates the viz with a new name; lands in `/visualizations/:newId` edit mode.
- **Delete.** Soft-delete; recoverable via the trash drawer.

### Chrome

Breadcrumb: `Library > Visualizations > Name` (or project breadcrumb). Toolbar: Edit / Save, Format SQL, Copy, Delete, Add to Dashboard.

### Quirks

- `projectId` prop drives breadcrumb and back-button destination — the same component handles both the cross-project and project-scoped routes.
- Format SQL hits the same `/api/v1/ai/formatSql` endpoint as SQL Lab; it's an LLM-backed format, not a deterministic one.

---

## ProjectVisualizationDetailPage

**File:** `src/pages/ProjectVisualizationDetailPage.tsx`

Thin wrapper: pulls `projectId` from `useProjectContext()` and renders `VisualizationDetailPage` with it.
