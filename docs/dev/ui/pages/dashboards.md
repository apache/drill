# Dashboards

Covers the dashboard grid and the dashboard view / edit canvas.

## DashboardsPage

**File:** `src/pages/DashboardsPage.tsx`
**Routes:** `/dashboards`, `/projects/:id/dashboards` (project-scoped via `ProjectDashboardsPage`)

### Purpose

Browse dashboards. Filter by favorites or visibility. Create, edit metadata, delete (soft).

### Data sources

| API | Module |
|---|---|
| `GET /api/v1/dashboards` (`getDashboards`) | `api/dashboards.ts` |
| `POST /api/v1/dashboards` (`createDashboard`) | `api/dashboards.ts` |
| `PUT /api/v1/dashboards/:id` (`updateDashboard`) | `api/dashboards.ts` |
| `DELETE /api/v1/dashboards/:id` (`deleteDashboard`) | `api/dashboards.ts` |
| `POST /api/v1/dashboards/:id/restore` (`restoreDashboard`) | `api/dashboards.ts` |
| `GET /api/v1/dashboards/favorites` (`getFavorites`) | `api/dashboards.ts` |
| `POST /api/v1/dashboards/favorites/:id` (`toggleFavorite`) | `api/dashboards.ts` |

### Child components

- `DashboardTile` — thumbnail, panel count, last-updated, visibility, favorite star, actions menu.
- `NewDashboardTile` — create button.
- `AddToProjectModal`, `BulkAddToProjectModal`.

### Key state

- `searchText`
- `filter` — `'all' | 'favorites' | 'public'`
- `createModalOpen`, `createForm`
- `editModalOpen`, `editingDashboard`
- `bulkSelected: Set<string>`
- `favoriteSet` (memoized for O(1))
- `useUndoableDelete`

### Behavior

- Favorites are pinned to the top.
- Tiles show panel counts grouped by type (visualization, markdown, image, etc.).
- Delete is soft; restore via the toast.

### Quirks

- Same `filterIds` / `projectId` / `projectName` / `projectOwner` prop pattern as Saved Queries and Visualizations.

---

## DashboardViewPage

**File:** `src/pages/DashboardViewPage.tsx`
**Routes:** `/dashboards/:id`, `/projects/:id/dashboards/:dashboardId`

### Purpose

The dashboard canvas. Grid layout with live panels (visualizations, markdown, images, AI Q&A, alerts, NL filters). View mode and edit mode share the same component; edit mode adds drag-resize handles, an "Add panel" affordance, theme picker, and refresh-rate settings.

### Data sources

| API | Module |
|---|---|
| `GET /api/v1/dashboards/:id` (`getDashboard`) | `api/dashboards.ts` |
| `PUT /api/v1/dashboards/:id` (`updateDashboard`) | `api/dashboards.ts` |
| Visualization panels execute their own queries via `executeQuery` | `api/queries.ts` |
| `POST /api/v1/ai/chat` (`streamChat`) — for AI Q&A and NL filters | `api/ai.ts` |
| `POST /api/v1/dashboards/upload-image` (`uploadImage`) | `api/dashboards.ts` |

### Child components

- `ResponsiveGridLayout` (react-grid-layout) — the grid.
- `DashboardPanelCard` — renders one panel of any type.
- `DashboardFilterBar` — natural-language filter input.
- `DashboardSettingsDrawer` — theme, refresh rate, export to PDF.

### Key state

- `isEditMode` — toggled via Edit button
- `dashboard` — the full definition (panels, tabs, theme, filters, refresh interval)
- `panels` — derived data from per-panel queries
- `filterValues` — read from URL `searchParams` so dashboard links carry filter state
- `selectedPanelId` — for edit-mode panel toolbar
- `refreshIntervalMs` — 0 (off), 10s, 30s, 1m, 5m

### Behavior

- **Drag-resize.** Panels are draggable by their header and resizable by corner handles in edit mode.
- **Add panel.** Modal with tabs: visualization, markdown, image, title, executiveSummary, aiQnA, aiAlerts, nlFilter.
- **NL filter.** The text input is translated to a Drill filter expression via `streamChat`. The expression is stored on the dashboard's filter set and applied to panel queries.
- **AI Q&A.** A panel type that runs a streaming chat against the dashboard context.
- **Refresh.** Manual refresh icon or an auto-interval; both re-execute all panel queries.
- **Export PDF.** `html2canvas` snapshots the dashboard div and `jsPDF` assembles the PDF. Multi-tab dashboards export tab-by-tab.
- **Favorites.** Toggled with `StarFilled` / `StarOutlined`.
- **Project context.** When mounted under `/projects/:id/dashboards/:dashboardId`, the breadcrumb shows the parent project.

### Chrome

Breadcrumb varies by context. Toolbar: Edit / Save, Refresh, Settings, Favorite, Export PDF.

### Quirks

- The dashboard definition has its own theme tokens (`--db-*` CSS custom properties) so individual dashboards can override the global theme.
- `react-grid-layout` requires its CSS to be imported (handled in the page); the layout-system is not part of the global stylesheet.
- Export PDF is best-effort — large dashboards with many panels can take several seconds and may exceed PDF page bounds; the implementation paginates tabs but not panels.
- The AI Q&A panel and the NL filter both share Prospector's `streamChat`, but with different system prompts. See [`components/prospector.md`](components/prospector.md) for the chat client.
