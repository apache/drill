# Saved Queries

**File:** `src/pages/SavedQueriesPage.tsx`
**Routes:** `/saved-queries` (cross-project), `/projects/:id/queries` (project-scoped via `ProjectSavedQueriesPage` wrapper)

## Purpose

Browse, preview, edit, schedule, and run saved SQL queries. Two-pane layout: a list on the left, a preview of the selected query on the right with metadata, SQL, and actions.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /api/v1/savedQueries` (`getSavedQueries`) | `api/savedQueries.ts` | The list |
| `PUT /api/v1/savedQueries/:id` (`updateSavedQuery`) | `api/savedQueries.ts` | Edit modal |
| `DELETE /api/v1/savedQueries/:id` (`deleteSavedQuery`) | `api/savedQueries.ts` | Soft-delete |
| `POST /api/v1/savedQueries/:id/restore` (`restoreSavedQuery`) | `api/savedQueries.ts` | Undo toast |
| `GET /api/v1/projects` (`getProjects`) | `api/projects.ts` | Add-to-project dropdowns |
| `GET /api/v1/schedules` (`getSchedules`) | `api/schedules.ts` | "Scheduled" filter, schedule status |
| Current user (`useCurrentUser`) | `hooks/useCurrentUser.ts` | `canEdit`, `canView` checks |

## Child components

- `SelectedQueryPreview` — right pane with read-only Monaco editor showing the SQL, plus metadata and action buttons (Run in SQL Lab, Edit, Delete, Schedule, Add to Project).
- `AddToProjectModal`, `BulkAddToProjectModal` — assignment dialogs.
- `ScheduleModal` — configure a schedule for the selected query.

## Key state

- `searchText` — filter
- `filter` — `'all' | 'mine' | 'public' | 'scheduled'`
- `selectedId` — highlighted row (drives right pane)
- `editModalOpen`, `editingQuery` — edit dialog
- `bulkSelected` — `Set<string>` of row IDs for bulk operations
- `projectFilter` — filter list to queries inside a chosen project
- `scheduleQueryId`, `scheduleQueryName`, `scheduleQuerySql` — schedule modal state
- `useUndoableDelete` — soft-delete with undo toast

## Behavior

- Auto-selects the first item on mount and whenever the filter changes (so the right pane is never empty).
- `'scheduled'` filter cross-references `/api/v1/schedules` for enabled entries matching each query's id.
- "Run in SQL Lab" navigates to `/query` with `state: { sql, name }` so the editor opens with the query pre-loaded into a new tab.
- Bulk delete via checkbox-driven selection; the bulk row appears when at least one checkbox is ticked.
- Add-to-project (single or bulk) calls into the project's saved-query-ids array via `updateProject`.

## Chrome

Breadcrumb: `Library > Saved Queries` (cross-project) or `Project > Saved Queries` (project-scoped). Toolbar: search, filter chips, bulk-action menu.

## Quirks

- The page is exported as a default React component but accepts optional props: `filterIds?: string[]`, `projectId?`, `projectOwner?`. The project wrapper passes `project.savedQueryIds`. Cross-project usage leaves them undefined.
- Read-only Monaco in the preview pane re-creates an editor instance per selection change — fine in practice because Monaco caches its workers.
- Undo-restore uses the same `useUndoableDelete` hook as Projects, Visualizations, Dashboards.

---

## ProjectSavedQueriesPage

**File:** `src/pages/ProjectSavedQueriesPage.tsx`
**Route:** `/projects/:id/queries`

Decorator: pulls `project.savedQueryIds` from `useProjectContext()` and renders `SavedQueriesPage` with `filterIds={project.savedQueryIds}`. Also opens an `AddItemModal` to add globally-existing saved queries into the project (mutates the project, not the saved queries themselves).

The list UI is identical; the only differences are the filter and the project-aware breadcrumb / "Add" button surfaced by the wrapper.
