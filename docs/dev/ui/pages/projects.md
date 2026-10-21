# Projects page

**File:** `src/pages/ProjectsPage.tsx`
**Route:** `/projects` (also the app's root redirect target)

## Purpose

The home page. Lists every project the user has access to as a tile grid. Lets the user create, edit, favorite, soft-delete, and import/export projects, and is the jumping-off point into project-scoped work.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /api/v1/projects` (`getProjects`) | `api/projects.ts` | The grid |
| `POST /api/v1/projects` (`createProject`) | `api/projects.ts` | New project modal |
| `PUT /api/v1/projects/{id}` (`updateProject`) | `api/projects.ts` | Edit modal |
| `DELETE /api/v1/projects/{id}` (`deleteProject`) | `api/projects.ts` | Soft-delete |
| `POST /api/v1/projects/{id}/restore` (`restoreProject`) | `api/projects.ts` | Undo-delete toast |
| `GET /api/v1/projects/favorites` (`getFavorites`) | `api/projects.ts` | Favorite tiles |
| `POST /api/v1/projects/favorites/{id}` (`toggleFavorite`) | `api/projects.ts` | Star button |

Query keys: `['projects']`, `['project-favorites']`. Mutations invalidate both as appropriate.

## Child components

- `ProjectTile` — card with appearance (color or image), name, description, metadata counts (datasets, queries, viz, dashboards), favorite star, actions menu (Edit, Export, Delete).
- `NewProjectTile` — call-to-action card that opens the create modal.
- `ProjectsWelcome` — first-run state shown when there are no projects.
- `ProjectAppearanceField` — color / image picker used by both create and edit modals.
- `ExportImportModal` — JSON export and import for project definitions.

## Key state

- `searchText` — filter
- `favoritesOnly` — boolean filter toggle
- `createModalOpen`, `createCover`, `createForm` — new project modal
- `editModalOpen`, `editingProject`, `editCover` — edit modal
- Server data via react-query (`['projects']`, `['project-favorites']`)
- `useUndoableDelete` hook stages deletes and shows an Undo toast that calls `restoreProject` if clicked

## Behavior

- Tiles are sorted favorites-first, then by last-updated descending.
- Soft-delete shows an Undo toast; the project is hidden immediately but only purged after the toast expires.
- Editing the appearance immediately updates the sidebar's project glyph because both read the same color via `usePinnedRecentProjects` / `getProjects` cache.
- Import accepts a JSON blob produced by Export.

## Chrome

Registers a breadcrumb (`Home`) and a toolbar action for "New Project". No inspector tabs, no left rail.

## Quirks

- The "favorites" set is server-side state but the toggle updates local UI immediately and reconciles on mutation success.
- `useUndoableDelete` is shared with Saved Queries, Visualizations, and Dashboards — same pattern across the library pages.
