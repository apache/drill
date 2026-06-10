# Project Layout

**File:** `src/components/project/ProjectLayout.tsx`
**Route:** `/projects/:id/*` (wraps all project-scoped pages)

## Purpose

The shell for everything that happens inside a project. Loads the project, provides project state via `ProjectContext`, renders the tabbed project nav (`ProjectNavBar`), and renders child pages via React Router's `<Outlet />`.

## Architecture

```
/projects/:id  →  ProjectLayout
                  ├─ ProjectContextProvider  (fetches /api/v1/projects/:id, provides via useProjectContext())
                  ├─ ProjectNavBar           (horizontal tabs mirroring the sidebar project sub-nav)
                  └─ <Outlet />              (renders the active child route)
```

`ProjectContextProvider` is in `src/contexts/ProjectContext.tsx`. It exposes the project plus memoized `Set`s of saved-query, visualization, and dashboard ids for O(1) filtering by child pages. See [`../contexts.md`](../contexts.md) for the full hook signature.

## Tabs

`ProjectNavBar` renders, in order:

- Query → `/projects/:id/query`
- Saved Queries → `/projects/:id/queries`
- Visualizations → `/projects/:id/visualizations`
- Dashboards → `/projects/:id/dashboards`
- Data Sources → `/projects/:id/datasources`
- Workflows → `/projects/:id/workflows` (conditional; same visibility rule as the sidebar — only when the project has scheduled queries)
- Wiki → `/projects/:id/wiki`
- (right-aligned) Recent items chip, project switcher dropdown, Settings button → `/projects/:id/settings`

The active tab is derived from the URL pathname. The project switcher lets users jump to another project while staying on the same tab.

## Child pages

Each tab is a separate page component:

| Tab | Route | Component | Doc |
|---|---|---|---|
| Query | `query` | `ProjectQueryPage` | [`project-query.md`](project-query.md) |
| Saved Queries | `queries` | `ProjectSavedQueriesPage` | Wrapper; see [`saved-queries.md`](saved-queries.md) |
| Visualizations | `visualizations` | `ProjectVisualizationsPage` | Wrapper; see [`visualizations.md`](visualizations.md) |
| Visualization detail | `visualizations/:vizId` | `ProjectVisualizationDetailPage` | Wrapper; see [`visualizations.md`](visualizations.md) |
| Dashboards | `dashboards` | `ProjectDashboardsPage` | Wrapper; see [`dashboards.md`](dashboards.md) |
| Dashboard view | `dashboards/:dashboardId` | `DashboardViewPage` | Shared; see [`dashboards.md`](dashboards.md) |
| Data Sources | `datasources` | `ProjectDataSourcesPage` | [`project-data-sources.md`](project-data-sources.md) |
| Data Source edit | `datasources/:name` | `ProjectDataSourceEditPage` | Wrapper; see [`data-sources.md`](data-sources.md) |
| Workflows | `workflows` | `ProjectWorkflowsPage` | [`project-workflows.md`](project-workflows.md) |
| Wiki | `wiki`, `wiki/:pageId` | `ProjectWikiPage` | [`project-wiki.md`](project-wiki.md) |
| Settings | `settings` | `ProjectDetailPage` | [`project-detail.md`](project-detail.md) |

Project pages come in three flavors:

1. **Project-specific** — no cross-project equivalent. `ProjectDetailPage`, `ProjectQueryPage`, `ProjectWikiPage`, `ProjectDataSourcesPage`. Documented in their own files.
2. **Decorators** — render the cross-project page with `filterIds` and an "Add Item" modal that mutates the project's id arrays. `ProjectSavedQueriesPage`, `ProjectVisualizationsPage`, `ProjectDashboardsPage`. Documented as a short section inside the matching cross-project page doc.
3. **Pass-through wrappers** — pull `projectId` from `ProjectContext` and pass it to the cross-project page so breadcrumb / back-nav stay in-project. `ProjectVisualizationDetailPage`, `ProjectDataSourceEditPage`, `ProjectWorkflowsPage`. Documented as a section inside the matching cross-project page doc.

## Data sources

| API | Module | Used for |
|---|---|---|
| `GET /api/v1/projects/:id` (`getProject`) | `api/projects.ts` | Owner of `ProjectContext` |
| `PUT /api/v1/projects/:id` (`updateProject`) | `api/projects.ts` | Project edit / appearance |
| `DELETE /api/v1/projects/:id` (`deleteProject`) | `api/projects.ts` | Delete from Settings |

## Behavior

- A 404 on `getProject` does not retry; the layout renders an empty state and a back link to `/projects`.
- All child pages read from `useProjectContext()` rather than re-fetching the project — saves duplicate requests.
- Updating the project (rename, appearance change, dataset change) invalidates `['project', projectId]` and re-renders all subscribed children.
- The "Workflows" tab visibility is computed in `ProjectNavBar` the same way the sidebar computes it: cross-reference `project.savedQueryIds` against `/api/v1/schedules`.

## Quirks

- `ProjectContextProvider` is a hard requirement for project pages. Calling `useProjectContext()` outside it throws. This intentional guard makes mounting outside `ProjectLayout` fail loudly rather than silently.
- The project switcher dropdown does not include archived / deleted projects.
- The "Recent items" chip is populated by `usePinnedRecentProjects` and shows the user's most recently visited items inside this project — distinct from the sidebar's recents.
