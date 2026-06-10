# Project Detail (Settings)

**File:** `src/pages/ProjectDetailPage.tsx`
**Route:** `/projects/:id/settings`

## Purpose

Project administration: edit metadata, manage member datasets, view activity, see lineage. Reached from the project nav's "Settings" button (gear icon on the right side of `ProjectNavBar`).

## Tabs

| Tab | Shows |
|---|---|
| Overview | Description, owner, visibility, created/updated timestamps, item counts |
| Datasets | Datasets attached to the project — add / remove |
| Activity | `ProjectActivityFeed` of recent changes |
| Lineage | `ProjectLineage` visualization of data dependencies |

## Data sources

| API | Module |
|---|---|
| `getProject()` via `ProjectContext` | `api/projects.ts` (indirect) |
| `removeDataset()` (mutation) | `api/projects.ts` |

The activity feed and lineage components read from the project object — no extra fetches.

## Child components

- `ShareModal` — visibility + sharing settings
- `DatasetPickerModal` — pick datasets to add
- `ExportImportModal` — JSON export / import for the project
- `ProjectActivityFeed`
- `ProjectLineage`

## Key state

- `activeTab` — Overview / Datasets / Activity / Lineage
- `shareModalOpen`, `datasetRefModalOpen`, `exportModalOpen` — modal controllers
- `removeDatasetMutation` — invalidates `['project', projectId]` on success

## Chrome

Breadcrumb: `Projects > <Project Name> > Settings`. Toolbar: Share, Export.

## Behavior

- Dataset removal is confirmed via modal.
- Stat cards on Overview: Datasets, Saved Queries, Visualizations, Dashboards, Wiki Pages.
- Activity and Lineage tabs lazy-render their components from the in-context project — no API calls until the tab is opened.

## Quirks

- This is the only project page without a cross-project equivalent.
- Lineage data structure lives on the project itself; if the lineage graph is large, the visualization can be heavy — it's lazy-mounted on tab activation.
