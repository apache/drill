# Project Data Sources

**File:** `src/pages/ProjectDataSourcesPage.tsx`
**Route:** `/projects/:id/datasources`

## Purpose

Manage which datasets are bound to the project: tables, schemas, plugins, and saved queries. This is distinct from the global Data Sources page — global manages plugins, project manages **dataset references**.

## Data sources

| API | Module |
|---|---|
| `project.datasets` via `ProjectContext` | indirect |
| `removeDataset()` | `api/projects.ts` |

## Child components

- `DatasetPickerModal` — choose datasets to add (filters out anything already in the project)

## Key state

- `pickerOpen` — controls the picker modal

## Behavior

- Renders an AntD `Card` with a `List` of datasets.
- Each row: dataset name + type + per-row buttons:
  - **Delete** — confirms, then `removeDataset` + invalidate
  - **Settings** — for plugin-backed datasets, navigates to `/projects/:id/datasources/:name` (the `ProjectDataSourceEditPage` wrapper)
- Empty state: "No datasets added to this project yet."
- "Add Datasets" button opens the picker.

## File upload

The header has an **Upload file** button (AntD `Upload`, `beforeUpload` hands the file to `useProjectFileUpload`), the discoverable twin of dragging a file anywhere in the project. The first upload adds a `folder` dataset ("Uploaded files") to the list; it is an ordinary row and can be removed like any other, which only drops the reference; the next upload adds it back, because the server only checks whether any folder reference exists. Files themselves are deleted from the schema explorer. Full behavior: [`../../FileUploads.md`](../../FileUploads.md).

## Chrome

Breadcrumb: `Projects > <Project Name> > Data Sources`. Toolbar: Add Datasets.

## Quirks

- Saved-query datasets do NOT get a Settings button — they're configured from the Saved Queries page.
- Removing a dataset from a project doesn't delete the underlying plugin or saved query — only the project's reference to it.
- For plugin configuration, the Settings button hands off to `DataSourceEditPage` (with `projectId` set so back-nav returns here). See [`data-sources.md`](data-sources.md).
