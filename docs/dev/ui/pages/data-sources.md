# Data Sources

Covers two pages and their project-scoped wrappers.

## DataSourcesPage

**File:** `src/pages/DataSourcesPage.tsx`
**Route:** `/datasources`

### Purpose

Lists every configured storage plugin. Enable / disable, edit, authorize OAuth, export, or delete.

### Data sources

| API | Module | Used for |
|---|---|---|
| `GET /storage.json` (`getPlugins`) | `api/storage.ts` | The grid |
| `POST /storage/{name}/enable/{val}` (`enablePlugin`) | `api/storage.ts` | Enable / disable switch |
| `DELETE /storage/{name}.json` (`deletePlugin`) | `api/storage.ts` | Delete |
| `GET /storage/{name}/export` (`getExportUrl`) | `api/storage.ts` | Download config JSON |
| `POST /api/v1/projects/cleanup-plugin-datasets/{name}` (`cleanupPluginDatasets`) | `api/projects.ts` | Orphan project datasets pointing at a deleted plugin |

### Child components

- `NewDataSourceTile` — create button (opens the new-data-source modal).
- Per-plugin tile — logo (by type), connection snippet, enabled switch, actions menu (Edit, Authorize, Export, Delete).

### Key state

- `searchText` — filter
- `filterMode` — `'all' | 'enabled' | 'disabled'`
- `createModalOpen`, `createForm` — new-plugin modal
- `deleteMutation`, `enableMutation` — react-query mutations

### Behavior

- Sorted by enabled-first, then name.
- OAuth detection (`isOAuthPlugin`) inspects the plugin config for `authorizationURL` + `clientID`. OAuth-capable plugins show an Authorize button.
- Authorize opens a popup; on close, the page refetches plugins so the new credentials are visible.
- Delete shows a confirmation: deleting a plugin orphans any project datasets pointing at it, so the cleanup endpoint is called too.
- Editing or creating opens [DataSourceEditPage](#datasourceeditpage).

### Quirks

- Plugin types come from the plugin metadata, not a hardcoded list — adding a new Drill storage type does not require a UI change to appear, but adding a guided form does (see below).

---

## DataSourceEditPage

**File:** `src/pages/DataSourceEditPage.tsx`
**Routes:** `/datasources/:name`, `/projects/:id/datasources/:name`

### Purpose

Configure a storage plugin. Two tabs: a type-aware guided form (when one exists for the plugin type) and a raw JSON Monaco editor. Used for both create and update.

### Data sources

| API | Module |
|---|---|
| `GET /storage/{name}.json` (`getPlugin`) | `api/storage.ts` |
| `POST /storage/{name}.json` (`savePlugin`) | `api/storage.ts` |
| `DELETE /storage/{name}.json` (`deletePlugin`) | `api/storage.ts` |
| `POST /storage/{name}/enable/{val}` (`enablePlugin`) | `api/storage.ts` |
| `GET /storage/{name}/export` (`getExportUrl`) | `api/storage.ts` |

### Child components

In `src/components/storage/`:

- Guided forms, one per plugin type with bespoke UI:
  - `FileSystemForm`, `JdbcForm`, `HttpForm`, `MongoForm`, `SplunkForm`, `CassandraForm`, `DruidForm`, `ElasticsearchForm`, `GoogleSheetsForm`, `HBaseForm`, `HiveForm`, `KafkaForm`, `KuduForm`, `OpenTSDBForm`, `PhoenixForm`
- `@monaco-editor/react` `Editor` for the JSON tab.

If a plugin type has no matching form, only the JSON tab is shown.

### Key state

- `config` — parsed JSON object backing the guided form.
- `jsonText` — raw JSON string backing the JSON tab.
- `activeTab` — `'config' | 'json'`.
- `dirty` — unsaved changes flag.
- `isValid` — form validation result; gates the Save button.
- `pluginName` — from URL params (or undefined for new).

For new plugins the URL is `/datasources/new` with a `location.state.config` payload describing the template.

### Chrome

Breadcrumb is project-aware: `/projects/:id/datasources/:name` shows `Project > Data Sources > Plugin`; cross-project shows `Data Sources > Plugin`. The `projectId` prop comes from the wrapping `ProjectDataSourceEditPage`.

### Behavior

- **Tab sync.** Switching from guided form to JSON re-serializes `config`. Switching from JSON to form re-parses; invalid JSON keeps the user on the JSON tab.
- **Validation.** Forms can opt into `onValidationChange` to expose their valid/invalid state to the page (e.g. FileSystemForm requires at least one workspace).
- **OAuth.** Detected as in DataSourcesPage; if present, exposes an Authorize button that opens the OAuth flow.
- **Delete / Disable.** Warn about downstream impact on project datasets.
- **Export.** Downloads the config JSON.
- **Dark mode.** Monaco theme picked from `useTheme().isDark`.

### Quirks

- The guided forms have grown organically; some types have form-only fields that aren't represented in the JSON (e.g. UI hints). The "JSON" tab always reflects what gets sent to the backend.
- Adding a new guided form: create a component in `src/components/storage/`, register it in the form-by-type map (find via `grep "JdbcForm" src/pages/DataSourceEditPage.tsx`), and add a logo to the plugin metadata.

---

## ProjectDataSourceEditPage

**File:** `src/pages/ProjectDataSourceEditPage.tsx`
**Route:** `/projects/:id/datasources/:name`

Thin wrapper: pulls `projectId` from `useProjectContext()` and passes it to `DataSourceEditPage` so the back / breadcrumb nav returns to the project's Data Sources tab instead of the global list.

For the project list view itself, see [`project-data-sources.md`](project-data-sources.md) — that's a separate component because the project view supports dataset CRUD on top of plugin browsing.
