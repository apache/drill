# File uploads

Drag a file into a project and it is queryable. An uploaded file is not a new kind of object: it is a file in a workspace, read by whichever format plugin claims its extension. The design rationale is in [`docs/superpowers/specs/2026-11-09-file-uploads-design.md`](../superpowers/specs/2026-11-09-file-uploads-design.md). This page describes the code as committed.

## Moving parts

| Piece | Where |
|---|---|
| Cluster upload target (setting + status) | `UploadConfigResources.java` |
| Upload, delete, download | `ProjectFileResources.java` |
| Project role model, `folder` `DatasetRef` | `ProjectResources.java` |
| Schema cache for files | `ProjectSchemaCache.java` |
| Uploads workspace exempt from the extension filter | `MetadataResources.java` (`getFiles`) |
| Frontend | `api/uploads.ts`, `hooks/useProjectFileUpload.ts`, `hooks/useConversationFileDrop.ts`, `components/project/ProjectFileDropZone.tsx`, `components/datasource/UploadTargetCard.tsx`, `components/schema-explorer/ContextMenu.tsx`, `components/schema-explorer/datasetAllowList.ts` |

There is no uploads list, no file metadata store and no Files tab. The schema explorer is the list of uploaded files.

## The upload target

One cluster-wide setting names a storage plugin and one of its workspaces. It is stored in a `PersistentStore` named `drill.sqllab.upload_config` (key `default`) and holds `plugin`, `workspace` and `maxSizeBytes` (default 1 GiB; a missing or non-positive value falls back to the default).

| Endpoint | Who | Purpose |
|---|---|---|
| `GET /api/v1/uploads/config` | admin | The stored setting, or an empty one |
| `PUT /api/v1/uploads/config` | admin | Validate and save the setting |
| `GET /api/v1/uploads/status` | any authenticated user | `{configured, plugin, workspace, maxSizeBytes}` |

`PUT` refuses a body without both `plugin` and `workspace` (400), an unknown plugin or workspace (404), a plugin that is not a `FileSystemPlugin` (400) and a workspace that is not writable (403). A successful response is `{config, warning}`.

`status` is open to any authenticated user because the drop surfaces and the context menu both need it, and a plugin and workspace name are not credentials. The admin UI is `UploadTargetCard`, shown on the Data Sources page.

### Multi-node caveat

If the target's connection starts with `file:` and more than one Drillbit is registered, `PUT` still saves and returns a `warning`. It is not refused. The upload lands on whichever node served the request, so queries planned onto other nodes will not see the file. Use shared storage (HDFS, S3) on a cluster unless this is intended.

## Per-project folder

Each project's files live in `<workspace location>/<projectId>` under the plugin's connection. The folder is named after the project id, not the name, so a rename does not move it. The folder is created on the first upload, not at project creation, so a cluster with no target configured grows no empty folders.

On the first successful upload the project gains one dataset reference of type `folder` (label "Uploaded files") with `schema = <plugin>.<workspace>` and `table = <projectId>`. `DatasetRef` otherwise understands `plugin`, `schema` and `table`; none of those fits a per-project directory inside a shared workspace without exposing every project's folder to every project. The frontend handles `folder` in `datasetAllowList.ts` (a `folders` map of schema to allowed folder paths, matching the folder and everything beneath it) and in `SchemaExplorer.tsx`.

Uploads are always flat in the project folder. Deleting a project is a soft delete and leaves its files in place; orphaned folders are an administrator's cleanup.

## Project access

Uploads are the first consumer of a small per-project permission model in `ProjectResources`:

| Field / rule | Meaning |
|---|---|
| `owner` | Can do everything |
| `sharedWith` | Editors: can read and write |
| `viewers` | Read-only |
| `isPublic` | Any authenticated user can read; grants no write access |

Two static helpers decide it, and the project endpoints use them rather than a copy of the rule:

- `canRead(project, user)`: owner, `isPublic`, `sharedWith` or `viewers`.
- `canWrite(project, user)`: owner or `sharedWith`. Viewers and `isPublic` do not write.

`canAdminister(project, user)` (owner only) gates delete, restore, purge and any change to `isPublic`, `sharedWith` or `viewers` (`changesSharing`). An editor can edit content but cannot change who has access. Soft-deleted projects are rejected separately by callers (`getDeletedAt() > 0`).

Upload and delete need `canWrite`; download needs `canRead`.

## Endpoints: `/api/v1/projects/{id}/files`

All require an authenticated user and the project access above.

### Upload: `POST /api/v1/projects/{id}/files`

Multipart form with the file in a part named `file`. `?overwrite=true` replaces an existing file of the same name. Response `201`:

```json
{ "name": "sales.csv", "path": "sales.csv", "schema": "dfs.uploads.<projectId>" }
```

| Status | Cause | Body |
|---|---|---|
| 400 | No `file` part, or a bad name (empty, over the length limit, contains `/`, `\`, `..` or a control character, or does not resolve inside the project folder) | `{message}` |
| 403 | Caller lacks `canWrite` | `{message}` |
| 404 | Project missing or soft-deleted | `{message}` |
| 409 | `EXISTS`: a file of that name exists and `overwrite` was not set | `{code: "EXISTS", message}` |
| 409 | `NOT_CONFIGURED`: no target set, or the configured plugin or workspace no longer exists | `{code: "NOT_CONFIGURED", message}` |
| 413 | Over `maxSizeBytes` | `{message}` |
| 500 | Write failed; detail is logged, not returned | `{message}` |

The 409 `code` is what the client keys off. `uploadProjectFile` in `api/uploads.ts` throws `UploadCollisionError` on `EXISTS` and `UploadNotConfiguredError` on `NOT_CONFIGURED`; `useProjectFileUpload` turns the first into a "Replace existing file?" confirm that retries with `overwrite=true`, and the second into a message telling the user to ask an administrator. Any other 409 is rethrown unchanged.

Nothing looks at the file's extension. Drill decides at query time what it can read, so an unreadable file is a query error, not an upload error.

### Delete: `DELETE /api/v1/projects/{id}/files/{name}`

Needs `canWrite`. `204` on success, `404` if the file is not there. Removes only that file's entry from the schema cache.

### Download: `GET /api/v1/projects/{id}/files/{name}/content`

Needs `canRead`. Streams the bytes through the Drillbit as `application/octet-stream` with a `Content-Disposition: attachment` header, so one path serves local disk and S3. A missing file is `404`.

Delete and download share a locator that applies the same name, traversal and target checks as upload. Their refusal bodies are fixed text so a missing file never reveals whether the project folder exists.

## How the bytes are written

- **Hadoop `FileSystem`, not `java.nio.file`.** Writes use `FileSystemPlugin.getFsConf()` and the plugin's `connection`, so a local `dfs` workspace and an `s3a://` one take the same code path. Nothing branches on the scheme.
- **Staged, then renamed.** Bytes are written to `<name>.upload-<uuid>` beside the target and renamed onto it once all of them have arrived. An aborted or oversize upload deletes its staging file and never touches an existing file.
- **A failed rename leaves the staging file in place.** This is deliberate: the staged file holds the only copy of what the user uploaded, so it stays under a recoverable name and its full path is written to the Drillbit log ("The uploaded bytes remain at ..."). Nothing sweeps these. Administrators should look for `*.upload-*` files in a project folder after a reported failure and rename or remove them by hand. For an overwrite the old file is deleted just before the rename (Hadoop's `rename` would otherwise move the source into a directory sitting at the target name), so if that rename then fails the old file is gone and the new bytes are in the staging file.
- **Size cap, two layers.** The first is a `Content-Length` gate (`UploadSizeFilter`, a request filter bound only to this resource) that rejects a request over `maxSizeBytes` plus 8 KiB of multipart envelope headroom with `413` before Jersey reads the body. Jersey otherwise spools any part over a few kilobytes to the Drillbit's temp directory to satisfy `@FormDataParam`, before the handler runs. The second is enforced while streaming: the copy stops one byte past the cap and discards the staging file. A chunked request that declares no `Content-Length` bypasses the first layer, so its body is still spooled to temp before the cap applies. The gate narrows that exposure rather than closing it.
- **Name limit.** Names are limited to 255 bytes minus the staging suffix, so the staging name still fits in one path component.
- **Impersonation.** When `drill.exec.impersonation` is enabled, write, delete and download run inside `ImpersonationUtil.createProxyUgi(user).doAs(...)`, so files belong to the uploading user. Writing as the Drillbit user would produce files the uploader cannot read, and Drill cannot fix that afterwards. With impersonation off, the process user is used.

## Schema cache

`INFORMATION_SCHEMA` lists a workspace's views and nothing else, so `ProjectSchemaCache.infoSchemaScan` cannot see files. Without more, an uploaded file would appear in the schema tree (which uses `SHOW FILES`) but not in autocomplete or in Prospector's context. Three pieces close that:

- **`putTable` / `removeTable` (the write path).** After an upload, `ProjectFileResources` probes only the file just written (`ProjectSchemaCache.fileEntry`, a `SELECT * ... LIMIT 1`) and merges it with `putTable`. After a delete it calls `removeTable` for that one entry. Neither rescans the folder, because a whole-folder scan runs one query per file on the request thread inside the cache's monitor. `putTable` deliberately does not bump `scannedAt`. Both are best-effort: a cache failure is logged and never fails a write that has already landed.
- **`fileScan` (the sweep).** Used on the dataset-scan path, where a sweep is what is being asked for. It lists with `SHOW FILES`, descends into subdirectories, and probes each file. Bounds: 500 probes (`MAX_TABLES_PER_SCHEMA`, enumeration stops one past it), 100 `SHOW FILES` listings (`MAX_FOLDER_LISTINGS`) and 10 levels of depth (`MAX_FOLDER_DEPTH`), with a 30 second deadline on each query. When any bound is hit the result is marked truncated rather than cached as if it were the whole folder. A query that hits its deadline fails the scan instead of caching a partial result.
- **`combinedScan`.** A schema can be referenced both as a plain schema and as one or more upload folders, and the cache holds a single entry per schema path, so the two are scanned together: `infoSchemaScan` plus a `fileScan` per folder.

A file Drill cannot parse (an unreadable file, or one no format plugin claims) is cached as a table with no columns. It does not fail the scan.

Table names in the cache are the path relative to the schema, `<projectId>/<file>`, and are queryable as ``dfs.uploads.`<projectId>/<file>` ``.

## Listing: the extension filter

`MetadataResources.getFiles` normally lists only files whose extension some format config claims. For the configured uploads workspace (`isUploadTarget`) it lists every file, so an upload such as a `.docx` is visible and can be deleted even though Drill cannot query it.

## Frontend

- **Drop surfaces.** `ProjectFileDropZone` wraps the project layout and shows an overlay for drags that carry `Files` (in-app drags such as tree nodes are ignored). `ChatInput` accepts drops when given `onFileDropped`. The Data Sources tab has an "Upload file" button. A drop on the composer calls `markDropHandled` (`utils/dropHandled.ts`) and lets the event bubble, so the project zone resets its overlay but skips its own upload.
- **`useProjectFileUpload`.** Checks `getUploadStatus` first, uploads, reports the outcome, and invalidates `['schemaCache', projectId]` and `['project', projectId]`. It resolves with the stored file, or undefined on failure or a declined replace.
- **`useConversationFileDrop`.** Uploads through the same hook, then sends one ordinary user message naming each file and its query path, so Prospector can query it with `execute_sql`. No new Prospector tool. Files dropped together are announced in a single message.
- **Context menu.** `ContextMenu.tsx` gains Delete (confirms, naming the file) and Download for file nodes under the configured uploads schema in the current project. They are shown only for flat names: a nested file did not come from this feature and the endpoints take a single path segment, so the menu offers neither for it. Download is a plain link to the `/content` URL.
