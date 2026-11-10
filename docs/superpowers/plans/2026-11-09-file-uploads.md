# File Uploads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drag a file into a project and query it, with no SQL and no configuration by the person doing it.

**Architecture:** An admin names one writable storage plugin workspace as the cluster's upload target. Each project writes into a folder under it named by project id, and that folder is registered as an ordinary `DatasetRef`, so the existing schema tree, Data Sources tab and project filtering show it. Bytes are written through the Hadoop `FileSystem` API so local disk and S3 share one code path. A new file-aware scan feeds uploaded files into `ProjectSchemaCache`, which is what autocomplete and Prospector read.

**Tech Stack:** Java 8 / Jersey (`exec/java-exec`), Hadoop `FileSystem`, JUnit 4 + `ClusterTest`; React 18 + TypeScript + AntD + react-query (`exec/java-exec/src/main/resources/webapp`), Vitest + Testing Library.

**Spec:** [docs/superpowers/specs/2026-11-09-file-uploads-design.md](../specs/2026-11-09-file-uploads-design.md)

## Global Constraints

- Every new source file (Java, TS, TSX, CSS) carries the Apache 2.0 license header.
- After any change under `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. All `if` statements use braces; no unused imports.
- No per-vendor branching anywhere near storage: one code path for `file://` and `s3a://`.
- Developer docs live under `docs/dev/`, never beside source.
- Commit messages are imperative. Do not add Claude as a co-author.
- No new files under `src/main/resources/webapp/` outside the existing SPA source tree, so `exec/jdbc-all`'s `webapp/**` exclusion still covers everything.
- The upload path is format-agnostic: no code in it may branch on file extension.

## Review Focus

Input classes the spec implies that no task's happy path exercises. Each has a test in the task that owns it.

1. A filename containing `../` or a path separator — must be refused, never resolved outside the project folder (Task 3).
2. An upload when no target workspace is configured — must return a clear "ask an admin" error, not a 500 (Task 3).
3. A file whose extension no format plugin claims — must still be listed and deletable, not invisible (Task 6).
4. A file Drill cannot parse — its column probe fails, and the scan must cache it with no columns rather than failing the whole scan (Task 7).
5. A second project's upload folder — must never appear in the first project's tree (Task 5).

---

### Task 1: Project write access

Today 16 call sites in `ProjectResources` gate mutations on `project.getOwner().equals(getCurrentUser())`, and `canRead` (`ProjectResources.java:1505`) is the shared read rule. Upload needs a write rule that a collaborator passes. The spec's "sharedWith entries gain an optional role" is implemented as a second list rather than a shape change, so stored projects and the frontend `sharedWith: string[]` type keep working: **`sharedWith` means editor, the new `viewers` means read-only.**

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java`
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/index.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/project/ShareModal.tsx`
- Create (test): `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectResources.java` (no such file today)

**Interfaces:**
- Produces: `public static boolean canWrite(Project project, String user)` — owner or `sharedWith` member. `canRead` additionally accepts `viewers`. `Project.getViewers()` / `setViewers(List<String>)`, serialized as `viewers`, defaulting to an empty list.

- [ ] **Step 1: Write the failing tests** in `TestProjectResources`:

```java
testCanWriteAllowsOwnerAndSharedWith()   // owner -> true; name in sharedWith -> true
testCanWriteRejectsViewerAndStranger()   // name in viewers -> false; unknown name -> false
testCanWriteIgnoresIsPublic()            // isPublic project, stranger -> false
testCanReadAllowsViewer()                // name in viewers -> true
testViewersDefaultsToEmptyForStoredProject() // Project deserialized from JSON without a
                                             // "viewers" key -> getViewers() is empty, not null
```

- [ ] **Step 2: Run them and watch them fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectResources`
Expected: FAIL, `canWrite` not defined.

- [ ] **Step 3: Add `viewers` to `Project`** — field, `@JsonProperty`, constructor parameter (defaulting to an empty list like its neighbors), getter and setter.

- [ ] **Step 4: Add `canWrite` beside `canRead`**, and add the `viewers` clause to `canRead`. Give `canWrite` a Javadoc saying it is the single write rule and that callers reject soft-deleted projects separately, as `canRead`'s does.

- [ ] **Step 5: Replace all 16 owner checks** with `canWrite(project, getCurrentUser())`, keeping each existing response body except the message, which becomes `"You do not have write access to this project"`.

- [ ] **Step 6: Run the tests and the suite**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectResources` then `mvn checkstyle:check -pl exec/java-exec`
Expected: PASS, 0 violations.

- [ ] **Step 7: Frontend** — add `viewers?: string[]` to the `Project` and project-update types in `types/index.ts`, and give `ShareModal` a per-name role select (Editor / Viewer) writing into `sharedWith` or `viewers`. A name already in `sharedWith` shows as Editor.

- [ ] **Step 8: Run the frontend gates**

Run (from `exec/java-exec/src/main/resources/webapp`): `npx tsc --noEmit && npm run lint && npx vitest run`
Expected: PASS.

- [ ] **Step 9: Commit** — `Add editor and viewer roles to projects`

---

### Task 2: The upload target setting

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/UploadConfigResources.java`
- Create (test): `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestUploadConfigResources.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/DrillRestServer.java` (register the resource beside the others)

**Interfaces:**
- Produces: `UploadConfig` POJO — `String plugin`, `String workspace`, `long maxSizeBytes` (default `1_073_741_824L`), persisted in a `PersistentStore<UploadConfig>` under the key `drill.sqllab.upload_config`, obtained exactly as `AiConfigResources.getStore()` does.
- Produces: `public static UploadConfig current(PersistentStoreProvider provider)` — the stored config or `null`, for Task 3 to call.
- Endpoints: `GET /api/v1/uploads/config` (admin, full config), `PUT /api/v1/uploads/config` (admin), `GET /api/v1/uploads/status` (authenticated) returning `{configured, plugin, workspace, maxSizeBytes}`.

- [ ] **Step 1: Write the failing tests:**

```java
testStatusReportsUnconfiguredWhenNothingStored()  // configured == false, plugin == null
testPutRejectsNonFileSystemPlugin()               // 400, message names the plugin
testPutRejectsNonWritableWorkspace()              // 403, message names the workspace
testPutThenStatusReportsConfigured()              // round trip through the store
testPutWarnsOnLocalConnectionWithMultipleBits()   // 200, body carries a non-empty `warning`
```

- [ ] **Step 2: Run them and watch them fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestUploadConfigResources`

- [ ] **Step 3: Write `UploadConfigResources`.** Validation on PUT mirrors `NotebookResources.java:330-351`: the plugin must exist and be a `FileSystemPlugin`, the named workspace must exist and `isWritable()`. The multi-node warning is advisory only — if `fsConfig.getConnection()` starts with `file:` and the cluster coordinator reports more than one registered Drillbit, the 200 response carries a `warning` string; it never refuses.

- [ ] **Step 4: Run the tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=TestUploadConfigResources && mvn checkstyle:check -pl exec/java-exec`
Expected: PASS, 0 violations.

- [ ] **Step 5: Commit** — `Add a cluster-wide upload target setting`

---

### Task 3: The upload endpoint

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectFileResources.java`
- Create (test): `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectFileResources.java`
- Modify: `DrillRestServer.java` (register)

**Interfaces:**
- Consumes: `UploadConfigResources.current(...)` (Task 2), `ProjectResources.canWrite(...)` (Task 1).
- Produces: `POST /api/v1/projects/{id}/files` — multipart, part name `file`, optional `?overwrite=true`. 201 with `{name, path, schema}` where `schema` is `<plugin>.<workspace>.<projectId>` and `path` is the name within it.
- Produces: `public static Path projectDir(UploadConfig cfg, FileSystemConfig fsConfig, String projectId)` and `public static String safeName(String raw)`, both used by Task 4.

- [ ] **Step 1: Write the failing tests:**

```java
testRejectsPathTraversal()        // "../../etc/passwd" and "a/b.csv" -> 400, nothing written
testRejectsWhenUnconfigured()     // 409, message contains "upload folder"
testRejectsWithoutWriteAccess()   // viewer of the project -> 403
testRejectsOversizeFile()         // maxSizeBytes = 10, 11-byte part -> 413, nothing written
testCollisionRequiresOverwrite()  // second upload of the same name -> 409;
                                  // with ?overwrite=true -> 201 and the new bytes on disk
testUploadWritesFileAndRegistersFolderDataset()
                                  // file exists under <ws>/<projectId>/; the project gains
                                  // exactly one DatasetRef of type "folder"; a second upload
                                  // adds no further DatasetRef
```

- [ ] **Step 2: Run them and watch them fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectFileResources`

- [ ] **Step 3: Write `ProjectFileResources`.** Notes the signatures do not settle:
  - Resolve the filesystem with `FileSystem.get(URI.create(fsConfig.getConnection()), fsPlugin.getFsConf())` — never `java.nio.file`, which only works for `file://`.
  - When `config.getBoolean(ExecConstants.IMPERSONATION_ENABLED)` is set, do the write inside `ImpersonationUtil.createProxyUgi(getCurrentUser()).doAs(...)`; otherwise write directly. A file written as the Drillbit user on an impersonating cluster is unreadable by its uploader and unfixable afterwards.
  - `safeName` refuses any name containing `/`, `\` or `..`, and the resolved path is `normalize()`d and verified to still start with the project directory, as `NotebookResources.java:367` does.
  - Stream part → output stream with `IOUtils.copyLarge`, counting bytes and aborting past `maxSizeBytes`; delete the partial file on abort. Do not read the part into memory.
  - Register the `DatasetRef` (`type: "folder"`, `schema: <plugin>.<workspace>`, `table: <projectId>`, `label: "Uploaded files"`) only if the project has no folder ref yet, inside the same `synchronized (id.intern())` block the other project mutations use.

- [ ] **Step 4: Run the tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectFileResources && mvn checkstyle:check -pl exec/java-exec`

- [ ] **Step 5: Commit** — `Add an endpoint for uploading a file into a project`

---

### Task 4: Delete and download

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectFileResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectFileResources.java`

**Interfaces:**
- Produces: `DELETE /api/v1/projects/{id}/files/{name}` → 204; `GET /api/v1/projects/{id}/files/{name}/content` → the bytes as `application/octet-stream` with a `Content-Disposition` filename.

- [ ] **Step 1: Write the failing tests:**

```java
testDeleteRemovesFile()                 // 204, file gone from disk
testDeleteRejectsPathTraversal()        // 400
testDeleteRejectsWithoutWriteAccess()   // viewer -> 403
testDownloadStreamsBytes()              // body equals what was uploaded
testDownloadAllowsViewer()              // read access is enough to download
testDownloadMissingFileIsNotFound()     // 404
```

- [ ] **Step 2: Run them and watch them fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectFileResources`

- [ ] **Step 3: Implement both**, reusing `projectDir` and `safeName`. Download is a `StreamingOutput` reading from the Hadoop `FileSystem` — never a redirect or a presigned URL, so one path serves local disk and S3.

- [ ] **Step 4: Run the tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectFileResources && mvn checkstyle:check -pl exec/java-exec`

- [ ] **Step 5: Commit** — `Let a project delete and download its uploaded files`

---

### Task 5: The `folder` dataset type

A per-project directory inside a shared workspace fits none of the existing ref types: `schema` would expose every project's folder to every project, and `table` is filtered out of the tree until its children load.

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/datasetAllowList.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/SchemaExplorer.tsx` (`filterTreeNodes`, from line 203)
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/index.ts` (`DatasetRef['type']` union)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/datasetAllowList.test.ts`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/TreeNodeBuilder.test.tsx` (filter cases live here already)

**Interfaces:**
- Produces: `DatasetAllowList.folders: Map<string, Set<string>>` — schema → set of folder paths.

- [ ] **Step 1: Write the failing tests:**

```ts
// datasetAllowList.test.ts
it('puts a folder ref in folders, not tables')
// filter tests
it('keeps a dir node that is an allowed folder')           // dir:dfs.uploads:proj-a
it('keeps a file inside an allowed folder')                // file:dfs.uploads:proj-a/sales.xlsx
it('drops another project\'s folder and its files')        // dir:dfs.uploads:proj-b -> gone,
                                                           // file:dfs.uploads:proj-b/x.csv -> gone
it('keeps a nested path under an allowed folder')          // proj-a/sub/x.csv
```

- [ ] **Step 2: Run them and watch them fail**

Run (from the webapp dir): `npx vitest run src/components/schema-explorer`

- [ ] **Step 3: Implement.** `buildDatasetAllowList` gains a `folder` branch keyed by `ds.schema` with `ds.table` as the path. In `filterTreeNodes`, a `file:` node is kept when its path equals or starts with `<folder>/`, and a `dir:` node is kept when its path equals a folder, starts with `<folder>/`, or is a prefix of one (so the parent chain survives to the folder).

- [ ] **Step 4: Run the tests**

Run: `npx vitest run src/components/schema-explorer && npx tsc --noEmit && npm run lint`

- [ ] **Step 5: Commit** — `Add a folder dataset type so a project sees only its own uploads`

---

### Task 6: Show every uploaded file, whatever its extension

`MetadataResources.getFiles` (line 603) lists only files whose extension a format config claims, so a `.docx` upload would be invisible and unreachable by Delete.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MetadataResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMetadataResources.java`

- [ ] **Step 1: Write the failing test:** `testUploadsWorkspaceListsUnrecognizedExtensions()` — with the upload target configured at `dfs.<ws>`, a `notes.docx` in that workspace appears in the listing, while the same file in another workspace does not.

- [ ] **Step 2: Run it and watch it fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestMetadataResources`

- [ ] **Step 3: Implement** — skip the `recognizedExtensions` filter when the requested schema is the configured upload target. The file remains unreadable by Drill; that is a query error, by design, but never an invisible file.

- [ ] **Step 4: Run the test and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=TestMetadataResources && mvn checkstyle:check -pl exec/java-exec`

- [ ] **Step 5: Commit** — `List every file in the uploads workspace`

---

### Task 7: Teach the schema cache to see files

`ProjectSchemaCache.infoSchemaScan` is built on `INFORMATION_SCHEMA`, which does not list files in a filesystem workspace — only views. Without this, an uploaded file is in the tree but never in autocomplete or in Prospector's context, and the headline story does not work.

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectSchemaCache.java`
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectFileResources.java` (refresh after write and delete)
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCache.java`

**Interfaces:**
- Produces: `public static List<CachedTable> fileScan(String schemaPath, String folder, WorkManager workManager, WebUserConnection conn)` — one `CachedTable` per file, `type` `"FILE"`, `name` the path relative to the schema, columns from `SELECT * FROM <file> LIMIT 1`.

- [ ] **Step 1: Write the failing tests:**

```java
testFileScanReturnsOneTablePerFile()      // two uploads -> two CachedTables, names are the
                                          // paths relative to the schema
testFileScanCachesColumnsFromTheProbe()   // a CSV with headers a,b -> columns a,b
testUnparseableFileIsCachedWithNoColumns()// a .docx -> one CachedTable, empty columns,
                                          // and the other file in the folder still scans
testUploadRefreshesTheCache()             // after POST, the new file is in the entry
testDeleteRefreshesTheCache()             // after DELETE, it is gone from the entry
```

- [ ] **Step 2: Run them and watch them fail**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectSchemaCache`

- [ ] **Step 3: Implement `fileScan`** — `SHOW FILES IN <schema>.`<folder>`` to enumerate (recursing into subdirectories), then one `SELECT * ... LIMIT 1` per file for columns, reusing the SQL shape of `MetadataResources.getFileColumns` (line 897). A probe that throws is caught per file and yields a `CachedTable` with no columns; it must not abort the scan. Honour `MAX_TABLES_PER_SCHEMA` and set `truncated` as `infoSchemaScan` does.

- [ ] **Step 4: Route folder datasets to it** — where `scanDatasetSchema` and `MetadataResources.java:848` pass `ProjectSchemaCache::infoSchemaScan`, a ref of type `folder` scans with `fileScan` instead.

- [ ] **Step 5: Refresh after upload, overwrite and delete** in `ProjectFileResources`, so a file is queryable by name without waiting for `STALE_INTERVAL_MS`.

- [ ] **Step 6: Run the tests and checkstyle**

Run: `mvn test -pl exec/java-exec -Dtest=TestProjectSchemaCache,TestProjectFileResources && mvn checkstyle:check -pl exec/java-exec`

- [ ] **Step 7: Commit** — `Scan uploaded files into the project schema cache`

---

### Task 8: The API client and the admin setting

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/api/uploads.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/DataSourcesPage.tsx`
- Create: `exec/java-exec/src/main/resources/webapp/src/components/datasource/UploadTargetCard.tsx`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/datasource/UploadTargetCard.test.tsx`

**Interfaces:**
- Produces: `getUploadStatus(): Promise<UploadStatus>`, `getUploadConfig()`, `setUploadConfig(cfg)`, `uploadProjectFile(projectId, file, opts?: {overwrite?: boolean}): Promise<UploadedFile>`, `deleteProjectFile(projectId, name)`, `downloadProjectFileUrl(projectId, name): string` — all in `api/uploads.ts`, following `api/storage.ts` for client conventions. `uploadProjectFile` posts `FormData` with the part named `file`.

- [ ] **Step 1: Write the failing tests** for `UploadTargetCard`: it shows "Not configured" with a prompt to pick a workspace when status says so; it lists only writable workspaces of filesystem plugins; it shows the returned `warning` after saving a `file://` target on a multi-node cluster.

- [ ] **Step 2: Run them and watch them fail**

Run: `npx vitest run src/components/datasource/UploadTargetCard.test.tsx`

- [ ] **Step 3: Implement** `api/uploads.ts` and the card, and mount the card on the admin Data Sources page.

- [ ] **Step 4: Run the gates**

Run: `npx tsc --noEmit && npm run lint && npx vitest run src/components/datasource`

- [ ] **Step 5: Commit** — `Add the upload target setting to the admin data sources page`

---

### Task 9: Dropping a file on a project

**Files:**
- Create: `exec/java-exec/src/main/resources/webapp/src/components/project/ProjectFileDropZone.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/project/ProjectLayout.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectDataSourcesPage.tsx` (a plain Upload button, so the feature is discoverable without the gesture)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/project/ProjectFileDropZone.test.tsx`

**Interfaces:**
- Consumes: `uploadProjectFile`, `getUploadStatus` (Task 8).
- Produces: `<ProjectFileDropZone projectId={id}>{children}</ProjectFileDropZone>` — wraps the project's pages, shows a full-area overlay reading `Drop to add to {projectName}` while a file is dragged over it, and nothing otherwise.

- [ ] **Step 1: Write the failing tests:**

```tsx
it('shows no overlay until a file is dragged over it')
it('shows the project name in the overlay while dragging')
it('uploads the dropped file and reports the new table path')
it('says to ask an admin when uploads are not configured')  // no upload call is made
it('asks before overwriting when the name is taken')        // 409 -> confirm -> retry with overwrite
```

- [ ] **Step 2: Run them and watch them fail**

Run: `npx vitest run src/components/project/ProjectFileDropZone.test.tsx`

- [ ] **Step 3: Implement.** Drag state comes from `dragenter`/`dragleave` counting on the wrapper; only drags carrying files count, so dragging a tree node inside the app shows nothing. On success invalidate the `['schemaCache', projectId]` and `['project', projectId]` queries so the tree and Data Sources tab catch up.

- [ ] **Step 4: Run the gates**

Run: `npx tsc --noEmit && npm run lint && npx vitest run src/components/project`

- [ ] **Step 5: Commit** — `Let a file be dropped anywhere in a project`

---

### Task 10: Dropping a file into the conversation

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ChatInput.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ConversationsPage.tsx`
- Test: `exec/java-exec/src/main/resources/webapp/src/components/prospector/ChatInput.test.tsx`

**Interfaces:**
- Consumes: `uploadProjectFile` (Task 8).
- `ChatInput` gains `onFileDropped?: (file: File) => void`; without it the drop target is inert, so SQL Lab's unscoped Prospector is unaffected.

- [ ] **Step 1: Write the failing tests:**

```tsx
it('uploads a file dropped on the composer')
it('sends a message naming the file and its table path')  // the message text contains the
                                                          // schema and file name
it('ignores a drop when there is no project to upload into')
```

- [ ] **Step 2: Run them and watch them fail**

Run: `npx vitest run src/components/prospector/ChatInput.test.tsx`

- [ ] **Step 3: Implement.** After the upload, send an ordinary user message naming the file and its full table path. Prospector queries it with the `execute_sql` tool it already has — no new tool, no prompt change.

- [ ] **Step 4: Run the gates**

Run: `npx tsc --noEmit && npm run lint && npx vitest run src/components/prospector`

- [ ] **Step 5: Commit** — `Let a file be dropped into a Prospector conversation`

---

### Task 11: Remove and download from the tree

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/ContextMenu.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/SchemaExplorer.tsx` (pass the uploads schema through to the menu)
- Test: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/ContextMenu.test.tsx`

**Interfaces:**
- Consumes: `deleteProjectFile`, `downloadProjectFileUrl`, `getUploadStatus` (Task 8).

- [ ] **Step 1: Write the failing tests:**

```tsx
it('offers Delete and Download on a file in the uploads folder')
it('offers neither on a file outside it')
it('confirms before deleting, naming the file')
it('refreshes the tree after a delete')
```

- [ ] **Step 2: Run them and watch them fail**

Run: `npx vitest run src/components/schema-explorer/ContextMenu.test.tsx`

- [ ] **Step 3: Implement** — the two items appear only for `file:` nodes whose schema is the configured upload target and whose path is inside the current project's folder.

- [ ] **Step 4: Run the gates**

Run: `npx tsc --noEmit && npm run lint && npx vitest run src/components/schema-explorer`

- [ ] **Step 5: Commit** — `Add Delete and Download to uploaded files in the schema tree`

---

### Task 12: Documentation

**Files:**
- Create: `docs/dev/FileUploads.md`
- Modify: `docs/dev/DevDocs.md`, `CLAUDE.md` (doc table row)
- Modify: `docs/dev/ui/pages/projects.md`, `docs/dev/ui/pages/conversations.md`, `docs/dev/ui/components/prospector.md`

- [ ] **Step 1: Write `docs/dev/FileUploads.md`** — the upload target setting and its endpoints, the per-project folder and its `folder` `DatasetRef`, the upload/delete/download endpoints with their failure codes, impersonation behavior, the multi-node `file://` caveat, and the `fileScan` path into `ProjectSchemaCache`.

- [ ] **Step 2: Add the project role model** — `sharedWith` means editor, `viewers` means read-only, `canRead`/`canWrite` are the two rules — to `docs/dev/FileUploads.md` or to the projects doc if one exists, and link it from `DevDocs.md` and the `CLAUDE.md` table.

- [ ] **Step 3: Update the three UI docs** with the drop surfaces and the new context-menu items.

- [ ] **Step 4: Run the full gates once**

Run: `mvn checkstyle:check -pl exec/java-exec`, then from the webapp dir `npx tsc --noEmit && npm run lint && npx vitest run`
Expected: 0 violations, all tests pass.

- [ ] **Step 5: Commit** — `Document file uploads`
