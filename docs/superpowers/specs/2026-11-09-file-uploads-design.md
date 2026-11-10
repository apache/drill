# File Uploads — Design

**Date:** 2026-11-09
**Status:** approved for planning

## Intent

Someone has a spreadsheet and wants to analyze it. Today that means getting the file
onto a machine Drill can read, knowing which workspace points at that directory, and
writing a path. That is three things a person who has never typed `CREATE TABLE`
cannot do.

This feature is one thing: drag the file into a project and it is queryable.

## The idea that shapes everything else

An uploaded file is not a new kind of object. It is a file in a workspace, read by
whichever format plugin claims its extension, exactly like every other file Drill
queries.

So the upload target is an ordinary writable storage plugin workspace, each project
gets a folder under it, and that folder is registered as one of the project's
datasets. From there the existing UI does the work: the folder appears in the Data
Sources tab, in the schema explorer tree, in SQL autocomplete, in the schema cache,
and in Prospector's context, because all of those already understand datasets.

What gets built is therefore small: a setting naming the workspace, an endpoint that
writes bytes into it, drop surfaces that call the endpoint, and two context-menu items
for removing and downloading a file. There is no uploads list, no file metadata store,
no per-file settings, no Files tab. The schema explorer is the uploads list.

## Goals

- Drag a file into a project and query it, with no SQL and no configuration by the
  person doing it.
- Work for every format Drill reads — CSV, TSV, JSON, Parquet, Excel, PCAP, logs.
  Nothing in the upload path is format-aware.
- Work against local disk and against S3 with the same code, because the deployment
  this is modeled on (DataDistillr) used a designated S3 bucket for uploads.
- Uploaded files are visible to the project, so a shared project shares its files.

## Non-goals

- No parse-time knobs at upload: no sheet picker, no header-row field, no delimiter
  override. A three-sheet workbook is one file whose default sheet is the first;
  reaching the others is Drill's `table()` function, the same as any other Excel file
  in Drill today.
- No preview or confirmation step. The file lands; what it looks like is a question
  the schema explorer and SQL Lab already answer.
- No per-project choice of upload target (see Constraints).
- No versioning, no undo of an overwrite, no quota accounting per user.

## Anti-goals

Things that would make this a failure even if it worked:

- A second place in the UI where data lives. If an uploaded file is listed anywhere
  that other tables are not, the feature has gone wrong.
- Uploads landing somewhere nobody backs up because each project named its own target.
- An upload that silently succeeds into a folder no project can see.

## Constraints

**The upload target is cluster-wide and admin-set.** One setting names a storage
plugin and one of its workspaces. Reasons, in the user's words: choosing a workspace
means reading plugin configs, which hold credentials, so it is an admin-only view
already — a project creator who is not an admin could not make the choice
meaningfully. And per-project targets mean projects whose uploads land in someone's
unbacked-up scratch directory. A per-project override can be added later without
changing the upload path.

**If no target is configured, the drop surface says so** and tells the user to ask an
admin to set the uploads folder. It does not fail silently and does not fall back to a
default directory.

**The workspace must be writable.** An upload to a non-writable workspace is refused,
as `NotebookResources` already refuses one.

**Uploading requires project edit rights**, and those rights do not exist yet. There
is no per-project access check anywhere in `ProjectResources` today: the class is
annotated `@RolesAllowed(AUTHENTICATED_ROLE)` and every mutation, `addDataset` and
`removeDataset` included, is open to any authenticated user. `sharedWith` is a flat
list of names with no viewer/editor distinction.

The user's decision: add project access control, starting with very basic controls.
So this feature carries a small permission model, and upload is its first consumer:

- The owner may do anything with their project.
- `sharedWith` entries gain an optional role, `viewer` or `editor`. A bare name
  string, which is what every stored project holds today, reads as `editor`, so
  nothing that works now stops working.
- A project with `isPublic` is viewable by any authenticated user, and writable by
  no one but its owner and editors.
- One helper decides it (`canView` / `canEdit`), and it is applied to the project
  mutations, not to upload alone. Guarding upload while `removeDataset` stays open
  would be security theater.

Anything beyond this -- groups, per-artifact permissions, an admin override -- is out
of scope.

## The parts the user decided

- The admin setting is a pointer at an existing plugin + workspace, not a new kind of
  configuration. Point it at `dfs` for a laptop, at an `s3` workspace for the
  DataDistillr model, and the upload code never knows which.
- Per-project folder under that workspace.
- The folder is exposed to the project as an ordinary data source, not as new UI.
- Three drop surfaces, all wanted: a Files-free project-wide drag overlay, a drop into
  the Prospector conversation, and (see below) the upload affordance the Data Sources
  tab needs so there is a discoverable button and not only a gesture.
- Removing a file is a right-click in the schema explorer tree.

## The parts left to the builder

Everything not named above, plus these, which are my calls and should be checked while
reading:

- **Writing the bytes.** Use the Hadoop `FileSystem` API via
  `FileSystemPlugin.getFsConf()` and the plugin's `connection`, not `java.nio.file` as
  `NotebookResources.java:353` does. The nio path only works for `file://` connections,
  and S3 is a stated requirement. Stream from the multipart part to the output stream;
  do not buffer the file in memory. Jersey multipart is already registered
  (`DrillRestServer.java:132`).
- **Where the setting lives.** A `PersistentStore`, as `AiConfigResources` does for
  `drill.sqllab.ai_config`. Holds plugin name, workspace name, and a max upload size.
- **The folder name is the project id**, not the project name: ids do not change when
  a project is renamed, and they contain no characters that need escaping.
- **Registering the dataset** happens on the first successful upload into a project,
  not at project creation, so a cluster with no upload target configured grows no empty
  folders.
- **Name collisions** are refused with 409 unless the request passes an overwrite flag;
  the drop surface asks the user which they meant.
- **Filename safety.** Reject any name containing a path separator or `..`, and
  normalize-and-verify the resolved path still sits inside the project folder, the way
  `NotebookResources.java:367` does. Accept any extension: Drill decides what it can
  read, and an unreadable file is a query error, not an upload error.
- **Size limit** comes from the setting, enforced while streaming, with a clear error
  when exceeded.
- **The schema cache must learn to see files.** `ProjectSchemaCache.infoSchemaScan`
  is built on `INFORMATION_SCHEMA.TABLES`/`COLUMNS`, and Drill does not list the files
  in a filesystem workspace there -- only views. Left as is, an uploaded file appears
  in the schema tree (which goes through `SHOW FILES`) but never in autocomplete or in
  the context block Prospector reads, so the headline story -- upload a spreadsheet,
  ask a question about it -- does not work. The user chose to build this rather than
  defer it: enumerate the project's upload folder, probe each file's columns, and feed
  the result into the same cache the rest of the UI already reads. A file Drill cannot
  parse fails its probe and is cached as a file with no columns, not as a failed scan.
  Refresh after an upload, an overwrite and a delete.
- **The Prospector drop** uploads through the same endpoint and then puts a message in
  the conversation naming the file and its new table path, so Prospector can query it
  with the `execute_sql` tool it already has. No new Prospector tool.
- **The context menu** (`components/schema-explorer/ContextMenu.tsx`) gains Delete and
  Download, shown only for file nodes that sit under the configured uploads workspace.
  Delete confirms, naming the file.

## Calls I made while writing this

These came out of a builder's read of the draft. They are mine, not the user's, and
are the places to push back while reading.

- **The dataset reference is a new `folder` type.** `DatasetRef` understands
  `plugin`, `schema` and `table`, and none fits a per-project directory inside a
  shared workspace: `schema: dfs.uploads` exposes every project's folder to every
  project, and the `table` form is filtered out by `filterTreeNodes` until its
  children load. A `folder` ref carrying schema and path, with matching cases in
  `datasetAllowList.ts` and `SchemaExplorer.tsx`, is the honest shape. Cross-project
  visibility is a stated failure, so this is not optional.
- **The uploads workspace is exempt from the extension filter.**
  `MetadataResources.getFiles` lists only files whose extension some format config
  claims, so a `.docx` upload would be invisible and unreachable by the new Delete.
  The listing for the uploads workspace shows everything; an unreadable file is a
  query error, as the non-goals say, but it is never a file the user cannot see or
  remove.
- **Writes use the querying user's identity when impersonation is on**, through
  `ImpersonationUtil`, and the process user otherwise. Writing as the Drillbit user on
  a cluster with `drill.exec.impersonation` enabled produces files the uploader cannot
  then read, and that is not fixable afterwards without changing ownership on disk.
- **A local filesystem target on a multi-Drillbit cluster gets a warning, not a
  refusal.** The upload lands on whichever node served the request, so queries that
  plan onto other nodes see nothing. The settings UI says so when the connection is
  `file://` and more than one Drillbit is registered. An admin who knows their
  deployment may proceed.
- **Whether uploads are configured is readable by any authenticated user**, through a
  small endpoint returning `{configured, plugin, workspace, maxSizeBytes}`. The drop
  surface and the context menu both need it, and a plugin and workspace name are not
  credentials.
- **Deleting a project leaves its uploaded files in place.** Project delete is a soft
  delete; destroying the bytes would make it unrestorable. Orphaned folders are an
  admin cleanup problem, out of scope here.
- **Download streams through the Drillbit** rather than redirecting to a presigned
  URL, so one code path serves local disk and S3.
- **An overwrite invalidates that file's cache entry**, since its columns may have
  changed.

## What success looks like

A user with an `.xlsx` and no SQL drags it onto their project and, within a few
seconds, sees it in the schema tree and can ask Prospector a question about it. An
admin sets one setting, once, and never thinks about it again.
