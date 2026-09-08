Schema Explorer Improvements

 Overview

 Enhance the SchemaExplorer tree with brand logos, drag-to-query, context menus, copy names, favorites, column statistics, better caching, and incremental loading.

 ---
 Phase 1: Brand Logos & File-Type Icons

 Goal

 Replace generic Ant Design icons with actual brand logos (Splunk, MongoDB, Kafka, etc.) and file-type icons (PDF, CSV, Parquet, etc.).

 Approach

 Install react-icons which includes the simple-icons (Si*) set with 2500+ brand logos.

 Steps

 1. Install react-icons in exec/java-exec/src/main/resources/webapp/
 npm install react-icons
 2. Create src/components/schema-explorer/icons.tsx — centralized icon mapping
   - Plugin icons (brand logos from react-icons/si):
       - Splunk: SiSplunk
     - MongoDB: SiMongodb
     - Elasticsearch: SiElasticsearch
     - Kafka: SiApachekafka
     - S3/MinIO: SiAmazons3
     - Google Cloud: SiGooglecloud
     - Azure: SiMicrosoftazure
     - Hive: SiApachehive
     - HBase/Hadoop: SiApachehadoop
     - MySQL/PostgreSQL/Oracle: SiMysql / SiPostgresql / SiOracle
   - File-type icons (from react-icons/vsc or react-icons/si):
       - PDF: custom or VscFilePdf
     - CSV: VscFileCode with label
     - JSON: SiJson or VscJson
     - Parquet/Avro/ORC: custom colored file icons
   - Export getPluginIcon(type, name) and getFileIcon(filename) functions
   - Keep Ant Design FolderOutlined for folders, DatabaseOutlined as fallback
 3. Update SchemaExplorer.tsx
   - Import from new icons.tsx instead of inline icon functions
   - Remove getPluginIcon() and move to icons.tsx
   - Add getFileIcon(filename) calls for file nodes based on extension
   - Keep existing column type icons (FieldNumberOutlined, etc.) as-is

 Files
 ┌───────────────────────────────────────────────────┬──────────────────────────────┐
 │                       File                        │            Action            │
 ├───────────────────────────────────────────────────┼──────────────────────────────┤
 │ package.json                                      │ Add react-icons dependency   │
 ├───────────────────────────────────────────────────┼──────────────────────────────┤
 │ src/components/schema-explorer/icons.tsx          │ New — icon mapping functions │
 ├───────────────────────────────────────────────────┼──────────────────────────────┤
 │ src/components/schema-explorer/SchemaExplorer.tsx │ Import from icons.tsx        │
 └───────────────────────────────────────────────────┴──────────────────────────────┘
 ---
 Phase 2: Refactor & React Query Caching

 Goal

 Break the 580-line SchemaExplorer into subcomponents and replace manual cache state with React Query for automatic caching, stale-while-revalidate, and background refetching.

 Steps

 1. Create src/components/schema-explorer/hooks.ts — custom hooks
   - usePlugins() — wraps useQuery(['plugins'], getPlugins) (already exists, extract)
   - useSchemas(pluginName) — useQuery(['schemas', pluginName], () => getPluginSchemas(pluginName), { enabled: !!pluginName })
   - useTables(schemaName) — similar pattern
   - useColumns(schemaName, tableName) — similar pattern
   - useFiles(schemaName, subPath?) — similar pattern
   - useFileColumns(schemaName, filePath) — similar pattern
   - All queries use staleTime: 5 * 60 * 1000 (5 min) and cacheTime: 30 * 60 * 1000 (30 min)
 2. Create src/components/schema-explorer/TreeNodeBuilder.tsx — tree node construction
   - PluginNode component — renders a plugin with its schemas
   - SchemaNode component — renders schema with tables or files
   - TableNode / FileNode / ColumnNode components
   - Each uses its own React Query hook to fetch data on expand
   - This replaces the monolithic useMemo + loadData pattern
 3. Refactor SchemaExplorer.tsx
   - Remove all *Cache state variables (5 useState calls)
   - Remove loadData callback (100+ lines)
   - Remove monolithic treeData useMemo (115 lines)
   - Use the new subcomponents/hooks instead
   - Keep: search, refresh, expand/collapse state, selection handlers
 4. Refresh behavior
   - handleRefresh calls queryClient.invalidateQueries() for all schema-related queries
   - Individual node refresh via context menu (Phase 4)

 Files
 ┌────────────────────────────────────────────────────┬───────────────────────────────────────┐
 │                        File                        │                Action                 │
 ├────────────────────────────────────────────────────┼───────────────────────────────────────┤
 │ src/components/schema-explorer/hooks.ts            │ New — React Query hooks               │
 ├────────────────────────────────────────────────────┼───────────────────────────────────────┤
 │ src/components/schema-explorer/TreeNodeBuilder.tsx │ New — tree node components            │
 ├────────────────────────────────────────────────────┼───────────────────────────────────────┤
 │ src/components/schema-explorer/SchemaExplorer.tsx  │ Refactor to use hooks + subcomponents │
 └────────────────────────────────────────────────────┴───────────────────────────────────────┘
 ---
 Phase 3: Drag-to-Query & Copy Names

 Goal

 Allow users to drag schema/table/column names from the tree directly into the Monaco editor, and right-click to copy qualified names.

 Steps

 1. Make tree nodes draggable in SchemaExplorer.tsx
   - Set draggable prop on Ant Design <Tree>
   - In onDragStart, set dataTransfer.setData('text/plain', qualifiedName) with the backtick-quoted identifier
   - Qualified name logic (same as existing double-click):
       - Plugin: `pluginName`
     - Schema: `schemaName`
     - Table: `schema`.`table`
     - File: `schema`.`filePath`
     - Column: `columnName`
 2. Add drop handler to SqlEditor.tsx (src/components/query-editor/SqlEditor.tsx)
   - Add onDrop and onDragOver event handlers to the editor container
   - onDragOver: e.preventDefault() to allow drop
   - onDrop:
       - Get text from e.dataTransfer.getData('text/plain')
     - Use Monaco's getTargetAtClientPoint(e.clientX, e.clientY) to find cursor position
     - Insert text at that position using editor.executeEdits()
   - Visual feedback: highlight the editor border on dragover
 3. Copy to clipboard
   - Add onRightClick handler to <Tree> for right-click → copy
   - Uses navigator.clipboard.writeText(qualifiedName)
   - Show message.success('Copied!') notification
   - This is a simple addition before the full context menu in Phase 4

 Files
 ┌───────────────────────────────────────────────────┬──────────────────────────────────────────────┐
 │                       File                        │                    Action                    │
 ├───────────────────────────────────────────────────┼──────────────────────────────────────────────┤
 │ src/components/schema-explorer/SchemaExplorer.tsx │ Add draggable, onDragStart, right-click copy │
 ├───────────────────────────────────────────────────┼──────────────────────────────────────────────┤
 │ src/components/query-editor/SqlEditor.tsx         │ Add drop handler with getTargetAtClientPoint │
 └───────────────────────────────────────────────────┴──────────────────────────────────────────────┘
 ---
 Phase 4: Context Menus

 Goal

 Right-click context menus on tree nodes with actions relevant to the node type.

 Steps

 1. Create src/components/schema-explorer/ContextMenu.tsx
   - Uses Ant Design <Dropdown> with trigger={['contextMenu']}
   - Menu items vary by node type:
       - Plugin: Copy name, Refresh schemas, Collapse all
     - Schema: Copy name, Generate USE schema, Refresh tables
     - Table: Copy qualified name, Generate SELECT *, Generate DESCRIBE, Preview data, Refresh columns
     - File: Copy path, Generate SELECT *, Copy qualified reference
     - Column: Copy name, Copy column_name (backtick-quoted)
   - Actions use onInsertText callback for SQL generation
   - Copy actions use navigator.clipboard.writeText()
 2. Integrate in SchemaExplorer
   - Wrap tree nodes with ContextMenu component
   - Pass node type and metadata to determine menu items
   - Replace the Phase 3 simple right-click copy with this richer menu

 Files
 ┌───────────────────────────────────────────────────┬──────────────────────────────┐
 │                       File                        │            Action            │
 ├───────────────────────────────────────────────────┼──────────────────────────────┤
 │ src/components/schema-explorer/ContextMenu.tsx    │ New — context menu component │
 ├───────────────────────────────────────────────────┼──────────────────────────────┤
 │ src/components/schema-explorer/SchemaExplorer.tsx │ Integrate context menu       │
 └───────────────────────────────────────────────────┴──────────────────────────────┘
 ---
 Phase 5: Favorites/Pinning & Column Statistics

 Goal

 Pin frequently used tables/schemas to the top, and show column statistics on demand.

 Steps

 1. Favorites with localStorage
   - Create src/components/schema-explorer/useFavorites.ts hook
   - localStorage key: drill-sqllab-favorites
   - Stores array of node keys (e.g., ["table:dfs.tmp:myfile.csv", "schema:cp"])
   - API: { favorites, toggleFavorite, isFavorite }
   - In SchemaExplorer: add star icon toggle on hover for tables/schemas
   - Favorites section at top of tree (separate from main hierarchy)
   - Favorites render as flat list with qualified names
 2. Column statistics (on-demand)
   - Add "Show Statistics" to table context menu (Phase 4)
   - Executes: SELECT COUNT(*), COUNT(DISTINCT col), MIN(col), MAX(col) FROM schema.table
   - Uses existing query execution API endpoint (/query)
   - Shows results in a popover or small drawer
   - Only for numeric/date/string columns (skip binary)
   - New component: src/components/schema-explorer/ColumnStats.tsx

 Files
 ┌───────────────────────────────────────────────────┬───────────────────────────────────┐
 │                       File                        │              Action               │
 ├───────────────────────────────────────────────────┼───────────────────────────────────┤
 │ src/components/schema-explorer/useFavorites.ts    │ New — favorites hook              │
 ├───────────────────────────────────────────────────┼───────────────────────────────────┤
 │ src/components/schema-explorer/ColumnStats.tsx    │ New — statistics display          │
 ├───────────────────────────────────────────────────┼───────────────────────────────────┤
 │ src/components/schema-explorer/SchemaExplorer.tsx │ Favorites section + stats trigger │
 ├───────────────────────────────────────────────────┼───────────────────────────────────┤
 │ src/components/schema-explorer/ContextMenu.tsx    │ Add "Show Statistics" item        │
 └───────────────────────────────────────────────────┴───────────────────────────────────┘
 ---
 Files Summary (All Phases)
 ┌─────┬────────────────────────────────────────────────────┬───────┬────────────────────────────────┐
 │  #  │                        File                        │ Phase │             Action             │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 1   │ package.json                                       │ 1     │ Add react-icons                │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 2   │ src/components/schema-explorer/icons.tsx           │ 1     │ New — brand logos + file icons │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 3   │ src/components/schema-explorer/hooks.ts            │ 2     │ New — React Query hooks        │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 4   │ src/components/schema-explorer/TreeNodeBuilder.tsx │ 2     │ New — tree node components     │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 5   │ src/components/schema-explorer/SchemaExplorer.tsx  │ 1-5   │ Refactor throughout            │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 6   │ src/components/query-editor/SqlEditor.tsx          │ 3     │ Add drop handler               │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 7   │ src/components/schema-explorer/ContextMenu.tsx     │ 4     │ New — context menus            │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 8   │ src/components/schema-explorer/useFavorites.ts     │ 5     │ New — favorites hook           │
 ├─────┼────────────────────────────────────────────────────┼───────┼────────────────────────────────┤
 │ 9   │ src/components/schema-explorer/ColumnStats.tsx     │ 5     │ New — column statistics        │
 └─────┴────────────────────────────────────────────────────┴───────┴────────────────────────────────┘
 ---
 Verification

 After each phase:
 1. cd exec/java-exec/src/main/resources/webapp && npm run build — TypeScript compiles
 2. npm run lint — no lint errors
 3. npm test — existing tests pass

 Functional checks:
 - Phase 1: Correct brand logos appear for Splunk, MongoDB, Kafka, S3, GCS, Azure, JDBC databases. File icons show PDF/CSV/JSON/Parquet appropriately. Folders still use folder icon.
 - Phase 2: Tree still loads lazily on expand. Data is cached for 5 minutes. Refresh button clears all caches. No redundant API calls on re-expand.
 - Phase 3: Drag a table/column from tree into Monaco editor — text appears at drop position. Right-click → copy name works.
 - Phase 4: Right-click menu shows appropriate actions per node type. "Generate SELECT *" inserts query. "Preview data" works.
 - Phase 5: Star icon appears on hover. Starred items appear at top. Column statistics display correctly for tables.
