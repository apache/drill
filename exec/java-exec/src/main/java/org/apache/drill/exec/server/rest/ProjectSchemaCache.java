/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.server.rest.QueryWrapper.RestQueryBuilder;
import org.apache.drill.exec.server.rest.RestQueryRunner.QueryResult;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Per-project cache of the schemas/tables/columns a project's data sources expose.
 * Keyed by project id in the {@code drill.sqllab.schema_cache} PersistentStore.
 *
 * <p>All scanning is synchronous on the calling request thread, and holds this class's
 * monitor while it runs. That is a deliberate bound on scope rather than a limit of the
 * connection: a scan runs its queries on a fresh {@link WebUserConnection} built from
 * the caller's {@link WebSessionResources}, which belong to the HTTP session and outlive
 * the request. Scans are therefore kept short rather than moved off-thread — the write
 * path updates one table at a time, and the whole-schema sweep is bounded and reached
 * only by an explicit refresh or a stale metadata read.
 */
public class ProjectSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(ProjectSchemaCache.class);

  private static final String STORE_NAME = "drill.sqllab.schema_cache";

  public static final long STALE_INTERVAL_MS = 5 * 60 * 1000L;
  public static final int MAX_TABLES_PER_SCHEMA = 500;

  /** How far {@link #fileScan} descends into a folder's subdirectories. */
  public static final int MAX_FOLDER_DEPTH = 10;

  /** Cap on the {@code SHOW FILES} queries one {@link #fileScan} may issue. */
  public static final int MAX_FOLDER_LISTINGS = 100;

  /**
   * Deadline for one query {@link #fileScan} issues. Scanning holds this class's monitor
   * on a request thread, and a query submitted through {@link RestQueryRunner} otherwise
   * waits without one, so a single stuck file would block every project's scanning. A
   * query that hits the deadline is cancelled and its result rejected, so the scan fails
   * rather than caching a part of a folder as if it were all of it.
   */
  private static final long FILE_SCAN_TIMEOUT_MS = 30_000L;

  /** The {@code queryState} of a query that ran to the end of its results. */
  private static final String COMPLETED = "COMPLETED";

  /** The {@code type} given to a cached file. */
  private static final String FILE_TABLE_TYPE = "FILE";

  /**
   * The name of a staged upload: {@link ProjectFileResources#STAGING_SUFFIX} followed by
   * a UUID, at the end of the name.
   */
  private static final Pattern STAGING_FILE = Pattern.compile(
      Pattern.quote(ProjectFileResources.STAGING_SUFFIX)
          + "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  private static final Set<String> EXCLUDED_PLUGINS = new HashSet<>(Arrays.asList(
      "cp", "sys", "information_schema"));

  private static final Set<String> NON_SCANNABLE_CONFIG_CLASSES = new HashSet<>(Arrays.asList(
      "HttpStoragePluginConfig", "GoogleSheetsStoragePluginConfig"));

  private static volatile PersistentStore<ProjectSchemaCacheEntry> cachedStore;

  // Memoized so all production callers of get(...) share one instance (and thus one
  // monitor for the synchronized methods below). Constructing a fresh instance per
  // call would give each request its own lock, defeating the synchronization.
  private static volatile ProjectSchemaCache instance;

  private final PersistentStore<ProjectSchemaCacheEntry> store;

  public ProjectSchemaCache(PersistentStore<ProjectSchemaCacheEntry> store) {
    this.store = store;
  }

  public static ProjectSchemaCache get(PersistentStoreProvider provider, WorkManager workManager) {
    if (instance == null) {
      synchronized (ProjectSchemaCache.class) {
        if (instance == null) {
          try {
            cachedStore = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    ProjectSchemaCacheEntry.class)
                    .name(STORE_NAME)
                    .build());
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access schema cache store", e);
          }
          instance = new ProjectSchemaCache(cachedStore);
        }
      }
    }
    return instance;
  }

  // ---- scannability ----

  public static boolean isExcludedPlugin(String pluginName) {
    return pluginName != null && EXCLUDED_PLUGINS.contains(pluginName.toLowerCase());
  }

  public static boolean isNonScannableConfigClass(String configClassSimpleName) {
    return NON_SCANNABLE_CONFIG_CLASSES.contains(configClassSimpleName);
  }

  /**
   * True when the schema may be bulk-scanned. False for excluded plugins and for
   * HTTP / GoogleSheets, which cannot enumerate cheaply (too many edge cases).
   */
  public static boolean isScannable(StoragePluginRegistry registry, String schemaPath) {
    if (schemaPath == null || schemaPath.isEmpty()) {
      return false;
    }
    String pluginName = schemaPath.contains(".") ? schemaPath.split("\\.", 2)[0] : schemaPath;
    if (isExcludedPlugin(pluginName)) {
      return false;
    }
    try {
      StoragePlugin plugin = registry.getPlugin(pluginName);
      if (plugin == null) {
        return false;
      }
      return !isNonScannableConfigClass(plugin.getConfig().getClass().getSimpleName());
    } catch (Exception e) {
      logger.debug("Could not determine scannability for {}: {}", schemaPath, e.getMessage());
      return false;
    }
  }

  // ---- read / scan / peek ----

  public CachedSchema read(String projectId, String schemaPath, TableScanner scanner) {
    CachedSchema existing = peek(projectId, schemaPath);
    if (existing != null
        && System.currentTimeMillis() - existing.getScannedAt() < STALE_INTERVAL_MS) {
      return existing;
    }
    return scan(projectId, schemaPath, scanner);
  }

  public CachedSchema peek(String projectId, String schemaPath) {
    ProjectSchemaCacheEntry entry = getEntry(projectId);
    if (entry == null) {
      return null;
    }
    return entry.getSchemas().get(schemaPath);
  }

  public synchronized CachedSchema scan(String projectId, String schemaPath, TableScanner scanner) {
    List<CachedTable> tables;
    try {
      tables = scanner.scanTables(schemaPath);
    } catch (Exception e) {
      throw new DrillRuntimeException("Schema scan failed for " + schemaPath + ": " + e.getMessage(), e);
    }
    if (tables == null) {
      tables = new ArrayList<>();
    }

    // A scanner that had to stop early says so by returning a PartialTables, which is
    // as much a truncation as overflowing the cap below.
    boolean truncated = tables instanceof PartialTables;
    if (tables.size() > MAX_TABLES_PER_SCHEMA) {
      tables = new ArrayList<>(tables.subList(0, MAX_TABLES_PER_SCHEMA));
      truncated = true;
    }

    CachedSchema fresh = new CachedSchema();
    fresh.setSchemaPath(schemaPath);
    fresh.setScannedAt(System.currentTimeMillis());
    fresh.setTruncated(truncated);
    fresh.setTables(tables);

    ProjectSchemaCacheEntry entry = getEntry(projectId);
    if (entry == null) {
      entry = new ProjectSchemaCacheEntry();
      entry.setProjectId(projectId);
    }
    CachedSchema prev = entry.getSchemas().get(schemaPath);
    // Snapshot equality: avoid replacing the tables list (and any downstream identity
    // churn) when the scan came back the same, but still bump scannedAt and persist it
    // so the staleness clock resets. Otherwise a stable schema would get rescanned on
    // every read past STALE_INTERVAL_MS forever.
    if (prev != null && prev.isTruncated() == truncated && prev.getTables().equals(tables)) {
      prev.setScannedAt(fresh.getScannedAt());
      store.put(projectId, entry);
      return prev;
    }
    entry.getSchemas().put(schemaPath, fresh);
    store.put(projectId, entry);
    return fresh;
  }

  public synchronized void remove(String projectId, String schemaPath) {
    ProjectSchemaCacheEntry entry = getEntry(projectId);
    if (entry == null) {
      return;
    }
    if (entry.getSchemas().remove(schemaPath) != null) {
      store.put(projectId, entry);
    }
  }

  public synchronized void removeProject(String projectId) {
    if (store.get(projectId) != null) {
      store.delete(projectId);
    }
  }

  public ProjectSchemaCacheEntry getEntry(String projectId) {
    return store.get(projectId);
  }

  /**
   * Overwrites the cached entry for a project wholesale. Used by later tasks
   * (e.g. bulk re-scan / import) that build a complete entry off the request thread.
   */
  public synchronized void putEntry(String projectId, ProjectSchemaCacheEntry entry) {
    store.put(projectId, entry);
  }

  // ---- production scanner ----

  /**
   * Runs two INFORMATION_SCHEMA queries (TABLES for type, COLUMNS for columns),
   * filtered to the schema path and its {@code .%} sub-schemas, and joins them.
   */
  public static List<CachedTable> infoSchemaScan(String schemaPath, WorkManager workManager,
      WebUserConnection conn) throws Exception {
    String esc = schemaPath.replace("'", "''");
    String where = "WHERE TABLE_SCHEMA = '" + esc + "' OR TABLE_SCHEMA LIKE '" + esc + ".%'";

    Map<String, CachedTable> byKey = new LinkedHashMap<>();

    QueryResult tablesResult = runQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.`TABLES` " + where
            + " ORDER BY TABLE_SCHEMA, TABLE_NAME",
        workManager, conn);
    for (Map<String, String> row : tablesResult.rows) {
      String schema = row.get("TABLE_SCHEMA");
      String name = row.get("TABLE_NAME");
      if (schema == null || name == null) {
        continue;
      }
      CachedTable t = new CachedTable();
      t.setSchema(schema);
      t.setName(name);
      t.setType(row.get("TABLE_TYPE") != null ? row.get("TABLE_TYPE") : "TABLE");
      t.setColumns(new ArrayList<>());
      byKey.put(schema + "." + name, t);
    }

    QueryResult colsResult = runQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.`COLUMNS` "
            + where + " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION",
        workManager, conn);
    for (Map<String, String> row : colsResult.rows) {
      String schema = row.get("TABLE_SCHEMA");
      String name = row.get("TABLE_NAME");
      if (schema == null || name == null) {
        continue;
      }
      CachedTable t = byKey.get(schema + "." + name);
      if (t == null) {
        continue;
      }
      CachedColumn c = new CachedColumn();
      c.setName(row.get("COLUMN_NAME"));
      c.setType(row.get("DATA_TYPE"));
      t.getColumns().add(c);
    }

    return new ArrayList<>(byKey.values());
  }

  /**
   * The one scanner for a schema, whichever dataset references point at it. A schema can
   * be referenced both as a plain schema and as one or more upload folders, and the cache
   * holds a single entry per schema path, so the two must be scanned together: picking
   * one scanner per schema would let whichever reference was seen first decide the
   * entry's content and drop the other's tables.
   */
  public static List<CachedTable> combinedScan(String schemaPath, List<String> folders,
      WorkManager workManager, WebUserConnection conn) throws Exception {
    List<CachedTable> tables = new ArrayList<>(infoSchemaScan(schemaPath, workManager, conn));
    boolean partial = false;
    if (folders != null) {
      for (String folder : folders) {
        if (folder != null && !folder.isEmpty()) {
          List<CachedTable> files = fileScan(schemaPath, folder, workManager, conn);
          partial |= files instanceof PartialTables;
          tables.addAll(files);
        }
      }
    }
    return partial ? new PartialTables(tables) : tables;
  }

  /**
   * Scans the files inside one folder of a file system schema, which
   * {@link #infoSchemaScan} cannot see: INFORMATION_SCHEMA lists a workspace's views
   * and nothing else. {@code SHOW FILES} enumerates the folder, descending into
   * subdirectories, and each file is then probed with {@code SELECT * ... LIMIT 1} for
   * its columns, the same shape {@code MetadataResources.getFileColumns} uses.
   *
   * <p>Each table's name is the file's path relative to {@code schemaPath}, so an upload
   * in {@code dfs.tmp} under folder {@code p1} is cached as {@code p1/sales.csv} and is
   * queryable as {@code dfs.tmp.`p1/sales.csv`}.
   *
   * <p>Nothing here looks at a file's extension: a probe that throws (an unreadable
   * file, or one no format plugin claims) yields a table with no columns rather than
   * ending the scan, so one stray document cannot cost the project its schema cache.
   * Enumeration stops one past {@link #MAX_TABLES_PER_SCHEMA}, which both bounds the
   * number of probes and leaves {@link #scan} the overflow it needs to mark the schema
   * truncated.
   */
  public static List<CachedTable> fileScan(String schemaPath, String folder,
      WorkManager workManager, WebUserConnection conn) throws Exception {
    List<String> relativePaths = new ArrayList<>();
    boolean complete = collectFiles(schemaPath, folder, relativePaths, 0,
        new int[] {MAX_FOLDER_LISTINGS}, workManager, conn);

    List<CachedTable> tables = new ArrayList<>();
    for (String relativePath : relativePaths) {
      tables.add(tables.size() < MAX_TABLES_PER_SCHEMA
          ? fileEntry(schemaPath, relativePath, workManager, conn)
          : unprobedFileEntry(schemaPath, relativePath));
    }
    return complete ? tables : new PartialTables(tables);
  }

  /**
   * The cached table for one file, with the columns its probe reports. Static and
   * unsynchronized on purpose: the probe is a query, and callers run it before taking
   * the cache's monitor through {@link #putTable}.
   */
  public static CachedTable fileEntry(String schemaPath, String relativePath,
      WorkManager workManager, WebUserConnection conn) {
    CachedTable t = unprobedFileEntry(schemaPath, relativePath);
    t.setColumns(probeColumns(schemaPath, relativePath, workManager, conn));
    return t;
  }

  private static CachedTable unprobedFileEntry(String schemaPath, String relativePath) {
    CachedTable t = new CachedTable();
    t.setSchema(schemaPath);
    t.setName(relativePath);
    t.setType(FILE_TABLE_TYPE);
    t.setColumns(new ArrayList<>());
    return t;
  }

  /**
   * Appends the paths of the files under {@code folder}, relative to {@code schemaPath},
   * recursing into subdirectories, and returns whether the whole folder was listed.
   * Three things bound the walk, because each listing is a query: it stops one past
   * {@link #MAX_TABLES_PER_SCHEMA} collected paths, past {@link #MAX_FOLDER_DEPTH}
   * levels, and once {@code listingBudget} is spent. The file cap alone bounds neither
   * depth nor breadth, since a directory holding no files never advances the count.
   *
   * <p>Each of the three is reported as an incomplete listing, so the schema is marked
   * truncated instead of a part of the folder being cached as all of it. Only the cap
   * shows up in the table count; the other two would otherwise leave no trace at all.
   */
  private static boolean collectFiles(String schemaPath, String folder, List<String> into,
      int depth, int[] listingBudget, WorkManager workManager, WebUserConnection conn)
      throws Exception {
    if (into.size() > MAX_TABLES_PER_SCHEMA || depth > MAX_FOLDER_DEPTH
        || listingBudget[0] <= 0) {
      return false;
    }
    listingBudget[0]--;
    QueryResult result = runQuery(
        "SHOW FILES IN " + quoteTablePath(schemaPath, folder), workManager, conn,
        FILE_SCAN_TIMEOUT_MS);
    boolean complete = true;
    for (Map<String, String> row : result.rows) {
      String name = row.get("name");
      if (name == null || name.isEmpty() || isSkippedFile(name)) {
        continue;
      }
      String relativePath = folder + "/" + name;
      if ("true".equalsIgnoreCase(row.get("isDirectory"))) {
        complete &= collectFiles(schemaPath, relativePath, into, depth + 1, listingBudget,
            workManager, conn);
      } else {
        into.add(relativePath);
      }
      if (into.size() > MAX_TABLES_PER_SCHEMA || listingBudget[0] <= 0) {
        return false;
      }
    }
    return complete;
  }

  /**
   * Files the scan has no reason to spend a probe on: the staging file a failed upload
   * can leave behind, and the hidden files a file system keeps beside a write (such as
   * the local {@code .crc} sidecars), which Drill itself ignores when reading a folder.
   *
   * <p>The staging rule matches only what a staged write produces, the suffix followed
   * by a UUID at the end of the name. Nothing stops a client uploading a file called
   * {@code notes.upload-draft.csv}, and that is a real file the user wants to query.
   */
  private static boolean isSkippedFile(String name) {
    return name.startsWith(".") || STAGING_FILE.matcher(name).find();
  }

  /** The columns {@code SELECT * ... LIMIT 1} reports, or none when the probe fails. */
  private static List<CachedColumn> probeColumns(String schemaPath, String relativePath,
      WorkManager workManager, WebUserConnection conn) {
    List<CachedColumn> columns = new ArrayList<>();
    try {
      QueryResult result = runQuery(
          "SELECT * FROM " + quoteTablePath(schemaPath, relativePath) + " LIMIT 1",
          workManager, conn, FILE_SCAN_TIMEOUT_MS);
      List<String> names = new ArrayList<>(result.columns);
      for (int i = 0; i < names.size(); i++) {
        CachedColumn c = new CachedColumn();
        c.setName(names.get(i));
        c.setType(result.metadata != null && i < result.metadata.size()
            ? result.metadata.get(i) : "ANY");
        columns.add(c);
      }
    } catch (Exception e) {
      // Drill decides at query time what it can read. A file it cannot is cached
      // without columns, and the scan carries on to the next one.
      logger.debug("Column probe failed for {}.`{}`: {}", schemaPath, relativePath,
          e.getMessage());
    }
    return columns;
  }

  /**
   * {@code dfs.tmp} + {@code p1/sales.csv} to {@code dfs.`tmp`.`p1/sales.csv`}: the
   * plugin name stays unquoted, each workspace part and the relative path are
   * backtick-quoted.
   */
  private static String quoteTablePath(String schemaPath, String relativePath) {
    String[] parts = schemaPath.split("\\.");
    StringBuilder sb = new StringBuilder(parts[0]);
    for (int i = 1; i < parts.length; i++) {
      sb.append(".`").append(parts[i].replace("`", "``")).append("`");
    }
    return sb.append(".`").append(relativePath.replace("`", "``")).append("`").toString();
  }

  private static QueryResult runQuery(String sql, WorkManager workManager,
      WebUserConnection conn) throws Exception {
    return runQuery(sql, workManager, conn, 0L);
  }

  /**
   * Runs one scanning query. A positive {@code timeoutMs} gives it a deadline, enforced
   * by cancelling it through its own runner from a watchdog on the Drillbit's shared
   * executor. The watchdog holds its thread only for as long as the query runs.
   *
   * <p>A cancelled query does not fail: Drill delivers its partial results with
   * {@code queryState} {@code CANCELED} and no error, which the runner returns like any
   * other result. Caching that would be worse than failing — a truncated
   * {@code SHOW FILES} reads as a complete listing, so files would silently vanish from
   * the cache with {@code truncated} false and the staleness clock reset. So when a
   * deadline is set, anything but a completed query is turned into a failure.
   */
  private static QueryResult runQuery(String sql, WorkManager workManager,
      WebUserConnection conn, long timeoutMs) throws Exception {
    WebUserConnection fresh = new WebUserConnection(conn.webSessionResources);
    QueryWrapper wrapper = new RestQueryBuilder()
        .query(sql)
        .queryType("SQL")
        .rowLimit("10000")
        .build();
    RestQueryRunner runner = new RestQueryRunner(wrapper, workManager, fresh);
    if (timeoutMs <= 0) {
      return runner.run();
    }
    CountDownLatch finished = new CountDownLatch(1);
    workManager.getContext().getExecutor().execute(() -> {
      try {
        if (!finished.await(timeoutMs, TimeUnit.MILLISECONDS)) {
          logger.warn("Cancelling a schema scan query that exceeded {} ms: {}",
              timeoutMs, sql);
          runner.cancel();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    QueryResult result;
    try {
      result = runner.run();
    } finally {
      finished.countDown();
    }
    if (!COMPLETED.equals(result.queryState)) {
      throw new DrillRuntimeException("Schema scan query did not complete (state "
          + result.queryState + "): " + sql);
    }
    return result;
  }

  // ---- incremental updates ----

  /**
   * Merges one table into a schema's cached tables, replacing any table of the same
   * name. Used by the upload path, which knows exactly which file changed and so has no
   * reason to rescan the folder: a whole-folder scan runs one query per file, and on an
   * upload request that would mean the user waits for the folder while every other
   * project's scanning waits for this class's monitor.
   *
   * <p>{@code scannedAt} is deliberately not bumped. A merge adds one file's worth of
   * knowledge; it is not the full scan that {@link #read} measures staleness against, and
   * a schema this project has never scanned stays stale so the next read still scans it.
   */
  public synchronized void putTable(String projectId, String schemaPath, CachedTable table) {
    if (table == null || table.getName() == null) {
      return;
    }
    ProjectSchemaCacheEntry entry = getEntry(projectId);
    if (entry == null) {
      entry = new ProjectSchemaCacheEntry();
      entry.setProjectId(projectId);
    }
    CachedSchema schema = entry.getSchemas().get(schemaPath);
    if (schema == null) {
      schema = new CachedSchema();
      schema.setSchemaPath(schemaPath);
      entry.getSchemas().put(schemaPath, schema);
    }
    List<CachedTable> tables = schema.getTables();
    for (int i = 0; i < tables.size(); i++) {
      if (table.getName().equals(tables.get(i).getName())) {
        tables.set(i, table);
        store.put(projectId, entry);
        return;
      }
    }
    if (tables.size() >= MAX_TABLES_PER_SCHEMA) {
      schema.setTruncated(true);
    } else {
      tables.add(table);
    }
    store.put(projectId, entry);
  }

  /** Drops one table from a schema's cached tables. The counterpart of {@link #putTable}. */
  public synchronized void removeTable(String projectId, String schemaPath, String tableName) {
    ProjectSchemaCacheEntry entry = getEntry(projectId);
    if (entry == null || tableName == null) {
      return;
    }
    CachedSchema schema = entry.getSchemas().get(schemaPath);
    if (schema == null) {
      return;
    }
    if (schema.getTables().removeIf(t -> tableName.equals(t.getName()))) {
      store.put(projectId, entry);
    }
  }

  /**
   * The tables a scanner found when a bound stopped it before the end of its source.
   * A list subtype, so {@link TableScanner} keeps returning one value and {@link #scan}
   * can mark a schema truncated for a reason the table count cannot show.
   */
  public static class PartialTables extends ArrayList<CachedTable> {
    private static final long serialVersionUID = 1L;

    public PartialTables(List<CachedTable> tables) {
      super(tables);
    }
  }

  @FunctionalInterface
  public interface TableScanner {
    List<CachedTable> scanTables(String schemaPath) throws Exception;
  }

  // ---- POJOs ----

  public static class ProjectSchemaCacheEntry {
    @JsonProperty private String projectId;
    @JsonProperty private Map<String, CachedSchema> schemas = new LinkedHashMap<>();

    public String getProjectId() {
      return projectId;
    }

    public void setProjectId(String projectId) {
      this.projectId = projectId;
    }

    public Map<String, CachedSchema> getSchemas() {
      if (schemas == null) {
        schemas = new LinkedHashMap<>();
      }
      return schemas;
    }

    public void setSchemas(Map<String, CachedSchema> schemas) {
      this.schemas = schemas;
    }
  }

  public static class CachedSchema {
    @JsonProperty private String schemaPath;
    @JsonProperty private long scannedAt;
    @JsonProperty private boolean truncated;
    @JsonProperty private List<CachedTable> tables = new ArrayList<>();

    public String getSchemaPath() {
      return schemaPath;
    }

    public void setSchemaPath(String schemaPath) {
      this.schemaPath = schemaPath;
    }

    public long getScannedAt() {
      return scannedAt;
    }

    public void setScannedAt(long scannedAt) {
      this.scannedAt = scannedAt;
    }

    public boolean isTruncated() {
      return truncated;
    }

    public void setTruncated(boolean truncated) {
      this.truncated = truncated;
    }

    public List<CachedTable> getTables() {
      if (tables == null) {
        tables = new ArrayList<>();
      }
      return tables;
    }

    public void setTables(List<CachedTable> tables) {
      this.tables = tables;
    }
  }

  public static class CachedTable {
    @JsonProperty private String schema;
    @JsonProperty private String name;
    @JsonProperty private String type;
    @JsonProperty private List<CachedColumn> columns = new ArrayList<>();

    public String getSchema() {
      return schema;
    }

    public void setSchema(String schema) {
      this.schema = schema;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public List<CachedColumn> getColumns() {
      if (columns == null) {
        columns = new ArrayList<>();
      }
      return columns;
    }

    public void setColumns(List<CachedColumn> columns) {
      this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CachedTable)) {
        return false;
      }
      CachedTable that = (CachedTable) o;
      return Objects.equals(schema, that.schema)
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type)
          && Objects.equals(getColumns(), that.getColumns());
    }

    @Override
    public int hashCode() {
      return Objects.hash(schema, name, type, getColumns());
    }
  }

  public static class CachedColumn {
    @JsonProperty private String name;
    @JsonProperty private String type;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CachedColumn)) {
        return false;
      }
      CachedColumn that = (CachedColumn) o;
      return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type);
    }
  }
}
