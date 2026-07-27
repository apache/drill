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

/**
 * Per-project cache of the schemas/tables/columns a project's data sources expose.
 * Keyed by project id in the {@code drill.sqllab.schema_cache} PersistentStore.
 * All scanning is synchronous on the calling request thread (Drill's query
 * connection is request-scoped, so there is no background executor).
 */
public class ProjectSchemaCache {

  private static final Logger logger = LoggerFactory.getLogger(ProjectSchemaCache.class);

  private static final String STORE_NAME = "drill.sqllab.schema_cache";

  public static final long STALE_INTERVAL_MS = 5 * 60 * 1000L;
  public static final int MAX_TABLES_PER_SCHEMA = 500;

  private static final Set<String> EXCLUDED_PLUGINS = new HashSet<>(Arrays.asList(
      "cp", "sys", "information_schema"));

  private static final Set<String> NON_SCANNABLE_CONFIG_CLASSES = new HashSet<>(Arrays.asList(
      "HttpStoragePluginConfig", "GoogleSheetsStoragePluginConfig"));

  private static volatile PersistentStore<ProjectSchemaCacheEntry> cachedStore;

  private final PersistentStore<ProjectSchemaCacheEntry> store;

  public ProjectSchemaCache(PersistentStore<ProjectSchemaCacheEntry> store) {
    this.store = store;
  }

  public static ProjectSchemaCache get(PersistentStoreProvider provider, WorkManager workManager) {
    if (cachedStore == null) {
      synchronized (ProjectSchemaCache.class) {
        if (cachedStore == null) {
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
        }
      }
    }
    return new ProjectSchemaCache(cachedStore);
  }

  // ---- scannability ----

  public static boolean isExcludedPlugin(String pluginName) {
    return EXCLUDED_PLUGINS.contains(pluginName);
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

    boolean truncated = false;
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
    // Snapshot equality: skip the put when only the timestamp would change.
    if (prev != null && prev.isTruncated() == truncated && prev.getTables().equals(tables)) {
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

  private static QueryResult runQuery(String sql, WorkManager workManager,
      WebUserConnection conn) throws Exception {
    WebUserConnection fresh = new WebUserConnection(conn.webSessionResources);
    QueryWrapper wrapper = new RestQueryBuilder()
        .query(sql)
        .queryType("SQL")
        .rowLimit("10000")
        .build();
    return new RestQueryRunner(wrapper, workManager, fresh).run();
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
