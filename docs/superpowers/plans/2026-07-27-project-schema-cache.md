# Project Schema Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cache the schemas/tables/columns of a project's data sources so Prospector, the schema tree, and SQL autocomplete read pre-scanned metadata instead of querying `INFORMATION_SCHEMA` on demand.

**Architecture:** A per-project `PersistentStore` (`drill.sqllab.schema_cache`) keyed by project id holds each added schema's tables + columns. A new `ProjectSchemaCache` helper scans a schema with two `INFORMATION_SCHEMA` queries, all synchronously on the request thread (Drill's query connection is request-scoped — no background executor). `ProjectResources` scans on dataset-add and exposes refresh/inspect endpoints; `MetadataResources.getTables`/`getColumns` become read-through (serve from cache when fresh, rescan inline when stale) behind a `canRead` gate; `ProspectorResources` peeks the cache (never scanning) to build the system prompt.

**Tech Stack:** Java 8 (exec/java-exec), JAX-RS (Jersey) REST resources, Drill `PersistentStore`, `RestQueryRunner`; React + TypeScript + Ant Design + axios + react-query (webapp); JUnit 5 (`org.junit.jupiter`) unit tests with `InMemoryStore`, JUnit 4 `ClusterTest` HTTP integration tests; Vitest for webapp.

## Global Constraints

- **Design spec:** `docs/superpowers/specs/2026-07-27-project-schema-cache-design.md` — read it first; it is the source of truth for behavior.
- **Checkstyle:** after modifying anything in `exec/java-exec`, run `mvn checkstyle:check -pl exec/java-exec`. All `if`/`else`/`for`/`while` bodies MUST use braces `{}`. No unused imports.
- **License headers:** every new file (Java, TS, TSX) MUST start with the Apache 2.0 license header. Copy the header verbatim from an existing sibling file in the same directory.
- **Git:** do NOT add Claude as a co-author. Commit messages imperative ("Add …", not "Added …").
- **Skip these plugins from scanning:** any schema whose storage-plugin config class simple-name is `HttpStoragePluginConfig` or `GoogleSheetsStoragePluginConfig`, plus the excluded plugins `cp`, `sys`, `information_schema`. Never scan them; they fall through to today's live behavior.
- **No new Maven/npm dependencies.**
- `jdbc-all` size limit is NOT affected (no `webapp/**` resources added) — no `exec/jdbc-all/pom.xml` change needed.

---

## File Structure

**Backend (create):**
- `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectSchemaCache.java` — store + POJOs (`ProjectSchemaCacheEntry`, `CachedSchema`, `CachedTable`, `CachedColumn`) + logic (`scan`/`read`/`peek`/`remove`/`removeProject`, static `isScannable` + pure helpers, `TableScanner` seam, `infoSchemaScan`).
- `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCache.java` — unit tests (JUnit 5 + `InMemoryStore` + fake `TableScanner`).

**Backend (modify):**
- `ProjectResources.java` — inject `WebUserConnection`; refactor `getStore()` to delegate to a package-private static `openStore(...)`; scan on `addDataset`; cache-remove on `removeDataset`/`deleteProject`/`purgeProject`/`cleanupPluginDatasets`; add `POST /{id}/schema-cache/refresh` and `GET /{id}/schema-cache`.
- `MetadataResources.java` — `getTables`/`getColumns` gain optional `?projectId=` + `@Context SecurityContext`; read-through cache path.
- `ProspectorResources.java` — peek cache per dataset schema into the system prompt; soften the live-discovery instruction.

**Frontend (modify):**
- `api/metadata.ts` — `getTables`/`getColumns` accept optional `projectId`.
- `api/projects.ts` — `refreshSchemaCache` + `getSchemaCache` client fns + types.
- `types/index.ts` — `SchemaCacheResponse` type.
- `components/schema-explorer/SchemaExplorer.tsx` — pass `projectId` to `getTables`/`getColumns`.
- `hooks/useMonacoCompletion.ts` — accept + forward `projectId`.
- `pages/SqlLabPage.tsx` — pass `projectId` to `useMonacoCompletion`.
- `pages/ProjectDataSourcesPage.tsx` — refresh button + "last scanned" indicator.

**Docs (modify):** `docs/dev/PROSPECTOR.md` — note the cache; `docs/dev/ui/pages/` project-data-sources doc if present.

---

## Task 1: `ProjectSchemaCache` core (store + logic)

**Files:**
- Create: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectSchemaCache.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCache.java`

**Interfaces:**
- Produces (used by Tasks 2–4):
  - `class ProjectSchemaCache` with:
    - `ProjectSchemaCache(PersistentStore<ProjectSchemaCacheEntry> store)` — direct/test ctor.
    - `static ProjectSchemaCache get(PersistentStoreProvider provider, WorkManager wm)` — production singleton.
    - `static boolean isScannable(StoragePluginRegistry registry, String schemaPath)`
    - `static boolean isExcludedPlugin(String pluginName)`
    - `static boolean isNonScannableConfigClass(String configClassSimpleName)`
    - `CachedSchema scan(String projectId, String schemaPath, TableScanner scanner)`
    - `CachedSchema read(String projectId, String schemaPath, TableScanner scanner)`
    - `CachedSchema peek(String projectId, String schemaPath)`
    - `void remove(String projectId, String schemaPath)`
    - `void removeProject(String projectId)`
    - `ProjectSchemaCacheEntry getEntry(String projectId)`
    - `static List<CachedTable> infoSchemaScan(String schemaPath, WorkManager wm, WebUserConnection conn) throws Exception`
    - `static final long STALE_INTERVAL_MS`, `static final int MAX_TABLES_PER_SCHEMA`
  - `@FunctionalInterface interface TableScanner { List<CachedTable> scanTables(String schemaPath) throws Exception; }`
  - Nested static POJOs: `ProjectSchemaCacheEntry {String projectId; Map<String,CachedSchema> schemas;}`, `CachedSchema {String schemaPath; long scannedAt; boolean truncated; List<CachedTable> tables;}`, `CachedTable {String schema; String name; String type; List<CachedColumn> columns;}`, `CachedColumn {String name; String type;}` — all with `equals`/`hashCode` and Jackson-friendly getters/setters + no-arg ctors.

- [ ] **Step 1: Write the failing test**

Create `TestProjectSchemaCache.java` (license header copied from `ProjectContextBlockTest.java`):

```java
package org.apache.drill.exec.server.rest;

import org.apache.drill.exec.server.rest.ProjectSchemaCache.CachedColumn;
import org.apache.drill.exec.server.rest.ProjectSchemaCache.CachedSchema;
import org.apache.drill.exec.server.rest.ProjectSchemaCache.CachedTable;
import org.apache.drill.exec.server.rest.ProjectSchemaCache.ProjectSchemaCacheEntry;
import org.apache.drill.exec.server.rest.ProjectSchemaCache.TableScanner;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.store.InMemoryStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestProjectSchemaCache {

  private PersistentStore<ProjectSchemaCacheEntry> store;
  private ProjectSchemaCache cache;

  @BeforeEach
  public void setUp() {
    store = new InMemoryStore<>(100);
    cache = new ProjectSchemaCache(store);
  }

  private static TableScanner scannerReturning(List<CachedTable> tables, AtomicInteger calls) {
    return schemaPath -> {
      calls.incrementAndGet();
      return tables;
    };
  }

  private static CachedTable table(String schema, String name, String type, String... cols) {
    CachedTable t = new CachedTable();
    t.setSchema(schema);
    t.setName(name);
    t.setType(type);
    java.util.List<CachedColumn> columns = new java.util.ArrayList<>();
    for (int i = 0; i < cols.length; i += 2) {
      CachedColumn c = new CachedColumn();
      c.setName(cols[i]);
      c.setType(cols[i + 1]);
      columns.add(c);
    }
    t.setColumns(columns);
    return t;
  }

  @Test
  public void testExcludedAndNonScannablePredicates() {
    assertTrue(ProjectSchemaCache.isExcludedPlugin("sys"));
    assertTrue(ProjectSchemaCache.isExcludedPlugin("information_schema"));
    assertTrue(ProjectSchemaCache.isExcludedPlugin("cp"));
    assertFalse(ProjectSchemaCache.isExcludedPlugin("dfs"));
    assertTrue(ProjectSchemaCache.isNonScannableConfigClass("HttpStoragePluginConfig"));
    assertTrue(ProjectSchemaCache.isNonScannableConfigClass("GoogleSheetsStoragePluginConfig"));
    assertFalse(ProjectSchemaCache.isNonScannableConfigClass("FileSystemConfig"));
  }

  @Test
  public void testScanStoresEntryAndStampsTime() {
    AtomicInteger calls = new AtomicInteger();
    CachedSchema result = cache.scan("p1", "dfs.logs",
        scannerReturning(List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls));
    assertEquals(1, calls.get());
    assertEquals("dfs.logs", result.getSchemaPath());
    assertTrue(result.getScannedAt() > 0);
    assertEquals(1, result.getTables().size());
    assertEquals("events", result.getTables().get(0).getName());
    // persisted under the project entry
    assertNotNull(cache.getEntry("p1").getSchemas().get("dfs.logs"));
  }

  @Test
  public void testReadServesFreshWithoutRescanning() {
    AtomicInteger calls = new AtomicInteger();
    TableScanner scanner = scannerReturning(
        List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls);
    cache.scan("p1", "dfs.logs", scanner);          // 1 call
    cache.read("p1", "dfs.logs", scanner);          // fresh -> no rescan
    assertEquals(1, calls.get());
  }

  @Test
  public void testReadRescansWhenStale() {
    AtomicInteger calls = new AtomicInteger();
    TableScanner scanner = scannerReturning(
        List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls);
    CachedSchema s = cache.scan("p1", "dfs.logs", scanner);
    // force staleness by rewriting scannedAt to the epoch
    s.setScannedAt(1L);
    ProjectSchemaCacheEntry e = cache.getEntry("p1");
    e.getSchemas().put("dfs.logs", s);
    store.put("p1", e);
    cache.read("p1", "dfs.logs", scanner);          // stale -> rescan
    assertEquals(2, calls.get());
  }

  @Test
  public void testPeekNeverScans() {
    AtomicInteger calls = new AtomicInteger();
    assertNull(cache.peek("p1", "dfs.logs"));       // miss, no scan
    assertEquals(0, calls.get());
    cache.scan("p1", "dfs.logs",
        scannerReturning(List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls));
    assertNotNull(cache.peek("p1", "dfs.logs"));
    assertEquals(1, calls.get());                   // peek added no calls
  }

  @Test
  public void testScanTruncatesAtCap() {
    AtomicInteger calls = new AtomicInteger();
    java.util.List<CachedTable> many = new java.util.ArrayList<>();
    for (int i = 0; i < ProjectSchemaCache.MAX_TABLES_PER_SCHEMA + 5; i++) {
      many.add(table("dfs.logs", "t" + i, "TABLE", "id", "INT"));
    }
    CachedSchema s = cache.scan("p1", "dfs.logs", scannerReturning(many, calls));
    assertTrue(s.isTruncated());
    assertEquals(ProjectSchemaCache.MAX_TABLES_PER_SCHEMA, s.getTables().size());
  }

  @Test
  public void testRemoveAndRemoveProject() {
    AtomicInteger calls = new AtomicInteger();
    TableScanner scanner = scannerReturning(
        List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls);
    cache.scan("p1", "dfs.logs", scanner);
    cache.scan("p1", "mysql", scanner);
    cache.remove("p1", "dfs.logs");
    assertNull(cache.peek("p1", "dfs.logs"));
    assertNotNull(cache.peek("p1", "mysql"));
    cache.removeProject("p1");
    assertNull(cache.getEntry("p1"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl exec/java-exec test -Dtest=TestProjectSchemaCache`
Expected: FAIL — compilation error, `ProjectSchemaCache` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `ProjectSchemaCache.java` (license header copied from `ProjectResources.java`):

```java
package org.apache.drill.exec.server.rest;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.server.rest.QueryWrapper.RestQueryBuilder;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.StoreException;
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
      byKey.put(schema + " " + name, t);
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
      CachedTable t = byKey.get(schema + " " + name);
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

    public String getProjectId() { return projectId; }
    public void setProjectId(String projectId) { this.projectId = projectId; }
    public Map<String, CachedSchema> getSchemas() {
      if (schemas == null) {
        schemas = new LinkedHashMap<>();
      }
      return schemas;
    }
    public void setSchemas(Map<String, CachedSchema> schemas) { this.schemas = schemas; }
  }

  public static class CachedSchema {
    @JsonProperty private String schemaPath;
    @JsonProperty private long scannedAt;
    @JsonProperty private boolean truncated;
    @JsonProperty private List<CachedTable> tables = new ArrayList<>();

    public String getSchemaPath() { return schemaPath; }
    public void setSchemaPath(String schemaPath) { this.schemaPath = schemaPath; }
    public long getScannedAt() { return scannedAt; }
    public void setScannedAt(long scannedAt) { this.scannedAt = scannedAt; }
    public boolean isTruncated() { return truncated; }
    public void setTruncated(boolean truncated) { this.truncated = truncated; }
    public List<CachedTable> getTables() {
      if (tables == null) {
        tables = new ArrayList<>();
      }
      return tables;
    }
    public void setTables(List<CachedTable> tables) { this.tables = tables; }
  }

  public static class CachedTable {
    @JsonProperty private String schema;
    @JsonProperty private String name;
    @JsonProperty private String type;
    @JsonProperty private List<CachedColumn> columns = new ArrayList<>();

    public String getSchema() { return schema; }
    public void setSchema(String schema) { this.schema = schema; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<CachedColumn> getColumns() {
      if (columns == null) {
        columns = new ArrayList<>();
      }
      return columns;
    }
    public void setColumns(List<CachedColumn> columns) { this.columns = columns; }

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

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

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
```

Note: verify `InMemoryStore` constructor arity (`new InMemoryStore<>(100)` — capacity). If the class has a no-arg constructor instead, use it; check `org.apache.drill.exec.store.sys.store.InMemoryStore`. Verify `PersistentStore` has `delete(String)` (it does — used elsewhere); if the method is named `delete`/`deleteKey` adjust accordingly. Verify `QueryResult` exposes `public List<Map<String,String>> rows` (confirmed used in `MetadataResources`). Verify `RestQueryRunner`, `QueryWrapper.RestQueryBuilder`, and `WebUserConnection(webSessionResources)` are in the same package (they are — `MetadataResources` uses them unqualified).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl exec/java-exec test -Dtest=TestProjectSchemaCache`
Expected: PASS (7 tests).

- [ ] **Step 5: Checkstyle**

Run: `mvn -q checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectSchemaCache.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCache.java
git commit -m "Add ProjectSchemaCache store and scan/read/peek logic"
```

---

## Task 2: Wire scanning + endpoints into `ProjectResources`

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java`
- Test: `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCacheEndpoints.java` (create)

**Interfaces:**
- Consumes: `ProjectSchemaCache.get(...)`, `.isScannable(...)`, `.scan(...)`, `.remove(...)`, `.removeProject(...)`, `.infoSchemaScan(...)`, `.getEntry(...)` (Task 1).
- Produces (used by Task 3): `static PersistentStore<Project> openStore(PersistentStoreProvider provider, WorkManager workManager)`; `static boolean canRead(Project, String)` (already exists).

- [ ] **Step 1: Refactor `getStore()` to a static `openStore(...)`**

In `ProjectResources.java`, replace the body of `getStore()` (lines ~1808-1828) so the singleton logic lives in a package-private static method the other resource can reuse:

```java
  static PersistentStore<Project> openStore(PersistentStoreProvider provider, WorkManager workManager) {
    if (cachedStore == null) {
      synchronized (ProjectResources.class) {
        if (cachedStore == null) {
          try {
            cachedStore = provider.getOrCreateStore(
                PersistentStoreConfig.newJacksonBuilder(
                    workManager.getContext().getLpPersistence().getMapper(),
                    Project.class)
                    .name(STORE_NAME)
                    .build());
          } catch (StoreException e) {
            throw new DrillRuntimeException("Failed to access projects store", e);
          }
        }
      }
    }
    return cachedStore;
  }

  private PersistentStore<Project> getStore() {
    return openStore(storeProvider, workManager);
  }
```

- [ ] **Step 2: Inject `WebUserConnection` and add a cache accessor**

Add the field near the other `@Inject` fields (after `storeProvider`, line ~93):

```java
  @Inject
  WebUserConnection webUserConnection;

  @Inject
  StoragePluginRegistry storageRegistry;
```

Add import lines (top of file, with the other imports):

```java
import org.apache.drill.exec.store.StoragePluginRegistry;
```

(`WebUserConnection` is in the same package — no import needed.)

Add a private helper:

```java
  private ProjectSchemaCache schemaCache() {
    return ProjectSchemaCache.get(storeProvider, workManager);
  }

  private void scanDatasetSchema(String projectId, String schema) {
    if (schema == null || !ProjectSchemaCache.isScannable(storageRegistry, schema)) {
      return;
    }
    try {
      schemaCache().scan(projectId, schema,
          s -> ProjectSchemaCache.infoSchemaScan(s, workManager, webUserConnection));
    } catch (Exception e) {
      // Best-effort: a scan failure must never fail the triggering request.
      logger.warn("Schema scan failed for project {} schema {}: {}", projectId, schema, e.getMessage());
    }
  }
```

- [ ] **Step 3: Scan on add, remove on delete**

In `addDataset` (after `store.put(id, project);` at line ~809, still inside the `synchronized` block is fine, but move the scan to AFTER returning-data is built — call it just before `return`):

```java
        project.getDatasets().add(datasetRef);
        project.setUpdatedAt(Instant.now().toEpochMilli());
        store.put(id, project);

        scanDatasetSchema(id, datasetRef.getSchema());

        return Response.ok(project).build();
```

In `removeDataset`, capture the removed dataset's schema and drop it from the cache (replace the `removeIf` block ~844):

```java
      DatasetRef removed = project.getDatasets().stream()
          .filter(d -> d.getId().equals(datasetId))
          .findFirst().orElse(null);
      project.getDatasets().removeIf(d -> d.getId().equals(datasetId));
      project.setUpdatedAt(Instant.now().toEpochMilli());
      store.put(id, project);

      if (removed != null && removed.getSchema() != null) {
        schemaCache().remove(id, removed.getSchema());
      }
```

In `deleteProject` (soft delete) and `purgeProject` (hard delete), after the store write, add:

```java
      schemaCache().removeProject(id);
```

In `cleanupPluginDatasets`, after removing datasets for a plugin from a project (inside the `if (datasets.size() < before)` block, after `store.put`), drop any cached schema under that plugin:

```java
        // also purge cached schemas for the deleted plugin
        ProjectSchemaCacheEntry cacheEntry = schemaCache().getEntry(entry.getKey());
        if (cacheEntry != null) {
          cacheEntry.getSchemas().keySet().removeIf(sp ->
              sp.equals(pluginName) || sp.startsWith(pluginName + "."));
        }
```

Add import for the nested type:

```java
import org.apache.drill.exec.server.rest.ProjectSchemaCache.ProjectSchemaCacheEntry;
```

Then rewrite the last line of that block to persist the pruned cache entry via a new helper, or inline `schemaCache().remove(...)` per key. Simplest: after the loop that prunes keys, call a store put through the cache:

```java
        if (cacheEntry != null) {
          schemaCache().putEntry(entry.getKey(), cacheEntry);
        }
```

Add `putEntry` to `ProjectSchemaCache` (Task 1 class) — a thin setter used only here:

```java
  public synchronized void putEntry(String projectId, ProjectSchemaCacheEntry entry) {
    store.put(projectId, entry);
  }
```

(Add its declaration to Task 1's Interfaces list mentally; implement it in this task on the Task 1 file. Keep the change minimal.)

- [ ] **Step 4: Add refresh + inspect endpoints**

Add two endpoints after `removeDataset` (~line 853):

```java
  @POST
  @Path("/{id}/schema-cache/refresh")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Refresh schema cache",
      description = "Re-scans every scannable schema referenced by the project's datasets")
  public Response refreshSchemaCache(
      @Parameter(description = "Project ID") @PathParam("id") String id) {
    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);
      if (project == null) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found")).build();
      }
      if (!project.getOwner().equals(getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Only the owner can modify this project")).build();
      }
      Set<String> scanned = new HashSet<>();
      for (DatasetRef d : project.getDatasets()) {
        String schema = d.getSchema();
        if (schema != null && scanned.add(schema)) {
          scanDatasetSchema(id, schema);
        }
      }
      return Response.ok(schemaCache().getEntry(id)).build();
    } catch (Exception e) {
      logger.error("Error refreshing schema cache", e);
      throw new DrillRuntimeException("Failed to refresh schema cache: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/{id}/schema-cache")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get schema cache", description = "Returns the project's cached schema metadata")
  public Response getSchemaCache(
      @Parameter(description = "Project ID") @PathParam("id") String id) {
    try {
      PersistentStore<Project> store = getStore();
      Project project = store.get(id);
      if (project == null || project.getDeletedAt() > 0) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(new MessageResponse("Project not found")).build();
      }
      if (!canRead(project, getCurrentUser())) {
        return Response.status(Response.Status.FORBIDDEN)
            .entity(new MessageResponse("Not authorized to read this project")).build();
      }
      ProjectSchemaCacheEntry entry = schemaCache().getEntry(id);
      if (entry == null) {
        entry = new ProjectSchemaCacheEntry();
        entry.setProjectId(id);
      }
      return Response.ok(entry).build();
    } catch (Exception e) {
      logger.error("Error reading schema cache", e);
      throw new DrillRuntimeException("Failed to read schema cache: " + e.getMessage(), e);
    }
  }
```

Ensure `java.util.Set` and `java.util.HashSet` are imported (check existing imports; `HashSet` may need adding).

- [ ] **Step 5: Write the integration test**

Create `TestProjectSchemaCacheEndpoints.java`. Model it on `TestMetadataResources` (JUnit 4, `ClusterTest`, OkHttp). License header from `TestMetadataResources.java`. It should: start a cluster with the web server + `cp` plugin, create a project, add a dataset for a scannable schema (`cp.default` has no tables; use `dfs.tmp` after writing a file, OR simplest: add `sys` — but `sys` is excluded. Use `information_schema`? excluded). Practical approach: add a dataset with schema `cp` — but `cp` is excluded. Use a `dfs` workspace pointed at a temp dir with one CSV, register it, add that schema, then:

```java
  @Test
  public void testRefreshAndGetSchemaCache() throws Exception {
    // 1. POST /api/v1/projects  -> create project, capture id
    // 2. POST /api/v1/projects/{id}/datasets  with {"type":"schema","schema":"dfs.tmp","label":"tmp"}
    // 3. POST /api/v1/projects/{id}/schema-cache/refresh
    // 4. GET  /api/v1/projects/{id}/schema-cache -> assert JSON has schemas["dfs.tmp"]
    //    with scannedAt > 0
  }
```

Fill in with concrete OkHttp calls mirroring `TestMetadataResources` request helpers (copy its `httpClient`, `portNumber`, and a `get`/`post` helper). Assert the response body parses and `schemas` contains the added schema key with `scannedAt > 0`. If setting up a writable `dfs.tmp` with a table in-test is heavy, instead assert the weaker-but-real invariant: after refresh, `GET /schema-cache` returns HTTP 200 with a `schemas` object containing the `dfs.tmp` key (an empty table list is acceptable — the endpoint wiring and scan path are what this verifies).

- [ ] **Step 6: Run tests**

Run: `mvn -q -pl exec/java-exec test -Dtest=TestProjectSchemaCacheEndpoints`
Expected: PASS.

- [ ] **Step 7: Checkstyle**

Run: `mvn -q checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 8: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectResources.java \
        exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProjectSchemaCache.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestProjectSchemaCacheEndpoints.java
git commit -m "Scan project schemas on dataset add; add schema-cache refresh/inspect endpoints"
```

---

## Task 3: Read-through cache in `MetadataResources`

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MetadataResources.java`
- Test: extend `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMetadataResources.java`

**Interfaces:**
- Consumes: `ProjectSchemaCache.get/isScannable/read/infoSchemaScan` (Task 1), `ProjectResources.openStore` + `ProjectResources.canRead` (Task 2).

- [ ] **Step 1: Inject `PersistentStoreProvider` and `SecurityContext`, add a cache helper**

Add field with the other `@Inject`s (line ~74):

```java
  @Inject
  PersistentStoreProvider storeProvider;
```

Import:

```java
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.Context;
import java.security.Principal;
```

Add a helper that returns the authorized cache entry's tables for a schema, or `null` to signal "fall through to live query":

```java
  /**
   * Returns cached tables for {@code schema} when {@code projectId} is supplied, the
   * caller can read that project, and the schema is scannable; otherwise null (the
   * caller then runs the live query). Rescans inline when the entry is stale/missing.
   */
  private ProjectSchemaCache.CachedSchema cachedSchemaFor(String projectId, String schema,
      SecurityContext sc) {
    if (projectId == null || projectId.isEmpty()) {
      return null;
    }
    if (!ProjectSchemaCache.isScannable(storageRegistry, schema)) {
      return null;
    }
    try {
      ProjectResources.Project project =
          ProjectResources.openStore(storeProvider, workManager).get(projectId);
      if (project == null || project.getDeletedAt() > 0
          || !ProjectResources.canRead(project, resolveUser(sc))) {
        return null;
      }
      // schema must actually belong to the project
      boolean referenced = project.getDatasets().stream()
          .anyMatch(d -> schema.equals(d.getSchema())
              || (d.getSchema() != null && schema.startsWith(d.getSchema() + ".")));
      if (!referenced) {
        return null;
      }
      return ProjectSchemaCache.get(storeProvider, workManager)
          .read(projectId, effectiveTrackedSchema(project, schema),
              s -> ProjectSchemaCache.infoSchemaScan(s, workManager, webUserConnection));
    } catch (Exception e) {
      logger.debug("Cache lookup failed for project {} schema {}: {}", projectId, schema, e.getMessage());
      return null;
    }
  }

  /** The dataset schema path that {@code schema} lives under (its own path, or the plugin entry). */
  private String effectiveTrackedSchema(ProjectResources.Project project, String schema) {
    for (org.apache.drill.exec.server.rest.ProjectResources.DatasetRef d : project.getDatasets()) {
      String sp = d.getSchema();
      if (sp != null && (schema.equals(sp) || schema.startsWith(sp + "."))) {
        return sp;
      }
    }
    return schema;
  }

  private static String resolveUser(SecurityContext sc) {
    if (sc == null) {
      return "anonymous";
    }
    Principal p = sc.getUserPrincipal();
    return p != null ? p.getName() : "anonymous";
  }
```

Note the cache entry is keyed by the tracked dataset schema (e.g. `mysql`), and its `tables` each carry a fully-qualified `schema`. When serving `getTables("mysql.store")`, filter the cached tables to those whose `schema` equals the requested one.

- [ ] **Step 2: Read-through in `getTables`**

Change the signature and add the fast path at the top of the method body:

```java
  public TablesResponse getTables(
      @Parameter(description = "Schema name") @PathParam("schema") String schema,
      @Parameter(description = "Project id for cache scoping") @QueryParam("projectId") String projectId,
      @Context SecurityContext sc) {
    logger.debug("Fetching tables for schema: {} (projectId={})", schema, projectId);

    ProjectSchemaCache.CachedSchema cached = cachedSchemaFor(projectId, schema, sc);
    if (cached != null) {
      List<TableInfo> tables = new ArrayList<>();
      for (ProjectSchemaCache.CachedTable t : cached.getTables()) {
        if (schema.equals(t.getSchema())) {
          tables.add(new TableInfo(t.getName(), schema, t.getType() != null ? t.getType() : "TABLE"));
        }
      }
      return new TablesResponse(tables);
    }

    // ... existing live-query body unchanged ...
  }
```

Ensure `@QueryParam` is imported (`jakarta.ws.rs.QueryParam`) — check existing imports; `@PathParam` is already imported so add `QueryParam` similarly.

- [ ] **Step 3: Read-through in `getColumns`**

```java
  public ColumnsResponse getColumns(
      @Parameter(description = "Schema name") @PathParam("schema") String schema,
      @Parameter(description = "Table name") @PathParam("table") String table,
      @Parameter(description = "Project id for cache scoping") @QueryParam("projectId") String projectId,
      @Context SecurityContext sc) {
    logger.debug("Fetching columns for table: {}.{} (projectId={})", schema, table, projectId);

    ProjectSchemaCache.CachedSchema cached = cachedSchemaFor(projectId, schema, sc);
    if (cached != null) {
      for (ProjectSchemaCache.CachedTable t : cached.getTables()) {
        if (schema.equals(t.getSchema()) && table.equals(t.getName())) {
          List<ColumnInfo> columns = new ArrayList<>();
          for (ProjectSchemaCache.CachedColumn c : t.getColumns()) {
            columns.add(new ColumnInfo(c.getName(), c.getType(), true, schema, table));
          }
          // Empty column list (e.g. dynamic-schema "**" source) -> fall through to live probe.
          if (!columns.isEmpty()) {
            return new ColumnsResponse(columns);
          }
        }
      }
    }

    try {
      List<ColumnInfo> columns = fetchColumnsForTable(schema, table);
      return new ColumnsResponse(columns);
    } catch (Exception e) {
      logger.error("Error fetching columns for table: {}.{}", schema, table, e);
      throw new RuntimeException("Failed to fetch columns: " + e.getMessage(), e);
    }
  }
```

- [ ] **Step 4: Add integration assertions**

In `TestMetadataResources.java`, add a test that: creates a project, adds a `dfs.tmp` (or available writable) schema dataset, refreshes the cache, then calls `GET /api/v1/metadata/schemas/dfs.tmp/tables?projectId={id}` and asserts HTTP 200 with a `tables` array (same shape as the non-projectId call). Also assert that `GET .../tables?projectId={id}` for a schema NOT referenced by the project returns the same result as the live call (fall-through). Reuse the file's existing request helpers.

- [ ] **Step 5: Run tests + checkstyle**

Run: `mvn -q -pl exec/java-exec test -Dtest=TestMetadataResources`
Expected: PASS.
Run: `mvn -q checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/MetadataResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/TestMetadataResources.java
git commit -m "Serve project-scoped schema metadata from cache in MetadataResources"
```

---

## Task 4: Prospector reads the cache

**Files:**
- Modify: `exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java`
- Test: extend `exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java` (or a new `ProspectorSchemaCacheBlockTest.java`)

**Interfaces:**
- Consumes: `ProjectSchemaCache.peek(...)`, `.get(...)`, POJOs (Task 1); existing `loadProject` (authorizes via `canRead`).

- [ ] **Step 1: Add a cache-block builder (unit-testable, static)**

In `ProspectorResources.java`, add a static helper mirroring `buildProjectBlock`'s style:

```java
  /**
   * Renders cached tables + columns for the project's dataset schemas into the
   * system prompt, so the model does not need get_schema_info round-trips. Reads
   * are cache-only (peek) — a chat must never run scanning queries.
   */
  static String buildSchemaCacheBlock(ProjectResources.Project project, ProjectSchemaCache cache) {
    if (project == null || cache == null || project.getDatasets() == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    java.util.Set<String> seen = new java.util.HashSet<>();
    for (ProjectResources.DatasetRef d : project.getDatasets()) {
      String schema = d.getSchema();
      if (schema == null || !seen.add(schema)) {
        continue;
      }
      ProjectSchemaCache.CachedSchema cs = cache.peek(project.getId(), schema);
      if (cs == null || cs.getTables().isEmpty()) {
        continue;
      }
      for (ProjectSchemaCache.CachedTable t : cs.getTables()) {
        sb.append("- ").append(t.getSchema()).append(".").append(t.getName());
        if (!t.getColumns().isEmpty()) {
          sb.append(" (");
          for (int i = 0; i < t.getColumns().size(); i++) {
            if (i > 0) {
              sb.append(", ");
            }
            ProjectSchemaCache.CachedColumn c = t.getColumns().get(i);
            sb.append(c.getName()).append(" ").append(c.getType());
          }
          sb.append(")");
        }
        sb.append("\n");
      }
      if (cs.isTruncated()) {
        sb.append("  ...(schema ").append(schema).append(" truncated; use get_schema_info for the rest)\n");
      }
    }
    if (sb.length() == 0) {
      return "";
    }
    return "\nKnown tables and columns in this project (from cache):\n" + sb + "\n";
  }
```

- [ ] **Step 2: Inject the block into the system prompt**

In `buildMessages`, where the project block is appended (after line ~717 `systemPrompt.append(buildProjectBlock(...))`), add:

```java
          systemPrompt.append(buildSchemaCacheBlock(project,
              ProjectSchemaCache.get(storeProvider, workManager)));
```

- [ ] **Step 3: Soften the live-discovery instruction**

Replace the `IMPORTANT: You have tools available ...` block (lines ~890-898) so cached schema is preferred:

```java
    systemPrompt.append("IMPORTANT: Prefer the cached schema listed above when present:\n");
    systemPrompt.append("- If the tables/columns you need are listed above, use them directly and ");
    systemPrompt.append("do NOT call get_schema_info for them.\n");
    systemPrompt.append("- Only call list_schemas / get_schema_info when a needed table or column is ");
    systemPrompt.append("NOT listed above (e.g. the cache is truncated or a source could not be scanned).\n");
    systemPrompt.append("- Drill uses hierarchical schemas (e.g., 'mysql' plugin has sub-schemas ");
    systemPrompt.append("like 'mysql.store').\n");
    systemPrompt.append("- NEVER ask the user for schema or table names — use the list above or explore.\n");
    systemPrompt.append("- After identifying the schema, write and execute the SQL query.\n");
```

- [ ] **Step 4: Write the failing test**

Add to `ProjectContextBlockTest.java` (uses `InMemoryStore` + `ProjectSchemaCache` directly — no cluster):

```java
  @Test
  public void testSchemaCacheBlockListsTablesAndColumns() {
    PersistentStore<ProjectSchemaCache.ProjectSchemaCacheEntry> store = new InMemoryStore<>(100);
    ProjectSchemaCache cache = new ProjectSchemaCache(store);

    ProjectResources.Project p = new ProjectResources.Project();
    p.setId("p1");
    ProjectResources.DatasetRef ds = new ProjectResources.DatasetRef();
    ds.setSchema("dfs.logs");
    p.setDatasets(new ArrayList<>(List.of(ds)));

    cache.scan("p1", "dfs.logs", schemaPath -> {
      ProjectSchemaCache.CachedTable t = new ProjectSchemaCache.CachedTable();
      t.setSchema("dfs.logs");
      t.setName("events");
      t.setType("TABLE");
      ProjectSchemaCache.CachedColumn c = new ProjectSchemaCache.CachedColumn();
      c.setName("id");
      c.setType("INT");
      t.setColumns(new ArrayList<>(List.of(c)));
      return new ArrayList<>(List.of(t));
    });

    String block = ProspectorResources.buildSchemaCacheBlock(p, cache);
    assertTrue(block.contains("dfs.logs.events"));
    assertTrue(block.contains("id INT"));
  }

  @Test
  public void testSchemaCacheBlockEmptyWhenNoCache() {
    PersistentStore<ProjectSchemaCache.ProjectSchemaCacheEntry> store = new InMemoryStore<>(100);
    ProjectSchemaCache cache = new ProjectSchemaCache(store);
    ProjectResources.Project p = new ProjectResources.Project();
    p.setId("p1");
    p.setDatasets(new ArrayList<>());
    assertEquals("", ProspectorResources.buildSchemaCacheBlock(p, cache));
  }
```

Add imports to the test: `org.apache.drill.exec.store.sys.PersistentStore`, `org.apache.drill.exec.store.sys.store.InMemoryStore` (already imported in this file per its existing use).

- [ ] **Step 5: Run tests + checkstyle**

Run: `mvn -q -pl exec/java-exec test -Dtest=ProjectContextBlockTest`
Expected: PASS.
Run: `mvn -q checkstyle:check -pl exec/java-exec`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add exec/java-exec/src/main/java/org/apache/drill/exec/server/rest/ProspectorResources.java \
        exec/java-exec/src/test/java/org/apache/drill/exec/server/rest/ProjectContextBlockTest.java
git commit -m "Inject cached project schema into Prospector system prompt"
```

---

## Task 5: Frontend API client — projectId params + schema-cache endpoints

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/api/metadata.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/api/projects.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/types/index.ts`

**Interfaces:**
- Produces (used by Tasks 6–7): `getTables(schema, projectId?)`, `getColumns(schema, table, projectId?)`, `refreshSchemaCache(projectId)`, `getSchemaCache(projectId)`, type `SchemaCacheResponse`.

- [ ] **Step 1: Add `projectId` to `getTables`/`getColumns`**

In `api/metadata.ts`, replace the two functions:

```typescript
export async function getTables(schema: string, projectId?: string): Promise<TableInfo[]> {
  const response = await apiClient.get<TablesResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/tables`,
    { params: projectId ? { projectId } : undefined }
  );
  return response.data.tables;
}

export async function getColumns(schema: string, table: string, projectId?: string): Promise<ColumnInfo[]> {
  const response = await apiClient.get<ColumnsResponse>(
    `${METADATA_BASE}/schemas/${encodeURIComponent(schema)}/tables/${encodeURIComponent(table)}/columns`,
    { params: projectId ? { projectId } : undefined }
  );
  return response.data.columns;
}
```

- [ ] **Step 2: Add the `SchemaCacheResponse` type**

In `types/index.ts`, near the `Project` types (after line ~349), add:

```typescript
export interface CachedColumnInfo { name: string; type: string; }
export interface CachedTableInfo {
  schema: string;
  name: string;
  type: string;
  columns: CachedColumnInfo[];
}
export interface CachedSchemaInfo {
  schemaPath: string;
  scannedAt: number;
  truncated: boolean;
  tables: CachedTableInfo[];
}
export interface SchemaCacheResponse {
  projectId: string;
  schemas: Record<string, CachedSchemaInfo>;
}
```

- [ ] **Step 3: Add the schema-cache client functions**

In `api/projects.ts`, add (import `SchemaCacheResponse` from `../types`):

```typescript
export async function getSchemaCache(projectId: string): Promise<SchemaCacheResponse> {
  const response = await apiClient.get<SchemaCacheResponse>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/schema-cache`
  );
  return response.data;
}

export async function refreshSchemaCache(projectId: string): Promise<SchemaCacheResponse> {
  const response = await apiClient.post<SchemaCacheResponse>(
    `${PROJECTS_BASE}/${encodeURIComponent(projectId)}/schema-cache/refresh`
  );
  return response.data;
}
```

(Match the existing import style in `projects.ts` — confirm whether it uses `apiClient` or a local `apiFetch`; mirror the file's existing functions such as `addDataset`.)

- [ ] **Step 4: Typecheck / build**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/api/metadata.ts \
        exec/java-exec/src/main/resources/webapp/src/api/projects.ts \
        exec/java-exec/src/main/resources/webapp/src/types/index.ts
git commit -m "Add projectId params and schema-cache client functions to webapp API"
```

---

## Task 6: Thread `projectId` through the tree + autocomplete read path

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/SchemaExplorer.tsx`
- Modify: `exec/java-exec/src/main/resources/webapp/src/hooks/useMonacoCompletion.ts`
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx`

**Interfaces:**
- Consumes: `getTables(schema, projectId?)`, `getColumns(schema, table, projectId?)` (Task 5).

- [ ] **Step 1: Pass `projectId` in `SchemaExplorer`**

`SchemaExplorer` already destructures `projectId` (line ~310). Update the two expand call sites:
- Line ~561: `const tables = await getTables(schemaName, projectId);`
- Line ~689: `const columns = await getColumns(schemaName, tableName, projectId);`

- [ ] **Step 2: Accept + forward `projectId` in `useMonacoCompletion`**

Change the signature (line ~44):

```typescript
export function useMonacoCompletion(
  monaco: Monaco | null,
  schemas: SchemaInfo[] | undefined,
  projectId?: string,
) {
```

At the two internal call sites (near lines ~104 and ~134):

```typescript
const result = await getTables(qualifier, projectId);
...
const result = await getColumns(parentSchema, tableName, projectId);
```

Add `projectId` to the `useEffect` dependency array so the provider re-registers when project scope changes (find the effect's dep array in the hook and append `projectId`).

- [ ] **Step 3: Pass `projectId` from `SqlLabPage`**

Line ~267: `useMonacoCompletion(monacoInstance, schemas, projectId);`

- [ ] **Step 4: Typecheck**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/components/schema-explorer/SchemaExplorer.tsx \
        exec/java-exec/src/main/resources/webapp/src/hooks/useMonacoCompletion.ts \
        exec/java-exec/src/main/resources/webapp/src/pages/SqlLabPage.tsx
git commit -m "Pass projectId to tree and autocomplete metadata calls for cache scoping"
```

---

## Task 7: Refresh button + "last scanned" on Project Data Sources page

**Files:**
- Modify: `exec/java-exec/src/main/resources/webapp/src/pages/ProjectDataSourcesPage.tsx`

**Interfaces:**
- Consumes: `getSchemaCache(projectId)`, `refreshSchemaCache(projectId)` (Task 5).

- [ ] **Step 1: Load cache state + wire the refresh mutation**

Add imports:

```typescript
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { removeDataset, getSchemaCache, refreshSchemaCache } from '../api/projects';
import { message } from 'antd';
```

Inside the component (after the existing `removeDatasetMutation`):

```typescript
  const { data: schemaCache } = useQuery({
    queryKey: ['schemaCache', projectId],
    queryFn: () => getSchemaCache(projectId!),
    enabled: !!projectId,
  });

  const refreshMutation = useMutation({
    mutationFn: () => refreshSchemaCache(projectId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['schemaCache', projectId] });
      message.success('Schema cache refreshed');
    },
    onError: () => message.error('Failed to refresh schema cache'),
  });

  const lastScanned = schemaCache
    ? Object.values(schemaCache.schemas)
        .map((s) => s.scannedAt)
        .filter((t) => t > 0)
        .sort((a, b) => b - a)[0]
    : undefined;
```

- [ ] **Step 2: Render the button + indicator**

In the `Card` header/toolbar area (near the existing "Add" button ~line 69), add:

```tsx
            <Button
              onClick={() => refreshMutation.mutate()}
              loading={refreshMutation.isPending}
            >
              Refresh schema cache
            </Button>
            {lastScanned ? (
              <Typography.Text type="secondary" style={{ marginLeft: 8 }}>
                Last scanned {new Date(lastScanned).toLocaleString()}
              </Typography.Text>
            ) : null}
```

(Adjust JSX to fit the existing layout — place inside the same toolbar container as the current Add button.)

- [ ] **Step 3: Typecheck + build**

Run: `cd exec/java-exec/src/main/resources/webapp && npx tsc --noEmit && npm run build`
Expected: build succeeds.

- [ ] **Step 4: Manual verification**

Run a Drillbit, open a project, add a `dfs`/JDBC data source, confirm: (a) the add returns promptly and the cache populates; (b) "Refresh schema cache" updates the "Last scanned" time; (c) expanding that schema in SQL Lab and typing `schema.` autocompletes without a visible delay; (d) HTTP/GoogleSheets data sources show no cache and still browse live.

- [ ] **Step 5: Commit**

```bash
git add exec/java-exec/src/main/resources/webapp/src/pages/ProjectDataSourcesPage.tsx
git commit -m "Add schema-cache refresh button and last-scanned indicator to project data sources"
```

---

## Task 8: Documentation

**Files:**
- Modify: `docs/dev/PROSPECTOR.md`
- Modify: `docs/dev/ui/pages/` project data sources doc (if one exists; else skip)

- [ ] **Step 1: Document the cache**

In `docs/dev/PROSPECTOR.md`, under "Project Context", add a short subsection: the per-project schema cache (`drill.sqllab.schema_cache`), how it is populated (dataset add / stale read / refresh button), that HTTP and GoogleSheets are never scanned, and that Prospector peeks it (never scanning mid-chat) to inject known tables/columns, softening live discovery to a fallback.

- [ ] **Step 2: Commit**

```bash
git add docs/dev/PROSPECTOR.md
git commit -m "Document project schema cache"
```

---

## Self-Review Notes

- **Spec coverage:** per-project store (Task 1) · skip http/googlesheets (Task 1 `isScannable`, verified Task 3) · tables+columns with type via two IS queries (Task 1 `infoSchemaScan`) · scan on add (Task 2) · refresh/inspect endpoints (Task 2) · read-through for tree+autocomplete with `canRead` gate (Task 3, 6) · Prospector peek + softened instruction (Task 4) · refresh UI + last-scanned (Task 7) · size cap + staleness constants (Task 1) · access control (Task 3 `cachedSchemaFor`, integration-asserted). All covered.
- **Verification anchors to confirm during implementation** (do not assume): `InMemoryStore` constructor arity; `PersistentStore.delete` method name; `QueryResult.rows` type; `WebUserConnection.webSessionResources` visibility; `projects.ts` uses `apiClient` (vs a local fetch wrapper); exact dependency array in `useMonacoCompletion`'s effect; the toolbar container in `ProjectDataSourcesPage`.
- **`putEntry`** was introduced in Task 1's class but first used in Task 2 (`cleanupPluginDatasets`) — implement it on the Task 1 file when you reach Task 2, or add it during Task 1.
