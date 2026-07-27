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
