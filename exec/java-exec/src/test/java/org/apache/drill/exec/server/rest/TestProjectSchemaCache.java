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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  public void testScanOnUnchangedSnapshotResetsStalenessClock() {
    AtomicInteger calls = new AtomicInteger();
    TableScanner scanner = scannerReturning(
        List.of(table("dfs.logs", "events", "TABLE", "id", "INT")), calls);
    cache.scan("p1", "dfs.logs", scanner);            // 1 call

    // force staleness by rewriting scannedAt to the epoch
    CachedSchema s = cache.getEntry("p1").getSchemas().get("dfs.logs");
    s.setScannedAt(1L);
    ProjectSchemaCacheEntry e = cache.getEntry("p1");
    e.getSchemas().put("dfs.logs", s);
    store.put("p1", e);

    cache.read("p1", "dfs.logs", scanner);            // stale -> rescan
    assertEquals(2, calls.get());

    // The rescan returned the same tables snapshot, so the entry's scannedAt must
    // have been bumped to "now". A second read right away should therefore be
    // served fresh, without a third scan.
    cache.read("p1", "dfs.logs", scanner);
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

  /**
   * A scanner that had to stop early is as much a truncation as overflowing the cap, and
   * the table count cannot show it: a folder scan that runs out of listings, or hits its
   * depth limit, comes back under the cap with files missing. Left unmarked, the entry
   * would read as a complete picture of the schema.
   */
  @Test
  public void testScannerThatStoppedEarlyMarksTheSchemaTruncated() {
    AtomicInteger calls = new AtomicInteger();
    CachedSchema s = cache.scan("p1", "dfs.tmp", scannerReturning(
        new ProjectSchemaCache.PartialTables(
            List.of(table("dfs.tmp", "p1/one.csv", "FILE", "id", "INT"))),
        calls));
    assertTrue(s.isTruncated());
    assertEquals(1, s.getTables().size());

    // And a scanner that finished does not.
    CachedSchema full = cache.scan("p2", "dfs.tmp", scannerReturning(
        List.of(table("dfs.tmp", "p2/one.csv", "FILE", "id", "INT")), calls));
    assertFalse(full.isTruncated());
  }

  /**
   * The upload path's incremental merge. It must not claim the schema was scanned: a
   * project that has never had a full scan still needs one, and {@code read} decides
   * that from {@code scannedAt}.
   */
  @Test
  public void testPutTableAddsOneTableWithoutClaimingAScan() {
    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/sales.csv", "FILE", "id", "INT"));

    CachedSchema s = cache.peek("p1", "dfs.tmp");
    assertNotNull(s);
    assertEquals(1, s.getTables().size());
    assertEquals("p1/sales.csv", s.getTables().get(0).getName());
    assertEquals(0L, s.getScannedAt());

    AtomicInteger calls = new AtomicInteger();
    cache.read("p1", "dfs.tmp", scannerReturning(
        List.of(table("dfs.tmp", "p1/sales.csv", "FILE", "id", "INT")), calls));
    assertEquals(1, calls.get());
  }

  /** An overwrite re-probes a file that is already cached; it must not double it. */
  @Test
  public void testPutTableReplacesTheTableOfTheSameName() {
    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/sales.csv", "FILE", "a", "INT"));
    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/sales.csv", "FILE", "b", "VARCHAR"));

    CachedSchema s = cache.peek("p1", "dfs.tmp");
    assertEquals(1, s.getTables().size());
    assertEquals(1, s.getTables().get(0).getColumns().size());
    assertEquals("b", s.getTables().get(0).getColumns().get(0).getName());
  }

  @Test
  public void testPutTableHonoursTheCap() {
    AtomicInteger calls = new AtomicInteger();
    java.util.List<CachedTable> many = new java.util.ArrayList<>();
    for (int i = 0; i < ProjectSchemaCache.MAX_TABLES_PER_SCHEMA; i++) {
      many.add(table("dfs.tmp", "t" + i, "FILE", "id", "INT"));
    }
    cache.scan("p1", "dfs.tmp", scannerReturning(many, calls));

    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/one-too-many.csv", "FILE"));
    CachedSchema s = cache.peek("p1", "dfs.tmp");
    assertEquals(ProjectSchemaCache.MAX_TABLES_PER_SCHEMA, s.getTables().size());
    assertTrue(s.isTruncated());
  }

  @Test
  public void testRemoveTableDropsOneTableAndLeavesTheRest() {
    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/keep.csv", "FILE", "a", "INT"));
    cache.putTable("p1", "dfs.tmp", table("dfs.tmp", "p1/drop.csv", "FILE", "a", "INT"));

    cache.removeTable("p1", "dfs.tmp", "p1/drop.csv");
    assertEquals(1, cache.peek("p1", "dfs.tmp").getTables().size());
    assertEquals("p1/keep.csv", cache.peek("p1", "dfs.tmp").getTables().get(0).getName());

    // Unknown names, schemas and projects are all no-ops.
    cache.removeTable("p1", "dfs.tmp", "p1/never-there.csv");
    cache.removeTable("p1", "mysql", "p1/keep.csv");
    cache.removeTable("no-such-project", "dfs.tmp", "p1/keep.csv");
    assertEquals(1, cache.peek("p1", "dfs.tmp").getTables().size());
  }

  /**
   * {@code read} tests staleness without the monitor, so N readers of one stale schema
   * each used to run a full sweep in turn: with {@code combinedScan} that is up to 100
   * listings plus 500 probes apiece, every one of them holding a process-wide monitor.
   * The second reader must be served the first reader's result instead.
   */
  @Test
  public void testConcurrentStaleReadsCollapseToOneScan() throws Exception {
    // A schema that was scanned at the epoch: stale for both readers.
    ProjectSchemaCacheEntry seed = new ProjectSchemaCacheEntry();
    seed.setProjectId("p1");
    CachedSchema stale = new CachedSchema();
    stale.setSchemaPath("dfs.tmp");
    stale.setScannedAt(1L);
    seed.getSchemas().put("dfs.tmp", stale);
    store.put("p1", seed);

    AtomicInteger calls = new AtomicInteger();
    CountDownLatch scanning = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    TableScanner scanner = schemaPath -> {
      calls.incrementAndGet();
      scanning.countDown();
      release.await(10, TimeUnit.SECONDS);
      return List.of(table("dfs.tmp", "p1/one.csv", "FILE", "id", "INT"));
    };

    Thread first = new Thread(() -> cache.read("p1", "dfs.tmp", scanner), "reader-1");
    first.start();
    assertTrue(scanning.await(10, TimeUnit.SECONDS));

    // The second reader sees the stale entry (the store never blocks), then waits on the
    // monitor the first reader is holding. BLOCKED is therefore past its own peek.
    Thread second = new Thread(() -> cache.read("p1", "dfs.tmp", scanner), "reader-2");
    second.start();
    long until = System.currentTimeMillis() + 10_000L;
    while (second.getState() != Thread.State.BLOCKED && System.currentTimeMillis() < until) {
      Thread.sleep(5);
    }
    assertEquals(Thread.State.BLOCKED, second.getState());

    release.countDown();
    first.join(10_000L);
    second.join(10_000L);
    assertEquals(1, calls.get());
    assertEquals(1, cache.peek("p1", "dfs.tmp").getTables().size());
  }

  /**
   * The sweep's wall-clock budget. The per-query deadline bounds one query; without a
   * budget for the sweep as a whole, 100 listings and 500 probes could each take their
   * 30 seconds while this class's monitor was held. An expired budget returns what it
   * has as a {@link ProjectSchemaCache.PartialTables} and issues no query at all, which
   * is why a null {@code WorkManager} and connection are safe here: touching either
   * would be the failure.
   */
  @Test
  public void testFileScanStopsWhenItsBudgetIsSpent() throws Exception {
    List<CachedTable> tables = ProjectSchemaCache.fileScan("dfs.tmp", "p1", null, null,
        System.currentTimeMillis() - 1);
    assertTrue(tables instanceof ProjectSchemaCache.PartialTables);
    assertTrue(tables.isEmpty());
  }

  /**
   * And the budget is spent once per sweep, not once per folder. A project accumulates a
   * folder reference per upload target it has been pointed at, so a per-folder budget
   * would let one sweep hold the monitor for the budget times the number of references.
   * Two folders under one expired deadline must both stop, and neither may query.
   */
  @Test
  public void testTheBudgetIsSharedAcrossAllOfASchemasFolders() throws Exception {
    List<CachedTable> tables = ProjectSchemaCache.folderScans("dfs.tmp",
        List.of("p1", "p2"), null, null, System.currentTimeMillis() - 1);
    assertTrue(tables instanceof ProjectSchemaCache.PartialTables);
    assertTrue(tables.isEmpty());
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
