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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.server.rest.ResultCacheService.CacheMeta;
import org.apache.drill.exec.server.rest.ResultCacheService.CacheStats;
import org.apache.drill.exec.server.rest.ResultCacheService.PaginatedRows;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ResultCacheService}.
 */
public class TestResultCacheService {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ResultCacheService cacheService;
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    mapper = new ObjectMapper();
    File cacheDir = new File(tempFolder.getRoot(), "cache");
    // TTL=60min, maxTotal=100MB, maxResult=10MB, maxRows=10000
    cacheService = new ResultCacheService(cacheDir, 60, 100, 10, 10000, mapper);
  }

  private List<Map<String, String>> createTestRows(int count) {
    List<Map<String, String>> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      Map<String, String> row = new LinkedHashMap<>();
      row.put("id", String.valueOf(i));
      row.put("name", "row-" + i);
      row.put("value", String.valueOf(i * 10));
      rows.add(row);
    }
    return rows;
  }

  @Test
  public void testCacheAndRetrieveResult() throws IOException {
    List<String> columns = Arrays.asList("id", "name", "value");
    List<String> metadata = Arrays.asList("INTEGER", "VARCHAR", "INTEGER");
    List<Map<String, String>> rows = createTestRows(5);

    CacheMeta meta = cacheService.cacheResult(
        "q1", "SELECT * FROM test", "dfs", "user1", "COMPLETED",
        columns, metadata, rows);

    assertNotNull(meta);
    assertEquals("q1", meta.queryId);
    assertEquals("SELECT * FROM test", meta.sql);
    assertEquals(5, meta.totalRows);
    assertEquals(3, meta.columns.size());
    assertTrue(meta.sizeBytes > 0);

    // Retrieve metadata
    CacheMeta retrieved = cacheService.getMetadata(meta.cacheId);
    assertNotNull(retrieved);
    assertEquals(meta.cacheId, retrieved.cacheId);
  }

  @Test
  public void testFindBySqlHash() throws IOException {
    List<String> columns = Arrays.asList("id", "name");
    List<Map<String, String>> rows = createTestRows(3);

    cacheService.cacheResult(
        "q2", "SELECT id, name FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // Same SQL should find the cached result
    String cacheId = cacheService.findBySqlHash("SELECT id, name FROM test", "dfs", "user1");
    assertNotNull(cacheId);

    // Different SQL should not find it
    String noCacheId = cacheService.findBySqlHash("SELECT * FROM other", "dfs", "user1");
    assertNull(noCacheId);
  }

  @Test
  public void testSqlHashNormalization() throws IOException {
    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows = createTestRows(1);

    cacheService.cacheResult(
        "q3", "SELECT * FROM test;", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // Trailing semicolons and outer whitespace should be stripped
    String cacheId = cacheService.findBySqlHash("  SELECT * FROM test;  ", "dfs", "user1");
    assertNotNull(cacheId);

    // Case should not matter
    String cacheId2 = cacheService.findBySqlHash("select * from test", "dfs", "user1");
    assertNotNull(cacheId2);
  }

  @Test
  public void testSqlHashDiffersBySchemaAndUser() {
    // Same SQL with different schema should produce different hash
    String hash1 = ResultCacheService.computeSqlHash("SELECT 1", "dfs", "user1");
    String hash2 = ResultCacheService.computeSqlHash("SELECT 1", "cp", "user1");
    assertFalse(hash1.equals(hash2), "Different schemas should produce different hashes");

    // Same SQL with different user should produce different hash
    String hash3 = ResultCacheService.computeSqlHash("SELECT 1", "dfs", "user2");
    assertFalse(hash1.equals(hash3), "Different users should produce different hashes");
  }

  @Test
  public void testPaginatedRows() throws IOException {
    List<String> columns = Arrays.asList("id", "name", "value");
    List<Map<String, String>> rows = createTestRows(20);

    CacheMeta meta = cacheService.cacheResult(
        "q4", "SELECT * FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // First page
    PaginatedRows page1 = cacheService.getRows(meta.cacheId, 0, 5);
    assertNotNull(page1);
    assertEquals(5, page1.rows.size());
    assertEquals(0, page1.offset);
    assertEquals(5, page1.limit);
    assertEquals(20, page1.totalRows);
    assertTrue(page1.hasMore);
    assertEquals("0", page1.rows.get(0).get("id"));
    assertEquals("4", page1.rows.get(4).get("id"));

    // Second page
    PaginatedRows page2 = cacheService.getRows(meta.cacheId, 5, 5);
    assertNotNull(page2);
    assertEquals(5, page2.rows.size());
    assertEquals("5", page2.rows.get(0).get("id"));
    assertTrue(page2.hasMore);

    // Last page
    PaginatedRows lastPage = cacheService.getRows(meta.cacheId, 15, 5);
    assertNotNull(lastPage);
    assertEquals(5, lastPage.rows.size());
    assertFalse(lastPage.hasMore);
    assertEquals("19", lastPage.rows.get(4).get("id"));

    // Beyond end
    PaginatedRows empty = cacheService.getRows(meta.cacheId, 20, 5);
    assertNotNull(empty);
    assertEquals(0, empty.rows.size());
    assertFalse(empty.hasMore);
  }

  @Test
  public void testEvict() throws IOException {
    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows = createTestRows(3);

    CacheMeta meta = cacheService.cacheResult(
        "q5", "SELECT 1", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    assertNotNull(cacheService.getMetadata(meta.cacheId));

    boolean evicted = cacheService.evict(meta.cacheId);
    assertTrue(evicted);

    assertNull(cacheService.getMetadata(meta.cacheId));
    assertNull(cacheService.findBySqlHash("SELECT 1", "dfs", "user1"));
  }

  @Test
  public void testEvictNonexistent() {
    // Use a valid UUID format that doesn't exist in the cache
    boolean evicted = cacheService.evict("00000000-0000-0000-0000-000000000000");
    assertFalse(evicted);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEvictInvalidCacheId() {
    cacheService.evict("not-a-valid-uuid");
  }

  @Test
  public void testCacheStats() throws IOException {
    CacheStats stats = cacheService.getStats();
    assertEquals(0, stats.entryCount);
    assertEquals(0, stats.totalSizeBytes);
    assertEquals(100 * 1024 * 1024, stats.maxTotalSizeBytes);
    assertEquals(60, stats.ttlMinutes);

    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows = createTestRows(5);

    cacheService.cacheResult(
        "q6", "SELECT 1", "dfs", "user1", "COMPLETED",
        columns, null, rows);
    cacheService.cacheResult(
        "q7", "SELECT 2", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    stats = cacheService.getStats();
    assertEquals(2, stats.entryCount);
    assertTrue(stats.totalSizeBytes > 0);
  }

  @Test
  public void testMaxRowsLimit() throws IOException {
    // Create service with small maxRows
    File cacheDir = new File(tempFolder.getRoot(), "cache-small-rows");
    ResultCacheService smallService = new ResultCacheService(cacheDir, 60, 100, 10, 5, mapper);

    List<String> columns = Arrays.asList("id", "name");
    List<Map<String, String>> rows = createTestRows(20);

    CacheMeta meta = smallService.cacheResult(
        "q8", "SELECT * FROM big", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    assertNotNull(meta);
    assertEquals(5, meta.totalRows); // Truncated to maxRows

    PaginatedRows allRows = smallService.getRows(meta.cacheId, 0, 100);
    assertEquals(5, allRows.rows.size());
  }

  @Test
  public void testStreamAllRowsAsJson() throws IOException {
    List<String> columns = Arrays.asList("id", "name");
    List<Map<String, String>> rows = createTestRows(3);

    CacheMeta meta = cacheService.cacheResult(
        "q9", "SELECT id, name FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    cacheService.streamAllRows(meta.cacheId, "json", out);

    String json = out.toString("UTF-8");
    assertTrue(json.startsWith("["));
    assertTrue(json.endsWith("]"));
    assertTrue(json.contains("\"id\""));
    assertTrue(json.contains("\"name\""));
  }

  @Test
  public void testStreamAllRowsAsCsv() throws IOException {
    List<String> columns = Arrays.asList("id", "name");
    List<Map<String, String>> rows = new ArrayList<>();
    Map<String, String> row1 = new LinkedHashMap<>();
    row1.put("id", "1");
    row1.put("name", "Alice");
    rows.add(row1);
    Map<String, String> row2 = new LinkedHashMap<>();
    row2.put("id", "2");
    row2.put("name", "Bob, Jr.");
    rows.add(row2);

    CacheMeta meta = cacheService.cacheResult(
        "q10", "SELECT id, name FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    cacheService.streamAllRows(meta.cacheId, "csv", out);

    String csv = out.toString("UTF-8");
    String[] lines = csv.split("\n");
    assertEquals(3, lines.length); // header + 2 rows
    assertEquals("id,name", lines[0]);
    assertEquals("1,Alice", lines[1]);
    // Comma in value should be escaped with quotes
    assertEquals("2,\"Bob, Jr.\"", lines[2]);
  }

  @Test
  public void testRebuildIndexOnRestart() throws IOException {
    File cacheDir = new File(tempFolder.getRoot(), "cache-restart");
    ResultCacheService service1 = new ResultCacheService(cacheDir, 60, 100, 10, 10000, mapper);

    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows = createTestRows(3);

    CacheMeta meta = service1.cacheResult(
        "q11", "SELECT * FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    String cacheId = meta.cacheId;

    // Create a new service pointing to the same directory (simulating restart)
    ResultCacheService service2 = new ResultCacheService(cacheDir, 60, 100, 10, 10000, mapper);

    // Should find the entry from disk
    CacheMeta rebuilt = service2.getMetadata(cacheId);
    assertNotNull(rebuilt, "Should rebuild cache index from disk");
    assertEquals(cacheId, rebuilt.cacheId);
    assertEquals("q11", rebuilt.queryId);
    assertEquals(3, rebuilt.totalRows);

    // SQL hash lookup should also work
    String found = service2.findBySqlHash("SELECT * FROM test", "dfs", "user1");
    assertNotNull(found);
    assertEquals(cacheId, found);
  }

  @Test
  public void testExpiredEntriesEvictedOnRebuild() throws IOException {
    File cacheDir = new File(tempFolder.getRoot(), "cache-expire");
    // TTL = 0 means entries expire immediately
    ResultCacheService service1 = new ResultCacheService(cacheDir, 0, 100, 10, 10000, mapper);

    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows = createTestRows(1);

    CacheMeta meta = service1.cacheResult(
        "q12", "SELECT 1", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // Entries should expire (TTL=0 means cachedAt + 0 < now)
    // The entry might still be in the index of service1, but on a new service it should be evicted
    ResultCacheService service2 = new ResultCacheService(cacheDir, 0, 100, 10, 10000, mapper);
    CacheMeta expired = service2.getMetadata(meta.cacheId);
    assertNull(expired, "Expired entries should not be available");
  }

  @Test
  public void testLruEviction() throws IOException {
    File cacheDir = new File(tempFolder.getRoot(), "cache-lru");
    // maxTotal = 1KB to force eviction quickly
    ResultCacheService smallService = new ResultCacheService(cacheDir, 60, 0, 10, 10000, mapper);

    List<String> columns = Arrays.asList("id", "name", "value");
    List<Map<String, String>> rows = createTestRows(50);

    // Cache first entry
    CacheMeta meta1 = smallService.cacheResult(
        "q13", "SELECT 1", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // Cache second entry — this should trigger eviction of the first
    CacheMeta meta2 = smallService.cacheResult(
        "q14", "SELECT 2", "dfs", "user1", "COMPLETED",
        columns, null, rows);

    // At least one entry should exist (the newest one)
    if (meta2 != null) {
      CacheStats stats = smallService.getStats();
      // With maxTotal=0, all entries should be evicted except possibly the last
      assertTrue(stats.entryCount <= 1,
          "LRU eviction should keep entry count low when max total is 0");
    }
  }

  @Test
  public void testComputeSqlHashConsistency() {
    String hash1 = ResultCacheService.computeSqlHash("SELECT 1", "dfs", "user1");
    String hash2 = ResultCacheService.computeSqlHash("SELECT 1", "dfs", "user1");
    assertEquals(hash1, hash2, "Same inputs should produce same hash");

    // Hash should be 64 chars (SHA-256 hex)
    assertEquals(64, hash1.length());
  }

  @Test
  public void testComputeSqlHashNullHandling() {
    // Should not throw for null inputs
    String hash = ResultCacheService.computeSqlHash(null, null, null);
    assertNotNull(hash);
    assertEquals(64, hash.length());
  }

  @Test
  public void testGetRowsNonexistentEntry() throws IOException {
    // Use a valid UUID format that doesn't exist in the cache
    PaginatedRows result = cacheService.getRows("00000000-0000-0000-0000-000000000000", 0, 10);
    assertNull(result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetRowsInvalidCacheId() throws IOException {
    cacheService.getRows("../../../etc/passwd", 0, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetMetadataInvalidCacheId() {
    cacheService.getMetadata("../../../etc/passwd");
  }

  @Test
  public void testMetadataFields() throws IOException {
    List<String> columns = Arrays.asList("col1", "col2");
    List<String> metadata = Arrays.asList("VARCHAR", "INTEGER");
    List<Map<String, String>> rows = createTestRows(2);

    CacheMeta meta = cacheService.cacheResult(
        "q15", "SELECT col1, col2 FROM test", "myschema", "testuser", "COMPLETED",
        columns, metadata, rows);

    assertNotNull(meta);
    assertEquals("q15", meta.queryId);
    assertEquals("myschema", meta.defaultSchema);
    assertEquals("testuser", meta.userName);
    assertEquals("COMPLETED", meta.queryState);
    assertEquals(Arrays.asList("col1", "col2"), meta.columns);
    assertEquals(Arrays.asList("VARCHAR", "INTEGER"), meta.metadata);
    assertEquals(2, meta.totalRows);
    assertTrue(meta.cachedAt > 0);
    assertTrue(meta.lastAccessedAt > 0);
    assertNotNull(meta.sqlHash);
  }

  @Test
  public void testCacheOverwritesSameSql() throws IOException {
    List<String> columns = Arrays.asList("id");
    List<Map<String, String>> rows1 = createTestRows(3);
    List<Map<String, String>> rows2 = createTestRows(5);

    CacheMeta meta1 = cacheService.cacheResult(
        "q16", "SELECT * FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows1);
    CacheMeta meta2 = cacheService.cacheResult(
        "q17", "SELECT * FROM test", "dfs", "user1", "COMPLETED",
        columns, null, rows2);

    // The SQL hash index should now point to the second entry
    String found = cacheService.findBySqlHash("SELECT * FROM test", "dfs", "user1");
    assertEquals(meta2.cacheId, found);

    // Both entries should still exist in metadata index (old one is orphaned from hash index)
    // The old entry is accessible by direct cacheId
    assertNotNull(cacheService.getMetadata(meta1.cacheId));
    assertNotNull(cacheService.getMetadata(meta2.cacheId));
  }
}
