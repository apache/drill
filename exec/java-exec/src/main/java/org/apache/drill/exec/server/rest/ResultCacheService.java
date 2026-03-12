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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * File-based result cache for SQL Lab queries.
 * Stores query results on disk so they persist across Drill restarts.
 *
 * <p>Each cached result is stored as a directory containing:
 * <ul>
 *   <li>{@code meta.json} — columns, metadata, queryId, totalRows, timestamps</li>
 *   <li>{@code data.json} — the rows as a JSON array</li>
 * </ul>
 *
 * <p>Cache eviction is LRU-based with configurable TTL and max total size.
 */
public class ResultCacheService {

  private static final Logger logger = LoggerFactory.getLogger(ResultCacheService.class);
  private static final String META_FILE = "meta.json";
  private static final String DATA_FILE = "data.json";

  private final File cacheDir;
  private final long ttlMs;
  private final long maxTotalBytes;
  private final long maxResultBytes;
  private final int maxRows;
  private final ObjectMapper mapper;

  // In-memory index: sqlHash -> cacheId for cache hit lookups
  private final ConcurrentHashMap<String, String> sqlHashIndex = new ConcurrentHashMap<>();
  // In-memory metadata: cacheId -> CacheMeta
  private final ConcurrentHashMap<String, CacheMeta> metadataIndex = new ConcurrentHashMap<>();

  public ResultCacheService(File cacheDir, long ttlMinutes, long maxTotalMb,
                            long maxResultMb, int maxRows, ObjectMapper mapper) {
    this.cacheDir = cacheDir;
    this.ttlMs = ttlMinutes * 60 * 1000;
    this.maxTotalBytes = maxTotalMb * 1024 * 1024;
    this.maxResultBytes = maxResultMb * 1024 * 1024;
    this.maxRows = maxRows;
    this.mapper = mapper;

    if (!cacheDir.exists()) {
      if (!cacheDir.mkdirs()) {
        logger.warn("Failed to create cache directory: {}", cacheDir);
      }
    }

    rebuildIndex();
  }

  /**
   * Scan the cache directory on startup to rebuild the in-memory index.
   */
  private void rebuildIndex() {
    File[] entries = cacheDir.listFiles(File::isDirectory);
    if (entries == null) {
      return;
    }

    int loaded = 0;
    int evicted = 0;
    for (File dir : entries) {
      File metaFile = new File(dir, META_FILE);
      if (!metaFile.exists()) {
        deleteDirectory(dir);
        continue;
      }
      try {
        CacheMeta meta = mapper.readValue(metaFile, CacheMeta.class);
        if (isExpired(meta)) {
          deleteDirectory(dir);
          evicted++;
        } else {
          metadataIndex.put(meta.cacheId, meta);
          if (meta.sqlHash != null) {
            sqlHashIndex.put(meta.sqlHash, meta.cacheId);
          }
          loaded++;
        }
      } catch (IOException e) {
        logger.warn("Failed to read cache metadata: {}", metaFile, e);
        deleteDirectory(dir);
      }
    }
    logger.info("Result cache initialized: {} entries loaded, {} expired entries evicted from {}",
        loaded, evicted, cacheDir);
  }

  /**
   * Look up a cached result by SQL hash.
   * Returns the cacheId if a valid (non-expired) entry exists, null otherwise.
   */
  public String findBySqlHash(String sql, String defaultSchema, String userName) {
    String hash = computeSqlHash(sql, defaultSchema, userName);
    String cacheId = sqlHashIndex.get(hash);
    if (cacheId == null) {
      return null;
    }
    CacheMeta meta = metadataIndex.get(cacheId);
    if (meta == null || isExpired(meta)) {
      evict(cacheId);
      return null;
    }
    // Update last-accessed time
    meta.lastAccessedAt = System.currentTimeMillis();
    try {
      writeMeta(meta);
    } catch (IOException e) {
      logger.debug("Failed to update last-accessed time for cache entry: {}", cacheId, e);
    }
    return cacheId;
  }

  /**
   * Store a query result in the cache.
   *
   * @return the CacheMeta, or null if the result is too large to cache
   */
  public CacheMeta cacheResult(String queryId, String sql, String defaultSchema,
                               String userName, String queryState,
                               Collection<String> columns, List<String> metadata,
                               List<Map<String, String>> rows) throws IOException {
    // Enforce row limit
    List<Map<String, String>> rowsToCache = rows;
    if (rows.size() > maxRows) {
      rowsToCache = rows.subList(0, maxRows);
    }

    String cacheId = UUID.randomUUID().toString();
    String sqlHash = computeSqlHash(sql, defaultSchema, userName);

    CacheMeta meta = new CacheMeta();
    meta.cacheId = cacheId;
    meta.queryId = queryId;
    meta.sqlHash = sqlHash;
    meta.sql = sql;
    meta.defaultSchema = defaultSchema;
    meta.userName = userName;
    meta.queryState = queryState;
    meta.columns = new ArrayList<>(columns);
    meta.metadata = metadata != null ? new ArrayList<>(metadata) : new ArrayList<>();
    meta.totalRows = rowsToCache.size();
    meta.cachedAt = System.currentTimeMillis();
    meta.lastAccessedAt = meta.cachedAt;

    // Create cache directory
    File entryDir = new File(cacheDir, cacheId);
    if (!entryDir.mkdirs()) {
      throw new IOException("Failed to create cache directory: " + entryDir);
    }

    // Write data file first (to check size)
    File dataFile = new File(entryDir, DATA_FILE);
    try (FileOutputStream fos = new FileOutputStream(dataFile);
         JsonGenerator gen = mapper.getFactory().createGenerator(fos)) {
      gen.writeStartArray();
      for (Map<String, String> row : rowsToCache) {
        gen.writeStartObject();
        for (Map.Entry<String, String> entry : row.entrySet()) {
          gen.writeStringField(entry.getKey(), entry.getValue());
        }
        gen.writeEndObject();
      }
      gen.writeEndArray();
    }

    // Check size limit
    long dataSize = dataFile.length();
    if (dataSize > maxResultBytes) {
      logger.info("Result too large to cache ({} bytes > {} max), removing", dataSize, maxResultBytes);
      deleteDirectory(entryDir);
      return null;
    }
    meta.sizeBytes = dataSize;

    // Write metadata
    writeMeta(meta);

    // Update indexes
    metadataIndex.put(cacheId, meta);
    sqlHashIndex.put(sqlHash, cacheId);

    // Evict old entries if total size exceeded
    evictIfOverBudget();

    logger.info("Cached query result: cacheId={}, rows={}, size={} bytes, sql={}",
        cacheId, meta.totalRows, dataSize, truncateSql(sql));

    return meta;
  }

  /**
   * Get cache metadata by cacheId.
   */
  public CacheMeta getMetadata(String cacheId) {
    CacheMeta meta = metadataIndex.get(cacheId);
    if (meta != null && isExpired(meta)) {
      evict(cacheId);
      return null;
    }
    return meta;
  }

  /**
   * Fetch paginated rows from a cached result.
   */
  public PaginatedRows getRows(String cacheId, int offset, int limit) throws IOException {
    CacheMeta meta = getMetadata(cacheId);
    if (meta == null) {
      return null;
    }

    // Update last-accessed time
    meta.lastAccessedAt = System.currentTimeMillis();

    File dataFile = new File(new File(cacheDir, cacheId), DATA_FILE);
    if (!dataFile.exists()) {
      evict(cacheId);
      return null;
    }

    List<Map<String, String>> rows = new ArrayList<>();
    try (FileInputStream fis = new FileInputStream(dataFile);
         JsonParser parser = mapper.getFactory().createParser(fis)) {
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        throw new IOException("Expected JSON array in data file");
      }

      int index = 0;
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        if (index >= offset && index < offset + limit) {
          @SuppressWarnings("unchecked")
          Map<String, String> row = mapper.readValue(parser, Map.class);
          rows.add(row);
        } else {
          parser.skipChildren();
        }
        index++;
        if (index >= offset + limit) {
          break;
        }
      }
    }

    PaginatedRows result = new PaginatedRows();
    result.rows = rows;
    result.offset = offset;
    result.limit = limit;
    result.totalRows = meta.totalRows;
    result.hasMore = offset + limit < meta.totalRows;
    return result;
  }

  /**
   * Stream all rows for a cached result to an output stream in the given format.
   */
  public void streamAllRows(String cacheId, String format, OutputStream out) throws IOException {
    CacheMeta meta = getMetadata(cacheId);
    if (meta == null) {
      throw new IOException("Cache entry not found: " + cacheId);
    }

    File dataFile = new File(new File(cacheDir, cacheId), DATA_FILE);
    if (!dataFile.exists()) {
      throw new IOException("Data file not found for cache entry: " + cacheId);
    }

    if ("csv".equalsIgnoreCase(format)) {
      streamAsCsv(meta, dataFile, out);
    } else {
      // Default: stream raw JSON
      Files.copy(dataFile.toPath(), out);
    }
  }

  private void streamAsCsv(CacheMeta meta, File dataFile, OutputStream out) throws IOException {
    // Write CSV header
    StringBuilder header = new StringBuilder();
    for (int i = 0; i < meta.columns.size(); i++) {
      if (i > 0) {
        header.append(',');
      }
      header.append(escapeCsv(meta.columns.get(i)));
    }
    header.append('\n');
    out.write(header.toString().getBytes(StandardCharsets.UTF_8));

    // Stream rows
    try (FileInputStream fis = new FileInputStream(dataFile);
         JsonParser parser = mapper.getFactory().createParser(fis)) {
      if (parser.nextToken() != JsonToken.START_ARRAY) {
        return;
      }
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        @SuppressWarnings("unchecked")
        Map<String, String> row = mapper.readValue(parser, Map.class);
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < meta.columns.size(); i++) {
          if (i > 0) {
            line.append(',');
          }
          String val = row.get(meta.columns.get(i));
          line.append(escapeCsv(val != null ? val : ""));
        }
        line.append('\n');
        out.write(line.toString().getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  private String escapeCsv(String value) {
    if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  /**
   * Evict a cache entry.
   */
  public boolean evict(String cacheId) {
    CacheMeta meta = metadataIndex.remove(cacheId);
    if (meta != null && meta.sqlHash != null) {
      sqlHashIndex.remove(meta.sqlHash, cacheId);
    }
    File entryDir = new File(cacheDir, cacheId);
    if (entryDir.exists()) {
      deleteDirectory(entryDir);
      logger.debug("Evicted cache entry: {}", cacheId);
      return true;
    }
    return false;
  }

  /**
   * Get cache statistics.
   */
  public CacheStats getStats() {
    CacheStats stats = new CacheStats();
    stats.entryCount = metadataIndex.size();
    stats.totalSizeBytes = metadataIndex.values().stream()
        .mapToLong(m -> m.sizeBytes)
        .sum();
    stats.maxTotalSizeBytes = maxTotalBytes;
    stats.ttlMinutes = ttlMs / 60000;
    stats.cacheDirectory = cacheDir.getAbsolutePath();
    return stats;
  }

  // ==================== Internal Helpers ====================

  private boolean isExpired(CacheMeta meta) {
    return System.currentTimeMillis() - meta.cachedAt > ttlMs;
  }

  private void evictIfOverBudget() {
    long totalSize = metadataIndex.values().stream()
        .mapToLong(m -> m.sizeBytes)
        .sum();

    if (totalSize <= maxTotalBytes) {
      return;
    }

    // Sort by lastAccessedAt (oldest first) for LRU eviction
    List<CacheMeta> entries = new ArrayList<>(metadataIndex.values());
    entries.sort(Comparator.comparingLong(m -> m.lastAccessedAt));

    for (CacheMeta meta : entries) {
      if (totalSize <= maxTotalBytes) {
        break;
      }
      totalSize -= meta.sizeBytes;
      evict(meta.cacheId);
      logger.info("LRU evicted cache entry: {} (lastAccessed={})", meta.cacheId, meta.lastAccessedAt);
    }
  }

  private void writeMeta(CacheMeta meta) throws IOException {
    File entryDir = new File(cacheDir, meta.cacheId);
    File metaFile = new File(entryDir, META_FILE);
    mapper.writerWithDefaultPrettyPrinter().writeValue(metaFile, meta);
  }

  static String computeSqlHash(String sql, String defaultSchema, String userName) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      String normalized = (sql != null ? sql.trim().replaceAll(";\\s*$", "").toLowerCase() : "")
          + "|" + (defaultSchema != null ? defaultSchema : "")
          + "|" + (userName != null ? userName : "");
      byte[] hash = digest.digest(normalized.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (byte b : hash) {
        sb.append(String.format("%02x", b));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }

  private String truncateSql(String sql) {
    if (sql == null) {
      return "";
    }
    return sql.length() > 80 ? sql.substring(0, 80) + "..." : sql;
  }

  private void deleteDirectory(File dir) {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          deleteDirectory(f);
        } else {
          f.delete();
        }
      }
    }
    dir.delete();
  }

  // ==================== Data Classes ====================

  public static class CacheMeta {
    public String cacheId;
    public String queryId;
    public String sqlHash;
    public String sql;
    public String defaultSchema;
    public String userName;
    public String queryState;
    public List<String> columns;
    public List<String> metadata;
    public int totalRows;
    public long sizeBytes;
    public long cachedAt;
    public long lastAccessedAt;

    public CacheMeta() {}
  }

  public static class PaginatedRows {
    public List<Map<String, String>> rows;
    public int offset;
    public int limit;
    public int totalRows;
    public boolean hasMore;
  }

  public static class CacheStats {
    public int entryCount;
    public long totalSizeBytes;
    public long maxTotalSizeBytes;
    public long ttlMinutes;
    public String cacheDirectory;
  }
}
