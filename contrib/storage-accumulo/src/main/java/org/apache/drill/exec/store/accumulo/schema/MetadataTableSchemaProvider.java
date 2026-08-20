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
package org.apache.drill.exec.store.accumulo.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Schema provider that reads table schema from an Accumulo metadata table.
 *
 * <p>The metadata table stores schema information in the following format:</p>
 * <pre>
 * Row Key: {table_name}
 * Column Family: "schema"
 * Column Qualifiers:
 *   - "row_key_type": The type of the row key (e.g., "VARCHAR", "VARBINARY")
 *   - "columns": JSON array of column definitions
 *
 * Example:
 *   Row: "users"
 *   schema:row_key_type = "VARCHAR"
 *   schema:columns = [
 *     {"name":"name","columnFamily":"cf1","columnQualifier":"name","type":"VARCHAR","nullable":true},
 *     {"name":"age","columnFamily":"cf1","columnQualifier":"age","type":"INT","nullable":true}
 *   ]
 * </pre>
 *
 * <p>This provider includes a configurable cache to reduce Accumulo metadata table lookups.</p>
 */
public class MetadataTableSchemaProvider implements AccumuloSchemaProvider {
  private static final Logger logger = LoggerFactory.getLogger(MetadataTableSchemaProvider.class);

  private static final String SCHEMA_COLUMN_FAMILY = "schema";
  private static final String ROW_KEY_TYPE_QUALIFIER = "row_key_type";
  private static final String COLUMNS_QUALIFIER = "columns";

  private static final long DEFAULT_CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(5);

  private final String metadataTableName;
  private final ObjectMapper objectMapper;
  private final Map<String, CachedSchema> schemaCache;
  private final long cacheTtlMs;

  /**
   * Creates a new MetadataTableSchemaProvider.
   *
   * @param metadataTableName the name of the Accumulo table storing schema metadata
   */
  public MetadataTableSchemaProvider(String metadataTableName) {
    this(metadataTableName, DEFAULT_CACHE_TTL_MS);
  }

  /**
   * Creates a new MetadataTableSchemaProvider with a custom cache TTL.
   *
   * @param metadataTableName the name of the Accumulo table storing schema metadata
   * @param cacheTtlMs cache time-to-live in milliseconds
   */
  public MetadataTableSchemaProvider(String metadataTableName, long cacheTtlMs) {
    this.metadataTableName = metadataTableName;
    this.objectMapper = new ObjectMapper();
    this.schemaCache = new ConcurrentHashMap<>();
    this.cacheTtlMs = cacheTtlMs;
  }

  @Override
  public TableSchema getTableSchema(AccumuloClient client, String tableName) {
    // Check cache first
    CachedSchema cached = schemaCache.get(tableName);
    if (cached != null && !cached.isExpired()) {
      logger.debug("Returning cached schema for table: {}", tableName);
      return cached.schema;
    }

    // Try to load from metadata table
    TableSchema schema = loadSchemaFromMetadata(client, tableName);

    // Cache the result (even if dynamic)
    schemaCache.put(tableName, new CachedSchema(schema));

    return schema;
  }

  @Override
  public Set<String> discoverTableNames(AccumuloClient client) {
    Set<String> tableNames = new HashSet<>();
    try {
      for (String name : client.tableOperations().list()) {
        // Filter out system tables and the metadata table itself
        if (!name.startsWith("accumulo.") && !name.equals(metadataTableName)) {
          tableNames.add(name);
        }
      }
    } catch (Exception e) {
      logger.warn("Failed to discover table names", e);
    }
    return tableNames;
  }

  @Override
  public boolean hasSchema(AccumuloClient client, String tableName) {
    // Check cache first
    CachedSchema cached = schemaCache.get(tableName);
    if (cached != null && !cached.isExpired()) {
      return cached.schema.hasExplicitColumns();
    }

    // Check metadata table
    if (!metadataTableExists(client)) {
      return false;
    }

    try (Scanner scanner = client.createScanner(metadataTableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(tableName));
      scanner.fetchColumn(new Text(SCHEMA_COLUMN_FAMILY), new Text(COLUMNS_QUALIFIER));
      return scanner.iterator().hasNext();
    } catch (TableNotFoundException e) {
      return false;
    }
  }

  @Override
  public void clearCache() {
    schemaCache.clear();
    logger.debug("Cleared all cached schemas");
  }

  @Override
  public void clearCache(String tableName) {
    schemaCache.remove(tableName);
    logger.debug("Cleared cached schema for table: {}", tableName);
  }

  /**
   * Loads schema from the metadata table.
   */
  private TableSchema loadSchemaFromMetadata(AccumuloClient client, String tableName) {
    if (!metadataTableExists(client)) {
      logger.debug("Metadata table '{}' does not exist, using dynamic schema for: {}",
          metadataTableName, tableName);
      return TableSchema.dynamic(tableName);
    }

    try (Scanner scanner = client.createScanner(metadataTableName, Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(tableName));
      scanner.fetchColumnFamily(new Text(SCHEMA_COLUMN_FAMILY));

      AccumuloColumnType rowKeyType = AccumuloColumnType.VARBINARY;
      List<ColumnDef> columns = null;

      for (Map.Entry<Key, Value> entry : scanner) {
        String qualifier = entry.getKey().getColumnQualifier().toString();
        String value = entry.getValue().toString();

        if (ROW_KEY_TYPE_QUALIFIER.equals(qualifier)) {
          rowKeyType = AccumuloColumnType.fromString(value);
        } else if (COLUMNS_QUALIFIER.equals(qualifier)) {
          columns = parseColumnsJson(value);
        }
      }

      if (columns == null || columns.isEmpty()) {
        logger.debug("No explicit schema found for table: {}, using dynamic schema", tableName);
        return TableSchema.dynamic(tableName);
      }

      logger.debug("Loaded schema for table '{}' with {} columns", tableName, columns.size());
      return new TableSchema(tableName, rowKeyType, columns);

    } catch (TableNotFoundException e) {
      logger.debug("Metadata table not found, using dynamic schema for: {}", tableName);
      return TableSchema.dynamic(tableName);
    } catch (Exception e) {
      logger.warn("Error loading schema for table '{}', using dynamic schema", tableName, e);
      return TableSchema.dynamic(tableName);
    }
  }

  /**
   * Parses the JSON column definitions.
   */
  private List<ColumnDef> parseColumnsJson(String json) {
    try {
      return objectMapper.readValue(json, new TypeReference<List<ColumnDef>>() {});
    } catch (Exception e) {
      logger.warn("Failed to parse column definitions JSON: {}", e.getMessage());
      return new ArrayList<>();
    }
  }

  /**
   * Checks if the metadata table exists.
   */
  private boolean metadataTableExists(AccumuloClient client) {
    return client.tableOperations().exists(metadataTableName);
  }

  /**
   * Returns the name of the metadata table.
   */
  public String getMetadataTableName() {
    return metadataTableName;
  }

  /**
   * Cached schema entry with expiration tracking.
   */
  private class CachedSchema {
    final TableSchema schema;
    final long timestamp;

    CachedSchema(TableSchema schema) {
      this.schema = schema;
      this.timestamp = System.currentTimeMillis();
    }

    boolean isExpired() {
      return System.currentTimeMillis() - timestamp > cacheTtlMs;
    }
  }

  /**
   * Writes schema metadata for a table to the metadata table.
   *
   * <p>This is a utility method for setting up schema metadata.
   * It creates the metadata table if it doesn't exist.</p>
   *
   * @param client the Accumulo client
   * @param schema the table schema to write
   * @throws Exception if the write fails
   */
  public void writeSchema(AccumuloClient client, TableSchema schema) throws Exception {
    // Create metadata table if it doesn't exist
    if (!client.tableOperations().exists(metadataTableName)) {
      client.tableOperations().create(metadataTableName);
      logger.info("Created metadata table: {}", metadataTableName);
    }

    // Write schema to metadata table
    try (var writer = client.createBatchWriter(metadataTableName)) {
      org.apache.accumulo.core.data.Mutation mutation =
          new org.apache.accumulo.core.data.Mutation(schema.getTableName());

      // Write row key type
      mutation.put(SCHEMA_COLUMN_FAMILY, ROW_KEY_TYPE_QUALIFIER,
          schema.getRowKeyType().name());

      // Write columns as JSON
      String columnsJson = objectMapper.writeValueAsString(schema.getColumns());
      mutation.put(SCHEMA_COLUMN_FAMILY, COLUMNS_QUALIFIER, columnsJson);

      writer.addMutation(mutation);
    }

    // Clear cache for this table
    clearCache(schema.getTableName());
    logger.info("Wrote schema for table: {}", schema.getTableName());
  }
}
