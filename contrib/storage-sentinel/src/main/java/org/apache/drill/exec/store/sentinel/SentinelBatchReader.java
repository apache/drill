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

package org.apache.drill.exec.store.sentinel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Cache;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.sentinel.auth.SentinelTokenManager;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SentinelBatchReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(SentinelBatchReader.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json");

  private final SentinelStoragePluginConfig config;
  private final SentinelScanSpec scanSpec;
  private final SentinelTokenManager tokenManager;
  private final String username;
  private final OkHttpClient httpClient;

  private JsonNode responseData;
  private List<ColumnMetadata> columnMetadata;
  private List<List<Object>> rows;
  private int currentRowIndex;
  private RowSetLoader rowWriter;
  private List<ScalarWriter> columnWriters;

  private static class ColumnMetadata {
    String name;
    String kqlType;
    MinorType drillType;

    ColumnMetadata(String name, String kqlType) {
      this.name = name;
      this.kqlType = kqlType;
      this.drillType = mapKqlTypeToDrill(kqlType);
    }
  }

  public SentinelBatchReader(SentinelStoragePluginConfig config, SentinelScanSpec scanSpec,
                           SentinelTokenManager tokenManager, String username) {
    this.config = config;
    this.scanSpec = scanSpec;
    this.tokenManager = tokenManager;
    this.username = username;

    OkHttpClient.Builder builder = new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS);

    if (config.cacheResults()) {
      setupCache(builder);
    }

    this.httpClient = builder.build();
    this.rows = new ArrayList<>();
    this.currentRowIndex = 0;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    try {
      queryLogAnalytics();

      if (columnMetadata == null || columnMetadata.isEmpty()) {
        throw UserException.dataReadError()
            .message("No columns returned from query")
            .build(logger);
      }

      SchemaBuilder schemaBuilder = new SchemaBuilder();
      for (ColumnMetadata col : columnMetadata) {
        schemaBuilder.add(col.name, col.drillType);
      }
      TupleMetadata schema = schemaBuilder.build();

      negotiator.tableSchema(schema, false);
      ResultSetLoader resultSetLoader = negotiator.build();
      this.rowWriter = resultSetLoader.writer();
      buildColumnWriters();

      return true;
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message("Error querying Log Analytics: %s", e.getMessage())
          .build(logger);
    }
  }

  @Override
  public boolean next() {
    if (currentRowIndex >= rows.size()) {
      return false;
    }

    List<Object> row = rows.get(currentRowIndex++);
    rowWriter.start();

    for (int i = 0; i < columnWriters.size() && i < row.size(); i++) {
      Object value = row.get(i);
      ScalarWriter writer = columnWriters.get(i);

      if (value == null) {
        writer.setNull();
      } else {
        writeValue(writer, value, columnMetadata.get(i).drillType);
      }
    }

    rowWriter.save();
    return true;
  }

  @Override
  public void close() {
    httpClient.dispatcher().executorService().shutdown();
  }

  private void queryLogAnalytics() throws IOException {
    String queryUrl = String.format(
        "%s/workspaces/%s/query",
        config.getApiEndpoint(),
        config.getWorkspaceId());

    String token = tokenManager.getBearerToken(username);

    String requestBody = String.format(
        "{\"query\": \"%s\", \"timespan\": \"%s\"}",
        escapeJson(scanSpec.getKqlQuery()),
        config.getDefaultTimespan());

    Request request = new Request.Builder()
        .url(queryUrl)
        .post(RequestBody.create(requestBody, JSON_MEDIA_TYPE))
        .addHeader("Authorization", token)
        .build();

    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw UserException.dataReadError()
            .message("Log Analytics query failed: HTTP %d - %s",
                response.code(),
                response.body() != null ? response.body().string() : "")
            .build(logger);
      }

      String responseBody = response.body().string();
      parseResponse(responseBody);
    }
  }

  private void parseResponse(String jsonResponse) throws IOException {
    JsonNode root = mapper.readTree(jsonResponse);
    JsonNode tables = root.get("tables");

    if (tables == null || !tables.isArray() || tables.size() == 0) {
      throw UserException.dataReadError()
          .message("Invalid Log Analytics response: no tables")
          .build(logger);
    }

    JsonNode table = tables.get(0);
    JsonNode columns = table.get("columns");
    JsonNode rowsArray = table.get("rows");

    if (columns == null || rowsArray == null) {
      throw UserException.dataReadError()
          .message("Invalid Log Analytics response: missing columns or rows")
          .build(logger);
    }

    columnMetadata = new ArrayList<>();
    for (JsonNode col : columns) {
      String name = col.get("name").asText();
      String type = col.get("type").asText();
      columnMetadata.add(new ColumnMetadata(name, type));
    }

    rows = new ArrayList<>();
    for (JsonNode rowNode : rowsArray) {
      List<Object> row = new ArrayList<>();
      if (rowNode.isArray()) {
        for (JsonNode cell : rowNode) {
          row.add(cell.isNull() ? null : cell.asText());
        }
      }
      rows.add(row);
    }

    logger.debug("Parsed {} rows with {} columns", rows.size(), columnMetadata.size());
  }

  private void buildColumnWriters() {
    columnWriters = new ArrayList<>();
    for (ColumnMetadata col : columnMetadata) {
      columnWriters.add(rowWriter.scalar(col.name));
    }
  }

  private void writeValue(ScalarWriter writer, Object value, MinorType drillType) {
    try {
      String strValue = value.toString();

      switch (drillType) {
        case VARCHAR:
          writer.setString(strValue);
          break;
        case BIGINT:
          writer.setLong(Long.parseLong(strValue.trim()));
          break;
        case FLOAT8:
          writer.setDouble(Double.parseDouble(strValue));
          break;
        case BIT:
          writer.setBoolean("true".equalsIgnoreCase(strValue) || "1".equals(strValue));
          break;
        case TIMESTAMP:
          long epochSeconds = Long.parseLong(strValue);
          writer.setTimestamp(Instant.ofEpochSecond(epochSeconds));
          break;
        default:
          writer.setString(strValue);
      }
    } catch (NumberFormatException e) {
      logger.warn("Error parsing value '{}' as type {}", value, drillType);
      writer.setNull();
    }
  }

  private static MinorType mapKqlTypeToDrill(String kqlType) {
    switch (kqlType.toLowerCase()) {
      case "int":
      case "long":
        return MinorType.BIGINT;
      case "real":
      case "decimal":
        return MinorType.FLOAT8;
      case "bool":
        return MinorType.BIT;
      case "datetime":
        return MinorType.TIMESTAMP;
      case "string":
      case "guid":
      case "timespan":
      case "dynamic":
      default:
        return MinorType.VARCHAR;
    }
  }

  private String escapeJson(String str) {
    if (str == null) {
      return "";
    }
    return str.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }

  private void setupCache(OkHttpClient.Builder builder) {
    int cacheSize = 10 * 1024 * 1024;
    String tempDir = System.getProperty("java.io.tmpdir");
    File cacheDirectory = new File(tempDir, "sentinel-cache");
    if (!cacheDirectory.exists()) {
      if (!cacheDirectory.mkdirs()) {
        throw UserException.dataWriteError()
            .message("Could not create the Sentinel query cache directory")
            .addContext("Path", cacheDirectory.getAbsolutePath())
            .build(logger);
      }
    }

    try {
      Cache cache = new Cache(cacheDirectory, cacheSize);
      logger.debug("Caching Sentinel query results at: {}", cacheDirectory);
      builder.cache(cache);
    } catch (Exception e) {
      throw UserException.dataWriteError(e)
          .message("Error setting up Sentinel query cache")
          .build(logger);
    }
  }
}
