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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.exec.expr.fn.registry.FunctionHolder;
import org.apache.drill.exec.expr.fn.registry.LocalFunctionRegistry;
import org.apache.drill.exec.server.rest.RestQueryRunner.QueryResult;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * REST API for browsing database metadata (schemas, tables, columns).
 * Used by the SQL Lab frontend for schema exploration and autocomplete.
 */
@Path("/api/v1/metadata")
@Tag(name = "Metadata", description = "Database metadata exploration APIs")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class MetadataResources {
  private static final Logger logger = LoggerFactory.getLogger(MetadataResources.class);

  @Inject
  WorkManager workManager;

  @Inject
  WebUserConnection webUserConnection;

  @Inject
  StoragePluginRegistry storageRegistry;

  // Plugins to completely exclude from the UI
  private static final Set<String> EXCLUDED_PLUGINS = new HashSet<>(Arrays.asList(
      "cp",                 // classpath plugin - internal use only
      "sys",                // system tables - internal use only
      "information_schema"  // INFORMATION_SCHEMA - metadata only
  ));

  // Plugins that appear but cannot enumerate their tables
  private static final Set<String> NON_BROWSABLE_PLUGINS = new HashSet<>(Arrays.asList(
      "http"          // http plugin - shows endpoints but can't enumerate table schema
  ));

  // ==================== Response Models ====================

  public static class PluginInfo {
    @JsonProperty
    public String name;
    @JsonProperty
    public String type;
    @JsonProperty
    public boolean enabled;
    @JsonProperty
    public boolean browsable;

    public PluginInfo() {}

    public PluginInfo(String name, String type, boolean enabled, boolean browsable) {
      this.name = name;
      this.type = type;
      this.enabled = enabled;
      this.browsable = browsable;
    }
  }

  public static class PluginsResponse {
    @JsonProperty
    public List<PluginInfo> plugins;

    public PluginsResponse(List<PluginInfo> plugins) {
      this.plugins = plugins;
    }
  }

  public static class SchemaInfo {
    @JsonProperty
    public String name;
    @JsonProperty
    public String type = "schema";
    @JsonProperty
    public String plugin;
    @JsonProperty
    public boolean browsable = true;

    public SchemaInfo() {}

    public SchemaInfo(String name) {
      this.name = name;
    }

    public SchemaInfo(String name, String plugin, boolean browsable) {
      this.name = name;
      this.plugin = plugin;
      this.browsable = browsable;
    }
  }

  public static class SchemasResponse {
    @JsonProperty
    public List<SchemaInfo> schemas;

    public SchemasResponse(List<SchemaInfo> schemas) {
      this.schemas = schemas;
    }
  }

  public static class TableInfo {
    @JsonProperty
    public String name;
    @JsonProperty
    public String schema;
    @JsonProperty
    public String type;

    public TableInfo() {}

    public TableInfo(String name, String schema, String type) {
      this.name = name;
      this.schema = schema;
      this.type = type;
    }
  }

  public static class TablesResponse {
    @JsonProperty
    public List<TableInfo> tables;

    public TablesResponse(List<TableInfo> tables) {
      this.tables = tables;
    }
  }

  public static class ColumnInfo {
    @JsonProperty
    public String name;
    @JsonProperty
    public String type;
    @JsonProperty
    public boolean nullable;
    @JsonProperty
    public String schema;
    @JsonProperty
    public String table;

    public ColumnInfo() {}

    public ColumnInfo(String name, String type, boolean nullable, String schema, String table) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      this.schema = schema;
      this.table = table;
    }
  }

  public static class ColumnsResponse {
    @JsonProperty
    public List<ColumnInfo> columns;

    public ColumnsResponse(List<ColumnInfo> columns) {
      this.columns = columns;
    }
  }

  public static class TablePreviewResponse {
    @JsonProperty
    public List<String> columns;
    @JsonProperty
    public List<Map<String, String>> rows;

    public TablePreviewResponse(List<String> columns, List<Map<String, String>> rows) {
      this.columns = columns;
      this.rows = rows;
    }
  }

  public static class FunctionsResponse {
    @JsonProperty
    public List<String> functions;

    public FunctionsResponse(List<String> functions) {
      this.functions = functions;
    }
  }

  /**
   * File or folder information from SHOW FILES command.
   */
  public static class FileInfo {
    @JsonProperty
    public String name;
    @JsonProperty
    public boolean isDirectory;
    @JsonProperty
    public boolean isFile;
    @JsonProperty
    public long length;
    @JsonProperty
    public String owner;
    @JsonProperty
    public String group;
    @JsonProperty
    public String permissions;
    @JsonProperty
    public String modificationTime;

    public FileInfo() {}

    public FileInfo(String name, boolean isDirectory, boolean isFile, long length,
                    String owner, String group, String permissions, String modificationTime) {
      this.name = name;
      this.isDirectory = isDirectory;
      this.isFile = isFile;
      this.length = length;
      this.owner = owner;
      this.group = group;
      this.permissions = permissions;
      this.modificationTime = modificationTime;
    }
  }

  public static class FilesResponse {
    @JsonProperty
    public List<FileInfo> files;
    @JsonProperty
    public String path;

    public FilesResponse(List<FileInfo> files, String path) {
      this.files = files;
      this.path = path;
    }
  }

  // ==================== API Endpoints ====================

  @GET
  @Path("/plugins")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List storage plugins", description = "Returns a list of enabled storage plugins")
  public PluginsResponse getPlugins() {
    logger.debug("Fetching storage plugins");

    List<PluginInfo> plugins = new ArrayList<>();

    try {
      // Get all enabled storage plugins
      Map<String, StoragePluginConfig> enabledPlugins = storageRegistry.enabledConfigs();
      for (Map.Entry<String, StoragePluginConfig> entry : enabledPlugins.entrySet()) {
        String pluginName = entry.getKey();

        // Skip excluded plugins (cp, sys, information_schema)
        if (EXCLUDED_PLUGINS.contains(pluginName.toLowerCase())) {
          continue;
        }

        StoragePluginConfig pluginConfig = entry.getValue();
        String pluginType = pluginConfig != null ? pluginConfig.getClass().getSimpleName() : "unknown";
        // Remove "Config" suffix if present for cleaner display
        if (pluginType.endsWith("Config")) {
          pluginType = pluginType.substring(0, pluginType.length() - 6);
        }
        boolean isBrowsable = !NON_BROWSABLE_PLUGINS.contains(pluginName.toLowerCase());
        plugins.add(new PluginInfo(pluginName, pluginType.toLowerCase(), true, isBrowsable));
      }
    } catch (Exception e) {
      logger.error("Error fetching plugins", e);
      throw new RuntimeException("Failed to fetch plugins: " + e.getMessage(), e);
    }

    return new PluginsResponse(plugins);
  }

  @GET
  @Path("/plugins/{plugin}/schemas")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List schemas for a plugin", description = "Returns a list of schemas for the specified plugin")
  public SchemasResponse getPluginSchemas(
      @Parameter(description = "Plugin name") @PathParam("plugin") String plugin) {
    logger.debug("Fetching schemas for plugin: {}", plugin);

    List<SchemaInfo> schemas = new ArrayList<>();
    boolean isBrowsable = !NON_BROWSABLE_PLUGINS.contains(plugin.toLowerCase());

    // For non-browsable plugins, just return the plugin name as the only schema
    if (!isBrowsable) {
      schemas.add(new SchemaInfo(plugin, plugin, false));
      return new SchemasResponse(schemas);
    }

    // Try to get sub-schemas from INFORMATION_SCHEMA
    String sql = String.format(
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA " +
        "WHERE SCHEMA_NAME = '%s' OR SCHEMA_NAME LIKE '%s.%%' " +
        "ORDER BY SCHEMA_NAME",
        escapeQuotes(plugin), escapeQuotes(plugin));

    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String schemaName = row.get("SCHEMA_NAME");
        if (schemaName != null) {
          schemas.add(new SchemaInfo(schemaName, plugin, isBrowsable));
        }
      }
    } catch (Exception e) {
      logger.warn("Error fetching schemas for plugin: {}, returning plugin as sole schema", plugin, e);
      // Return at least the plugin name as a schema
      schemas.add(new SchemaInfo(plugin, plugin, isBrowsable));
    }

    // If no schemas found, return the plugin name itself
    if (schemas.isEmpty()) {
      schemas.add(new SchemaInfo(plugin, plugin, isBrowsable));
    }

    return new SchemasResponse(schemas);
  }

  @GET
  @Path("/schemas")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List all schemas", description = "Returns a list of all available schemas/databases")
  public SchemasResponse getSchemas() {
    logger.debug("Fetching all schemas");

    List<SchemaInfo> schemas = new ArrayList<>();

    // First, add schemas from enabled storage plugins (fast, no connection needed)
    try {
      Map<String, StoragePluginConfig> enabledPlugins = storageRegistry.enabledConfigs();
      for (String pluginName : enabledPlugins.keySet()) {
        // Skip excluded plugins
        if (EXCLUDED_PLUGINS.contains(pluginName.toLowerCase())) {
          continue;
        }
        boolean isBrowsable = !NON_BROWSABLE_PLUGINS.contains(pluginName.toLowerCase());
        // Add the root plugin as a schema
        schemas.add(new SchemaInfo(pluginName, pluginName, isBrowsable));
      }
    } catch (Exception e) {
      logger.warn("Error fetching plugins for schema list, falling back to INFORMATION_SCHEMA query", e);
    }

    // If we got schemas from plugins, return them; otherwise fall back to SQL query
    if (!schemas.isEmpty()) {
      return new SchemasResponse(schemas);
    }

    // Fallback: try INFORMATION_SCHEMA query (may fail if plugins have connection issues)
    String sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME";
    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String schemaName = row.get("SCHEMA_NAME");
        if (schemaName != null) {
          schemas.add(new SchemaInfo(schemaName));
        }
      }
    } catch (Exception e) {
      logger.error("Error fetching schemas from INFORMATION_SCHEMA", e);
      // Return empty list rather than failing completely
      return new SchemasResponse(schemas);
    }

    return new SchemasResponse(schemas);
  }

  @GET
  @Path("/schemas/{schema}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List tables in schema", description = "Returns a list of tables in the specified schema")
  public TablesResponse getTables(
      @Parameter(description = "Schema name") @PathParam("schema") String schema) {
    logger.debug("Fetching tables for schema: {}", schema);

    String sql = String.format(
        "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.`TABLES` " +
        "WHERE TABLE_SCHEMA = '%s' ORDER BY TABLE_NAME",
        escapeQuotes(schema));

    List<TableInfo> tables = new ArrayList<>();

    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String tableName = row.get("TABLE_NAME");
        String tableType = row.get("TABLE_TYPE");
        if (tableName != null) {
          tables.add(new TableInfo(tableName, schema, tableType != null ? tableType : "TABLE"));
        }
      }
    } catch (Exception e) {
      logger.error("Error fetching tables for schema: {}", schema, e);
      throw new RuntimeException("Failed to fetch tables: " + e.getMessage(), e);
    }

    return new TablesResponse(tables);
  }

  @GET
  @Path("/schemas/{schema}/files")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List files in schema/workspace",
      description = "Returns a list of files and folders in the specified schema (for file-based plugins like dfs)")
  public FilesResponse getFiles(
      @Parameter(description = "Schema name (e.g., dfs.tmp)") @PathParam("schema") String schema,
      @Parameter(description = "Subdirectory path") @QueryParam("path") @DefaultValue("") String subPath) {
    logger.debug("Fetching files for schema: {}, path: {}", schema, subPath);

    List<FileInfo> files = new ArrayList<>();
    String fullPath = schema;
    if (subPath != null && !subPath.isEmpty()) {
      fullPath = schema + ".`" + subPath + "`";
    }

    // Use SHOW FILES command to list files
    String sql = String.format("SHOW FILES IN `%s`", escapeBackticks(fullPath));

    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String name = row.get("name");
        if (name == null) {
          continue;
        }

        boolean isDirectory = "true".equalsIgnoreCase(row.get("isDirectory"));
        boolean isFile = "true".equalsIgnoreCase(row.get("isFile"));
        long length = 0;
        try {
          String lenStr = row.get("length");
          if (lenStr != null) {
            length = Long.parseLong(lenStr);
          }
        } catch (NumberFormatException e) {
          // ignore
        }

        files.add(new FileInfo(
            name,
            isDirectory,
            isFile,
            length,
            row.get("owner"),
            row.get("group"),
            row.get("permissions"),
            row.get("modificationTime")
        ));
      }
    } catch (Exception e) {
      logger.warn("Error fetching files for schema: {} - this may not be a file-based plugin", schema, e);
      // Return empty list for non-file plugins rather than throwing
      return new FilesResponse(files, subPath);
    }

    return new FilesResponse(files, subPath);
  }

  @GET
  @Path("/schemas/{schema}/tables/{table}/columns")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List columns in table", description = "Returns a list of columns in the specified table")
  public ColumnsResponse getColumns(
      @Parameter(description = "Schema name") @PathParam("schema") String schema,
      @Parameter(description = "Table name") @PathParam("table") String table) {
    logger.debug("Fetching columns for table: {}.{}", schema, table);

    String sql = String.format(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE " +
        "FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' " +
        "ORDER BY ORDINAL_POSITION",
        escapeQuotes(schema), escapeQuotes(table));

    List<ColumnInfo> columns = new ArrayList<>();

    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String columnName = row.get("COLUMN_NAME");
        String dataType = row.get("DATA_TYPE");
        String isNullable = row.get("IS_NULLABLE");
        if (columnName != null) {
          columns.add(new ColumnInfo(
              columnName,
              dataType != null ? dataType : "ANY",
              "YES".equalsIgnoreCase(isNullable),
              schema,
              table
          ));
        }
      }
    } catch (Exception e) {
      logger.error("Error fetching columns for table: {}.{}", schema, table, e);
      throw new RuntimeException("Failed to fetch columns: " + e.getMessage(), e);
    }

    return new ColumnsResponse(columns);
  }

  @GET
  @Path("/schemas/{schema}/tables/{table}/preview")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Preview table data", description = "Returns a preview of data from the specified table")
  public TablePreviewResponse previewTable(
      @Parameter(description = "Schema name") @PathParam("schema") String schema,
      @Parameter(description = "Table name") @PathParam("table") String table,
      @Parameter(description = "Maximum rows to return") @QueryParam("limit") @DefaultValue("100") int limit) {
    logger.debug("Previewing table: {}.{} with limit {}", schema, table, limit);

    // Cap the limit to prevent excessive data retrieval
    int safeLimit = Math.min(Math.max(1, limit), 1000);

    String sql = String.format(
        "SELECT * FROM `%s`.`%s` LIMIT %d",
        escapeBackticks(schema), escapeBackticks(table), safeLimit);

    try {
      QueryResult result = executeQuery(sql);
      return new TablePreviewResponse(new ArrayList<>(result.columns), result.rows);
    } catch (Exception e) {
      logger.error("Error previewing table: {}.{}", schema, table, e);
      throw new RuntimeException("Failed to preview table: " + e.getMessage(), e);
    }
  }

  @GET
  @Path("/schemas/{schema}/files/columns")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get columns from a file",
      description = "Returns column names and types by executing SELECT * LIMIT 1 on the file")
  public ColumnsResponse getFileColumns(
      @Parameter(description = "Schema name (e.g., dfs.tmp)") @PathParam("schema") String schema,
      @Parameter(description = "File path within the schema") @QueryParam("path") String filePath) {
    logger.debug("Fetching columns for file: {}/{}", schema, filePath);

    if (filePath == null || filePath.isEmpty()) {
      throw new IllegalArgumentException("File path is required");
    }

    // Build the fully qualified path
    // Handle paths that may contain special characters
    String fullPath = String.format("`%s`.`%s`",
        escapeBackticks(schema), escapeBackticks(filePath));

    String sql = String.format("SELECT * FROM %s LIMIT 1", fullPath);

    List<ColumnInfo> columns = new ArrayList<>();

    try {
      QueryResult result = executeQuery(sql);

      // Get column names and infer types from the first row of results
      for (String columnName : result.columns) {
        // Try to infer type from first row value
        String dataType = "ANY";
        if (!result.rows.isEmpty()) {
          String value = result.rows.get(0).get(columnName);
          dataType = inferDataType(value);
        }
        columns.add(new ColumnInfo(columnName, dataType, true, schema, filePath));
      }
    } catch (Exception e) {
      logger.error("Error fetching columns for file: {}/{}", schema, filePath, e);
      throw new RuntimeException("Failed to get file columns: " + e.getMessage(), e);
    }

    return new ColumnsResponse(columns);
  }

  /**
   * Infer the data type from a string value.
   * This is a simple heuristic for display purposes.
   */
  private String inferDataType(String value) {
    if (value == null) {
      return "ANY";
    }

    // Try integer
    try {
      Long.parseLong(value);
      return "BIGINT";
    } catch (NumberFormatException e) {
      // not an integer
    }

    // Try floating point
    try {
      Double.parseDouble(value);
      return "DOUBLE";
    } catch (NumberFormatException e) {
      // not a number
    }

    // Check for boolean
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return "BOOLEAN";
    }

    // Check for date/time patterns
    if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return "DATE";
    }
    if (value.matches("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}.*")) {
      return "TIMESTAMP";
    }

    // Default to VARCHAR
    return "VARCHAR";
  }

  @GET
  @Path("/functions")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "List SQL functions", description = "Returns a list of available SQL functions for autocomplete")
  public FunctionsResponse getFunctions() {
    logger.debug("Fetching SQL functions");

    TreeSet<String> functionSet = new TreeSet<>();

    try {
      // Get built-in functions from the function registry
      List<FunctionHolder> builtInFunctions = workManager.getContext()
          .getFunctionImplementationRegistry()
          .getLocalFunctionRegistry()
          .getAllJarsWithFunctionsHolders()
          .get(LocalFunctionRegistry.BUILT_IN);

      if (builtInFunctions != null) {
        for (FunctionHolder holder : builtInFunctions) {
          String name = holder.getName();
          // Only include functions that start with a letter and don't contain spaces
          if (name != null && !name.contains(" ") && name.matches("([a-z]|[A-Z])\\w+")
              && !holder.getHolder().isInternal()) {
            functionSet.add(name);
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error fetching functions", e);
      // Return empty list on error rather than failing
    }

    return new FunctionsResponse(new ArrayList<>(functionSet));
  }

  // ==================== Helper Methods ====================

  /**
   * Execute a SQL query and return the results
   */
  private QueryResult executeQuery(String sql) throws Exception {
    QueryWrapper wrapper = new QueryWrapper.RestQueryBuilder()
        .query(sql)
        .queryType("SQL")
        .rowLimit("10000") // Reasonable limit for metadata queries
        .build();

    return new RestQueryRunner(wrapper, workManager, webUserConnection).run();
  }

  /**
   * Escape single quotes in SQL strings
   */
  private String escapeQuotes(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("'", "''");
  }

  /**
   * Escape backticks in SQL identifiers
   */
  private String escapeBackticks(String value) {
    if (value == null) {
      return "";
    }
    return value.replace("`", "``");
  }
}
