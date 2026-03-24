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
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.work.WorkManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

  public static class SchemaTreeRequest {
    @JsonProperty
    public List<String> schemas;

    public SchemaTreeRequest() {
    }
  }

  public static class SchemaTreeTable {
    @JsonProperty
    public String name;
    @JsonProperty
    public List<String> columns;

    public SchemaTreeTable(String name, List<String> columns) {
      this.name = name;
      this.columns = columns;
    }
  }

  public static class SchemaTreeEntry {
    @JsonProperty
    public String name;
    @JsonProperty
    public List<SchemaTreeTable> tables;

    public SchemaTreeEntry(String name, List<SchemaTreeTable> tables) {
      this.name = name;
      this.tables = tables;
    }
  }

  public static class SchemaTreeResponse {
    @JsonProperty
    public List<SchemaTreeEntry> schemas;

    public SchemaTreeResponse(List<SchemaTreeEntry> schemas) {
      this.schemas = schemas;
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
      description = "Returns a list of files and folders in the specified schema (for file-based plugins like dfs). "
          + "Only shows directories and files with extensions recognized by the plugin's format configurations.")
  public FilesResponse getFiles(
      @Parameter(description = "Schema name (e.g., dfs.tmp)") @PathParam("schema") String schema,
      @Parameter(description = "Subdirectory path") @QueryParam("path") @DefaultValue("") String subPath) {
    logger.debug("Fetching files for schema: {}, path: {}", schema, subPath);

    List<FileInfo> files = new ArrayList<>();

    // Collect recognized extensions from the plugin's format configs
    Set<String> recognizedExtensions = getRecognizedExtensions(schema);

    // Build a properly-quoted compound schema path:
    //   dfs.tmp + myFolder → dfs.`tmp`.`myFolder`
    String formattedPath;
    if (subPath != null && !subPath.isEmpty()) {
      formattedPath = formatSchemaPath(schema) + ".`" + escapeBackticks(subPath) + "`";
    } else {
      formattedPath = formatSchemaPath(schema);
    }

    // Use SHOW FILES command to list files
    String sql = String.format("SHOW FILES IN %s", formattedPath);

    try {
      QueryResult result = executeQuery(sql);
      for (Map<String, String> row : result.rows) {
        String name = row.get("name");
        if (name == null) {
          continue;
        }

        boolean isDirectory = "true".equalsIgnoreCase(row.get("isDirectory"));
        boolean isFile = "true".equalsIgnoreCase(row.get("isFile"));

        // Filter: always show directories, but only show files with recognized extensions
        if (isFile && !recognizedExtensions.isEmpty() && !hasRecognizedExtension(name, recognizedExtensions)) {
          continue;
        }

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

    try {
      List<ColumnInfo> columns = fetchColumnsForTable(schema, table);
      return new ColumnsResponse(columns);
    } catch (Exception e) {
      logger.error("Error fetching columns for table: {}.{}", schema, table, e);
      throw new RuntimeException("Failed to fetch columns: " + e.getMessage(), e);
    }
  }

  private void populateColumns(QueryResult result, List<ColumnInfo> columns,
      String schema, String table) {
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
  }

  /**
   * Private helper to fetch columns for a specific table.
   * Used by both the REST endpoint and getSchemaTree() to avoid calling
   * REST endpoints from within Java methods.
   */
  private List<ColumnInfo> fetchColumnsForTable(String schema, String table) throws Exception {
    // Handle dot-qualified table names like "store.order_items"
    String effectiveSchema = schema;
    String effectiveTable = table;
    int lastDot = table.lastIndexOf('.');
    if (lastDot > 0) {
      effectiveSchema = schema + "." + table.substring(0, lastDot);
      effectiveTable = table.substring(lastDot + 1);
    }

    String sql = String.format(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE " +
        "FROM INFORMATION_SCHEMA.`COLUMNS` " +
        "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' " +
        "ORDER BY ORDINAL_POSITION",
        escapeQuotes(effectiveSchema), escapeQuotes(effectiveTable));

    List<ColumnInfo> columns = new ArrayList<>();
    QueryResult result = executeQuery(sql);
    populateColumns(result, columns, schema, table);

    // Fallback: if dot-splitting produced no results, retry with original values
    if (columns.isEmpty() && lastDot > 0) {
      String fallbackSql = String.format(
          "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE " +
          "FROM INFORMATION_SCHEMA.`COLUMNS` " +
          "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' " +
          "ORDER BY ORDINAL_POSITION",
          escapeQuotes(schema), escapeQuotes(table));
      QueryResult fallbackResult = executeQuery(fallbackSql);
      populateColumns(fallbackResult, columns, schema, table);
    }

    // Handle dynamic-schema tables (e.g. Splunk, MongoDB)
    if (columns.size() == 1 && "**".equals(columns.get(0).name)) {
      if (isHttpPlugin(schema)) {
        return columns; // Keep the "**" marker for HTTP endpoints
      }
      columns.clear();
      try {
        String probeSql = String.format("SELECT * FROM %s.`%s` LIMIT 1",
            formatSchemaPath(schema), escapeBackticks(table));
        QueryResult probeResult = executeQuery(probeSql);
        List<String> columnNames = new ArrayList<>(probeResult.columns);
        for (int i = 0; i < columnNames.size(); i++) {
          String colName = columnNames.get(i);
          String dataType = "ANY";
          if (probeResult.metadata != null && i < probeResult.metadata.size()) {
            dataType = probeResult.metadata.get(i);
          } else if (!probeResult.rows.isEmpty()) {
            dataType = inferDataType(probeResult.rows.get(0).get(colName));
          }
          columns.add(new ColumnInfo(colName, dataType, true, schema, table));
        }
      } catch (Exception e) {
        logger.warn("Dynamic column probe failed for {}.{}: {}", schema, table, e.getMessage());
        columns.add(new ColumnInfo("**", "ANY", true, schema, table));
      }
    }

    return columns;
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
        "SELECT * FROM %s.`%s` LIMIT %d",
        formatSchemaPath(schema), escapeBackticks(table), safeLimit);

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
    // Plugin name stays unquoted, workspace parts are individually backtick-quoted,
    // and the file path is backtick-quoted. e.g. dfs.`test`.`file.xml`
    String fullPath = formatSchemaPath(schema) + ".`" + escapeBackticks(filePath) + "`";

    String sql = String.format("SELECT * FROM %s LIMIT 1", fullPath);

    List<ColumnInfo> columns = new ArrayList<>();

    try {
      QueryResult result = executeQuery(sql);

      // Use result.metadata for column types when available (preferred),
      // fall back to value-based inference.
      List<String> columnNames = new ArrayList<>(result.columns);
      for (int i = 0; i < columnNames.size(); i++) {
        String columnName = columnNames.get(i);
        String dataType = "ANY";
        if (result.metadata != null && i < result.metadata.size()) {
          dataType = result.metadata.get(i);
        } else if (!result.rows.isEmpty()) {
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

  @POST
  @Path("/schema-tree")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get schema tree",
      description = "Returns schemas with their tables and columns in a single call")
  public SchemaTreeResponse getSchemaTree(SchemaTreeRequest request) {
    List<SchemaTreeEntry> entries = new ArrayList<>();

    if (request.schemas == null || request.schemas.isEmpty()) {
      return new SchemaTreeResponse(entries);
    }

    // For each requested schema, fetch its tables and then columns for each table
    for (String schema : request.schemas) {
      try {
        TablesResponse tablesResp = getTables(schema);
        List<SchemaTreeTable> schemaTables = new ArrayList<>();

        for (TableInfo tableInfo : tablesResp.tables) {
          try {
            // Use private helper instead of REST endpoint to fetch columns
            List<ColumnInfo> columns = fetchColumnsForTable(schema, tableInfo.name);
            List<String> columnNames = new ArrayList<>();
            for (ColumnInfo col : columns) {
              columnNames.add(col.name);
            }
            schemaTables.add(new SchemaTreeTable(tableInfo.name, columnNames));
          } catch (Exception e) {
            logger.warn("Could not fetch columns for {}.{}: {}", schema, tableInfo.name, e.getMessage());
            // Add table with empty columns list on error
            schemaTables.add(new SchemaTreeTable(tableInfo.name, new ArrayList<>()));
          }
        }

        entries.add(new SchemaTreeEntry(schema, schemaTables));
      } catch (Exception e) {
        logger.warn("Could not fetch tables for schema {}: {}", schema, e.getMessage());
      }
    }

    return new SchemaTreeResponse(entries);
  }

  // ==================== Helper Methods ====================

  /**
   * Execute a SQL query and return the results.
   * Clears previous results from the shared WebUserConnection
   * to prevent accumulation across multiple queries in one request.
   */
  private QueryResult executeQuery(String sql) throws Exception {
    webUserConnection.results.clear();
    webUserConnection.columns.clear();
    webUserConnection.metadata.clear();

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

  /**
   * Check whether a schema belongs to an HTTP storage plugin.
   * Uses the config class name to avoid a compile-time dependency on the
   * contrib HTTP module.
   */
  private boolean isHttpPlugin(String schema) {
    try {
      String pluginName = schema.contains(".") ? schema.split("\\.", 2)[0] : schema;
      StoragePlugin plugin = storageRegistry.getPlugin(pluginName);
      if (plugin != null) {
        String configClass = plugin.getConfig().getClass().getSimpleName();
        return configClass.contains("HttpStoragePlugin");
      }
    } catch (Exception e) {
      logger.debug("Could not determine plugin type for schema {}: {}", schema, e.getMessage());
    }
    return false;
  }

  /**
   * Format a compound schema name for SQL queries.
   * Plugin name stays unquoted; workspace parts are individually backtick-quoted.
   * e.g. "dfs.test" → "dfs.`test`", "dfs" → "dfs"
   */
  private String formatSchemaPath(String schema) {
    if (schema == null || !schema.contains(".")) {
      return schema;
    }
    String[] parts = schema.split("\\.", 2);
    String[] workspaceParts = parts[1].split("\\.");
    StringBuilder sb = new StringBuilder(parts[0]);
    for (String wp : workspaceParts) {
      sb.append(".`").append(escapeBackticks(wp)).append("`");
    }
    return sb.toString();
  }

  /**
   * Common compression extensions that Drill can transparently decompress.
   * Files like "data.csv.gz" should be recognized if "csv" is a known extension.
   */
  private static final Set<String> COMPRESSION_EXTENSIONS = new HashSet<>(Arrays.asList(
      "gz", "gzip", "bz2", "xz", "snappy", "sz", "lz4", "zst", "zip"
  ));

  /**
   * Collect all recognized file extensions from the format configurations
   * of the storage plugin associated with the given schema.
   * For example, if the dfs plugin has json, csv, parquet formats configured,
   * this returns {"json", "csv", "parquet", "csvh", ...} etc.
   */
  @SuppressWarnings("unchecked")
  private Set<String> getRecognizedExtensions(String schema) {
    Set<String> extensions = new HashSet<>();
    try {
      // Extract plugin name from schema (e.g., "dfs.tmp" → "dfs")
      String pluginName = schema.contains(".") ? schema.split("\\.", 2)[0] : schema;
      StoragePlugin plugin = storageRegistry.getPlugin(pluginName);
      if (plugin == null) {
        return extensions;
      }

      StoragePluginConfig config = plugin.getConfig();
      if (!(config instanceof FileSystemConfig)) {
        return extensions;
      }

      FileSystemConfig fsConfig = (FileSystemConfig) config;
      Map<String, FormatPluginConfig> formats = fsConfig.getFormats();
      if (formats == null) {
        return extensions;
      }

      for (FormatPluginConfig formatConfig : formats.values()) {
        // Use reflection to call getExtensions() since it's not on the interface
        try {
          Method getExtensions = formatConfig.getClass().getMethod("getExtensions");
          @SuppressWarnings("unchecked")
          List<String> exts = (List<String>) getExtensions.invoke(formatConfig);
          if (exts != null) {
            for (String ext : exts) {
              extensions.add(ext.toLowerCase());
            }
          }
        } catch (NoSuchMethodException e) {
          // Try singular getExtension() for formats like logRegex
          try {
            Method getExtension = formatConfig.getClass().getMethod("getExtension");
            String ext = (String) getExtension.invoke(formatConfig);
            if (ext != null && !ext.isEmpty()) {
              extensions.add(ext.toLowerCase());
            }
          } catch (Exception ignored) {
            // Format doesn't expose extensions at all
          }
        } catch (Exception e) {
          logger.debug("Could not get extensions from format config: {}", formatConfig.getClass().getSimpleName(), e);
        }
      }

      // Always include parquet since it's handled specially
      if (formats.containsKey("parquet")) {
        extensions.add("parquet");
      }
    } catch (Exception e) {
      logger.debug("Could not determine recognized extensions for schema: {}", schema, e);
    }
    return extensions;
  }

  /**
   * Check if a filename has an extension recognized by the plugin's formats.
   * Handles compressed files (e.g., data.csv.gz) by stripping compression suffixes.
   */
  static boolean hasRecognizedExtension(String fileName, Set<String> recognizedExtensions) {
    if (fileName == null || recognizedExtensions.isEmpty()) {
      return true;
    }

    String lower = fileName.toLowerCase();

    // Check direct extension match
    int lastDot = lower.lastIndexOf('.');
    if (lastDot >= 0) {
      String ext = lower.substring(lastDot + 1);
      if (recognizedExtensions.contains(ext)) {
        return true;
      }

      // If the extension is a compression format, check the underlying extension
      if (COMPRESSION_EXTENSIONS.contains(ext)) {
        String stripped = lower.substring(0, lastDot);
        int prevDot = stripped.lastIndexOf('.');
        if (prevDot >= 0) {
          String innerExt = stripped.substring(prevDot + 1);
          if (recognizedExtensions.contains(innerExt)) {
            return true;
          }
        }
      }
    }

    return false;
  }
}
