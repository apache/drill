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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gtkcyber.sqlglot.SqlGlot;
import com.gtkcyber.sqlglot.dialect.Dialect;
import com.gtkcyber.sqlglot.expressions.Expression;
import com.gtkcyber.sqlglot.expressions.Nodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Singleton service that provides SQL dialect transpilation using Java sqlglot.
 * Thread-safe: each call creates independent AST objects with no shared mutable state.
 *
 * <p>Provides SQL transpilation, formatting, data type conversion, and time grain
 * manipulation using the pure Java sqlglot library.</p>
 */
public final class SqlTranspiler {

  private static final Logger logger = LoggerFactory.getLogger(SqlTranspiler.class);
  private static final SqlTranspiler INSTANCE = new SqlTranspiler();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Dialect DRILL = Dialect.of("DRILL");

  /**
   * Mapping of characters that are illegal in SQL identifiers to token placeholders.
   */
  private static final Map<String, String> CHAR_MAPPING;

  static {
    Map<String, String> map = new HashMap<>();
    map.put(" ", "__SPACE__");
    map.put("-", "__DASH__");
    map.put(",", "__COMMA__");
    map.put(":", "__COLON__");
    map.put(";", "__SEMICOLON__");
    map.put("(", "__RPAREN__");
    map.put(")", "__LPAREN__");
    map.put("1", "__ONE__");
    map.put("2", "__TWO__");
    map.put("3", "__THREE__");
    map.put("4", "__FOUR__");
    map.put("5", "__FIVE__");
    map.put("6", "__SIX__");
    map.put("7", "__SEVEN__");
    map.put("8", "__EIGHT__");
    map.put("9", "__NINE__");
    map.put("0", "__ZERO__");
    CHAR_MAPPING = Collections.unmodifiableMap(map);
  }

  /**
   * Known file extensions in Drill (used for table name resolution).
   */
  private static final Set<String> EXTENSIONS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "3g2", "3gp", "accdb", "mdb", "mdx", "ai", "arw",
      "avi", "avro", "bmp", "cr2", "crw", "csv", "csvh",
      "dng", "eps", "epsf", "epsi", "gif", "h5", "httpd",
      "ico", "jpe", "jpeg", "jpg", "json", "log", "ltsv",
      "m4a", "m4b", "m4p", "m4r", "m4v", "mov", "mp4",
      "nef", "orf", "parquet", "pcap", "pcapng", "pcx",
      "pdf", "png", "psd", "raf", "rw2", "rwl", "sav",
      "sas7bdat", "seq", "shp", "srw", "syslog", "tbl",
      "tif", "tiff", "tsv", "wav", "wave", "webp", "x3f",
      "xlsx", "xml", "zip"
  )));

  /**
   * Returns the singleton instance.
   */
  public static SqlTranspiler getInstance() {
    return INSTANCE;
  }

  private SqlTranspiler() {
    logger.info("SqlTranspiler initialized with Java sqlglot");
  }

  /**
   * Transpile SQL from one dialect to another (without schema metadata).
   */
  public String transpile(String sql, String sourceDialect, String targetDialect) {
    return transpile(sql, sourceDialect, targetDialect, "[]");
  }

  /**
   * Transpile SQL from one dialect to another.
   *
   * @param sql the SQL string to transpile
   * @param sourceDialect the source dialect (e.g. "mysql")
   * @param targetDialect the target dialect (e.g. "drill")
   * @param schemasJson JSON string of schema metadata for table name resolution
   * @return the transpiled SQL, or the original SQL if transpilation fails
   */
  public String transpile(String sql, String sourceDialect,
      String targetDialect, String schemasJson) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    try {
      String src = sourceDialect != null ? sourceDialect.toUpperCase() : "ANSI";
      String tgt = targetDialect != null ? targetDialect.toUpperCase() : "DRILL";
      String result = SqlGlot.transpile(sql, src, tgt);

      if (schemasJson != null && !schemasJson.equals("[]") && !schemasJson.isEmpty()) {
        result = ensureFullTableNames(result, schemasJson);
      }

      return result;
    } catch (Exception e) {
      logger.debug("SQL transpilation failed, returning original SQL: {}", e.getMessage());
      return sql;
    }
  }

  /**
   * Convert a column's data type in a SQL query using sqlglot AST manipulation.
   *
   * @param sql the SQL query
   * @param columnName the column to convert
   * @param dataType the target SQL data type (e.g. "INTEGER", "VARCHAR")
   * @param columnsJson optional JSON object mapping column names to types (for star queries)
   * @return the transformed SQL, or null if conversion fails
   */
  public String convertDataType(String sql, String columnName,
      String dataType, String columnsJson) {
    try {
      Optional<Expression> parsed = DRILL.parseOne(sql);
      if (!parsed.isPresent()) {
        return null;
      }
      Expression query = parsed.get();

      // Handle star queries by expanding to explicit columns
      if (isStarQuery(query)) {
        if (columnsJson == null || columnsJson.isEmpty()) {
          return sql;
        }
        Map<String, Object> columnMap = MAPPER.readValue(columnsJson,
            new TypeReference<Map<String, Object>>() { });
        query = replaceStarWithColumns(query, columnMap);
      }

      if (!(query instanceof Nodes.Select)) {
        return sql;
      }

      Nodes.Select select = (Nodes.Select) query;
      List<Expression> newExprs = new ArrayList<>();
      for (Expression expr : select.getExpressions()) {
        newExprs.add(wrapWithCast(expr, columnName, dataType));
      }

      Expression result = new Nodes.Select(newExprs, select.isDistinct(),
          select.getFrom(), select.getJoins(), select.getWhere(),
          select.getGroupBy(), select.getHaving(), select.getOrderBy(),
          select.getLimit(), select.getOffset());
      return DRILL.format(SqlGlot.generate(result, "DRILL"));
    } catch (Exception e) {
      logger.warn("Data type conversion failed: {}", e.getMessage(), e);
      return null;
    }
  }

  /**
   * Pretty-print a SQL string using sqlglot.
   *
   * @param sql the SQL string to format
   * @return the formatted SQL, or the original if formatting fails
   */
  public String formatSql(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    try {
      return DRILL.format(sql);
    } catch (Exception e) {
      logger.debug("SQL formatting failed, returning original: {}", e.getMessage());
      return sql;
    }
  }

  /**
   * Change the time grain of a temporal column in a SQL query using sqlglot AST manipulation.
   *
   * @param sql the SQL query
   * @param columnName the temporal column to transform
   * @param timeGrain the time grain (e.g. "MONTH", "YEAR")
   * @param columnsJson optional JSON array string of column names (for star queries)
   * @return the transformed SQL, or the original SQL if transformation fails
   */
  public String changeTimeGrain(String sql, String columnName,
      String timeGrain, String columnsJson) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    try {
      String grain = timeGrain.toUpperCase();
      Optional<Expression> parsed = DRILL.parseOne(sql);
      if (!parsed.isPresent()) {
        return sql;
      }
      Expression query = parsed.get();

      // Handle star queries
      if (isStarQuery(query)) {
        if (columnsJson != null && !columnsJson.isEmpty()) {
          List<String> columnList = MAPPER.readValue(columnsJson,
              new TypeReference<List<String>>() { });
          Map<String, Object> columnMap = new HashMap<>();
          for (String col : columnList) {
            columnMap.put(col, "VARCHAR");
          }
          query = replaceStarWithColumns(query, columnMap);
        }
      }

      if (!(query instanceof Nodes.Select)) {
        return sql;
      }

      Nodes.Select select = (Nodes.Select) query;
      List<Expression> newExprs = new ArrayList<>();
      for (Expression expr : select.getExpressions()) {
        newExprs.add(wrapWithDateTrunc(expr, columnName, grain));
      }

      Expression result = new Nodes.Select(newExprs, select.isDistinct(),
          select.getFrom(), select.getJoins(), select.getWhere(),
          select.getGroupBy(), select.getHaving(), select.getOrderBy(),
          select.getLimit(), select.getOffset());
      return DRILL.format(SqlGlot.generate(result, "DRILL"));
    } catch (Exception e) {
      logger.warn("Time grain change failed: {}", e.getMessage(), e);
      return sql;
    }
  }

  /**
   * Returns whether the transpiler is available. Always true with Java sqlglot.
   */
  public boolean isAvailable() {
    return true;
  }

  // ==================== Private Helpers ====================

  /**
   * Returns true if the query contains a star (*) in the outermost SELECT.
   */
  private boolean isStarQuery(Expression query) {
    List<Expression> stars = query.findAll(Nodes.Star.class)
        .collect(Collectors.toList());
    return !stars.isEmpty();
  }

  /**
   * Expands a star query to use explicit column names.
   */
  private Expression replaceStarWithColumns(Expression query, Map<String, Object> columnMap) {
    if (!(query instanceof Nodes.Select)) {
      return query;
    }
    Nodes.Select select = (Nodes.Select) query;

    List<Expression> newExprs = new ArrayList<>();
    for (String col : columnMap.keySet()) {
      newExprs.add(new Nodes.Identifier(col, true));
    }

    // Keep any non-star expressions from the original SELECT
    for (Expression expr : select.getExpressions()) {
      if (!(expr instanceof Nodes.Star)) {
        newExprs.add(expr);
      }
    }

    return new Nodes.Select(newExprs, select.isDistinct(),
        select.getFrom(), select.getJoins(), select.getWhere(),
        select.getGroupBy(), select.getHaving(), select.getOrderBy(),
        select.getLimit(), select.getOffset());
  }

  /**
   * Wraps a select expression with CAST if it references the target column.
   */
  private Expression wrapWithCast(Expression expr, String columnName, String dataType) {
    if (expr instanceof Nodes.Alias) {
      Nodes.Alias alias = (Nodes.Alias) expr;
      Expression inner = alias.getExpression();
      if (hasIdentifierNamed(inner, columnName)) {
        Nodes.Cast cast = new Nodes.Cast(inner, new Nodes.DataTypeExpr(dataType));
        return new Nodes.Alias(cast, alias.getAlias());
      }
    } else if (expr instanceof Nodes.Identifier) {
      if (((Nodes.Identifier) expr).getName().equalsIgnoreCase(columnName)) {
        Nodes.Cast cast = new Nodes.Cast(expr, new Nodes.DataTypeExpr(dataType));
        return new Nodes.Alias(cast, columnName);
      }
    } else if (expr instanceof Nodes.Dot) {
      if (hasIdentifierNamed(expr, columnName)) {
        Nodes.Cast cast = new Nodes.Cast(expr, new Nodes.DataTypeExpr(dataType));
        return new Nodes.Alias(cast, columnName);
      }
    } else if (expr instanceof Nodes.Function) {
      if (hasIdentifierNamed(expr, columnName)) {
        Nodes.Cast cast = new Nodes.Cast(expr, new Nodes.DataTypeExpr(dataType));
        return new Nodes.Alias(cast, columnName);
      }
    }
    return expr;
  }

  /**
   * Wraps a select expression with DATE_TRUNC if it references the target column.
   */
  private Expression wrapWithDateTrunc(Expression expr, String columnName, String timeGrain) {
    if (expr instanceof Nodes.Alias) {
      Nodes.Alias alias = (Nodes.Alias) expr;
      Expression inner = alias.getExpression();

      // Case: already a DATE_TRUNC function — update the grain
      if (inner instanceof Nodes.Function
          && ((Nodes.Function) inner).getName().equalsIgnoreCase("DATE_TRUNC")
          && hasIdentifierNamed(inner, columnName)) {
        List<Expression> args = ((Nodes.Function) inner).getArgs();
        List<Expression> newArgs = new ArrayList<>(args);
        newArgs.set(0, new Nodes.Literal(timeGrain, true));
        return new Nodes.Alias(new Nodes.Function("DATE_TRUNC", newArgs), alias.getAlias());
      }

      if (hasIdentifierNamed(inner, columnName)) {
        Nodes.Function trunc = new Nodes.Function("DATE_TRUNC",
            Arrays.asList(new Nodes.Literal(timeGrain, true), inner));
        return new Nodes.Alias(trunc, alias.getAlias());
      }
    } else if (expr instanceof Nodes.Function
        && ((Nodes.Function) expr).getName().equalsIgnoreCase("DATE_TRUNC")
        && hasIdentifierNamed(expr, columnName)) {
      // Naked DATE_TRUNC — update grain and add alias
      List<Expression> args = ((Nodes.Function) expr).getArgs();
      List<Expression> newArgs = new ArrayList<>(args);
      newArgs.set(0, new Nodes.Literal(timeGrain, true));
      return new Nodes.Alias(new Nodes.Function("DATE_TRUNC", newArgs), columnName);
    } else if (expr instanceof Nodes.Identifier) {
      if (((Nodes.Identifier) expr).getName().equalsIgnoreCase(columnName)) {
        Nodes.Function trunc = new Nodes.Function("DATE_TRUNC",
            Arrays.asList(new Nodes.Literal(timeGrain, true), expr));
        return new Nodes.Alias(trunc, columnName);
      }
    } else if (expr instanceof Nodes.Dot) {
      if (hasIdentifierNamed(expr, columnName)) {
        Nodes.Function trunc = new Nodes.Function("DATE_TRUNC",
            Arrays.asList(new Nodes.Literal(timeGrain, true), expr));
        return new Nodes.Alias(trunc, columnName);
      }
    } else if (expr instanceof Nodes.Function) {
      if (hasIdentifierNamed(expr, columnName)) {
        Nodes.Function trunc = new Nodes.Function("DATE_TRUNC",
            Arrays.asList(new Nodes.Literal(timeGrain, true), expr));
        return new Nodes.Alias(trunc, columnName);
      }
    }
    return expr;
  }

  /**
   * Checks whether an expression tree contains an Identifier with the given name.
   * In Java sqlglot, simple column references are parsed as Identifier nodes.
   */
  private boolean hasIdentifierNamed(Expression expr, String columnName) {
    if (expr instanceof Nodes.Identifier) {
      return ((Nodes.Identifier) expr).getName().equalsIgnoreCase(columnName);
    }
    return expr.findAll(Nodes.Identifier.class)
        .anyMatch(id -> ((Nodes.Identifier) id).getName().equalsIgnoreCase(columnName));
  }

  /**
   * Resolves short table names to fully qualified names using schema metadata.
   */
  private String ensureFullTableNames(String sql, String schemasJson) {
    try {
      List<Map<String, Object>> schemas = MAPPER.readValue(schemasJson,
          new TypeReference<List<Map<String, Object>>>() { });
      if (schemas == null || schemas.isEmpty()) {
        return sql;
      }

      schemas = cleanupTableNames(schemas);
      Optional<Expression> parsed = DRILL.parseOne(sql);
      if (!parsed.isPresent()) {
        return sql;
      }
      Expression query = parsed.get();

      // Count tables in query
      List<Expression> queryTables = query.findAll(Nodes.Table.class)
          .collect(Collectors.toList());

      // Build table-to-plugin mapping
      Map<String, String> tableToPluginMap = new HashMap<>();

      for (Map<String, Object> schema : schemas) {
        String pluginName = (String) schema.get("name");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("tables");
        if (tables == null) {
          continue;
        }

        for (Map<String, Object> table : tables) {
          String tableName = (String) table.get("name");
          if (tableName == null) {
            continue;
          }

          if (tableName.contains(".")) {
            String[] tableParts = tableName.split("\\.", 2);
            if (startsWithKnownExtension(tableParts[1])) {
              tableToPluginMap.put(tableName, pluginName + "." + tableName);
            }
            tableToPluginMap.put(tableParts[0], pluginName + "." + tableName);
            String tableNameWithPlugin = pluginName + "." + tableParts[0];
            tableToPluginMap.put(tableNameWithPlugin, pluginName + "." + tableName);
          } else if (tableName.isEmpty() && pluginName.contains(".")) {
            String[] parts = pluginName.split("\\.", 2);
            tableToPluginMap.put(parts[1], pluginName);
          } else {
            tableToPluginMap.put(tableName, pluginName + "." + tableName);
          }
        }
      }

      // Apply table name replacements using transform
      Expression result = query.transform(node -> {
        if (node instanceof Nodes.Table) {
          String tblName = ((Nodes.Table) node).getName();
          String fullName = tableToPluginMap.get(tblName);
          if (fullName != null && fullName.contains(".")) {
            String[] parts = fullName.split("\\.", 2);
            return new Nodes.Table(parts[1], parts[0]);
          }
        }
        return node;
      });

      // Fix aggregate query projection
      result = fixAggregateQueryProjection(result);

      String resultSql = SqlGlot.generate(result, "DRILL");
      resultSql = removeTokens(resultSql);
      return DRILL.format(resultSql);
    } catch (Exception e) {
      logger.debug("Table name resolution failed: {}", e.getMessage());
      return sql;
    }
  }

  /**
   * Fixes aggregate queries where projected columns are missing from GROUP BY.
   */
  private Expression fixAggregateQueryProjection(Expression query) {
    if (!(query instanceof Nodes.Select)) {
      return query;
    }
    Nodes.Select select = (Nodes.Select) query;
    Nodes.GroupBy groupBy = select.getGroupBy();
    if (groupBy == null) {
      return query;
    }

    List<Expression> groupExprs = new ArrayList<>(groupBy.getExpressions());
    boolean modified = false;
    for (Expression expr : select.getExpressions()) {
      if (expr instanceof Nodes.Identifier || expr instanceof Nodes.Column) {
        boolean alreadyInGroup = groupExprs.stream()
            .anyMatch(g -> g.toString().equals(expr.toString()));
        if (!alreadyInGroup) {
          groupExprs.add(expr);
          modified = true;
        }
      }
    }

    if (modified) {
      return new Nodes.Select(select.getExpressions(), select.isDistinct(),
          select.getFrom(), select.getJoins(), select.getWhere(),
          new Nodes.GroupBy(groupExprs), select.getHaving(),
          select.getOrderBy(), select.getLimit(), select.getOffset());
    }
    return query;
  }

  /**
   * Removes token placeholders and restores original characters.
   */
  private String removeTokens(String sql) {
    for (Map.Entry<String, String> entry : CHAR_MAPPING.entrySet()) {
      if (sql.contains(entry.getValue())) {
        sql = sql.replace(entry.getValue(), entry.getKey());
      }
    }
    return sql;
  }

  /**
   * Replaces illegal characters in a name with token placeholders.
   */
  private String replaceIllegalCharacterWithToken(String name) {
    for (Map.Entry<String, String> entry : CHAR_MAPPING.entrySet()) {
      if (name.contains(entry.getKey())) {
        name = name.replace(entry.getKey(), entry.getValue());
      }
    }
    return name;
  }

  /**
   * Checks if a name starts with a known file extension.
   */
  private boolean startsWithKnownExtension(String name) {
    for (String ext : EXTENSIONS) {
      if (name.startsWith(ext)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Cleans up table names in schema metadata:
   * removes .view.drill extensions and replaces illegal characters.
   */
  private List<Map<String, Object>> cleanupTableNames(List<Map<String, Object>> schemas) {
    for (Map<String, Object> schema : schemas) {
      String name = (String) schema.get("name");
      if (name != null) {
        schema.put("name", replaceIllegalCharacterWithToken(name));
      }

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("tables");
      if (tables == null) {
        continue;
      }

      for (Map<String, Object> table : tables) {
        String tableName = (String) table.get("name");
        if (tableName != null) {
          tableName = tableName.replace(".view.drill", "");
          tableName = replaceIllegalCharacterWithToken(tableName);
          table.put("name", tableName);
        }

        @SuppressWarnings("unchecked")
        List<String> columns = (List<String>) table.get("columns");
        if (columns != null) {
          for (int j = 0; j < columns.size(); j++) {
            columns.set(j, replaceIllegalCharacterWithToken(columns.get(j)));
          }
        }
      }
    }
    return schemas;
  }
}
