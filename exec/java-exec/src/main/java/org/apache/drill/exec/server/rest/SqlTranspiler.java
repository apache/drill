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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Singleton service that wraps a GraalPy context with sqlglot
 * for SQL dialect transpilation. Thread-safe via synchronized access
 * since GraalPy contexts are single-threaded.
 *
 * <p>Python functions are loaded from {@code python/sql_utils.py} on the
 * classpath rather than embedded as Java strings, making them easier
 * to maintain and extend.</p>
 *
 * <p>Falls back gracefully: if GraalPy or sqlglot cannot be initialized,
 * {@link #transpile} returns the original SQL unchanged.</p>
 */
public final class SqlTranspiler {

  private static final Logger logger = LoggerFactory.getLogger(SqlTranspiler.class);
  private static final String PYTHON_RESOURCE = "python/sql_utils.py";

  private static volatile SqlTranspiler instance;

  private Context pythonContext;
  private Value transpileFunc;
  private Value jsonParseFunc;
  private Value convertDataTypeFunc;
  private Value formatSqlFunc;
  private Value changeTimeGrainFunc;
  private boolean available;

  /**
   * Returns the singleton instance, creating it on first call.
   */
  public static SqlTranspiler getInstance() {
    if (instance == null) {
      synchronized (SqlTranspiler.class) {
        if (instance == null) {
          instance = new SqlTranspiler();
        }
      }
    }
    return instance;
  }

  private SqlTranspiler() {
    try {
      pythonContext = Context.newBuilder("python")
          .allowAllAccess(true)
          .build();

      String pythonCode = loadResource(PYTHON_RESOURCE);
      Source source = Source.newBuilder("python", pythonCode, PYTHON_RESOURCE).build();
      pythonContext.eval(source);

      transpileFunc = pythonContext.getBindings("python").getMember("transpile_sql");
      jsonParseFunc = pythonContext.getBindings("python").getMember("json_parse");
      convertDataTypeFunc = pythonContext.getBindings("python").getMember("convert_data_type_raw");
      formatSqlFunc = pythonContext.getBindings("python").getMember("format_sql");
      changeTimeGrainFunc = pythonContext.getBindings("python").getMember("change_time_grain_raw");
      available = true;
      logger.info("SqlTranspiler initialized successfully with GraalPy + sqlglot");
    } catch (Exception e) {
      logger.warn("SqlTranspiler unavailable: GraalPy or sqlglot could not be initialized. "
          + "SQL will be passed through without transpilation. Error: {}", e.getMessage());
      available = false;
    }
  }

  /**
   * Transpile SQL from one dialect to another (without schema metadata).
   */
  public synchronized String transpile(String sql, String sourceDialect, String targetDialect) {
    return transpile(sql, sourceDialect, targetDialect, "[]");
  }

  /**
   * Transpile SQL from one dialect to another.
   *
   * @param sql the SQL string to transpile
   * @param sourceDialect the source dialect (e.g. "mysql")
   * @param targetDialect the target dialect (e.g. "drill")
   * @param schemasJson JSON string of schema metadata for table name resolution
   * @return the transpiled SQL, or the original SQL if transpilation is unavailable or fails
   */
  public synchronized String transpile(String sql, String sourceDialect,
      String targetDialect, String schemasJson) {
    if (!available) {
      return sql;
    }
    try {
      Value schemas = jsonParseFunc.execute(schemasJson != null ? schemasJson : "[]");
      String result = transpileFunc.execute(sql, sourceDialect, targetDialect, schemas).asString();
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
  public synchronized String convertDataType(String sql, String columnName,
      String dataType, String columnsJson) {
    if (!available) {
      logger.warn("convertDataType called but transpiler is not available");
      return null;
    }
    try {
      String result;
      if (columnsJson != null && !columnsJson.isEmpty()) {
        result = convertDataTypeFunc.execute(sql, columnName, dataType, columnsJson).asString();
      } else {
        result = convertDataTypeFunc.execute(sql, columnName, dataType).asString();
      }
      return result;
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
  public synchronized String formatSql(String sql) {
    if (!available) {
      return sql;
    }
    try {
      return formatSqlFunc.execute(sql).asString();
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
  public synchronized String changeTimeGrain(String sql, String columnName,
      String timeGrain, String columnsJson) {
    if (!available) {
      return sql;
    }
    try {
      String result;
      if (columnsJson != null && !columnsJson.isEmpty()) {
        result = changeTimeGrainFunc.execute(sql, columnName, timeGrain, columnsJson).asString();
      } else {
        result = changeTimeGrainFunc.execute(sql, columnName, timeGrain).asString();
      }
      return result;
    } catch (Exception e) {
      logger.warn("Time grain change failed: {}", e.getMessage(), e);
      return sql;
    }
  }

  /**
   * Returns whether the transpiler is available (GraalPy + sqlglot initialized successfully).
   */
  public boolean isAvailable() {
    return available;
  }

  private static String loadResource(String resourcePath) throws IOException {
    try (InputStream is = SqlTranspiler.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
