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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    System.out.println("=== TRANSPILE INPUT ===");
    System.out.println("Available: " + available);
    System.out.println("SQL: " + sql);
    System.out.println("Source dialect: " + sourceDialect);
    System.out.println("Target dialect: " + targetDialect);
    System.out.println("Schemas JSON: " + schemasJson);
    System.out.println("======================");
    if (!available) {
      return sql;
    }
    try {
      Value schemas = jsonParseFunc.execute(schemasJson != null ? schemasJson : "[]");
      String result = transpileFunc.execute(sql, sourceDialect, targetDialect, schemas).asString();
      System.out.println("=== TRANSPILE OUTPUT ===");
      System.out.println("Result: " + result);
      System.out.println("========================");
      return result;
    } catch (Exception e) {
      logger.debug("SQL transpilation failed, returning original SQL: {}", e.getMessage());
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
