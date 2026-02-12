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

import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

/**
 * Unit tests for the {@link SqlTranspiler} singleton.
 * Tests that require GraalPy + sqlglot are skipped when the runtime is unavailable.
 */
public class TestSqlTranspiler extends BaseTest {

  private static final String GRAALPY_SKIP_MSG =
      "GraalPy + sqlglot not available in this environment";

  @Test
  public void testIsAvailableReturnsConsistently() {
    // Should not throw; returns true when GraalPy available, false otherwise
    boolean first = SqlTranspiler.getInstance().isAvailable();
    boolean second = SqlTranspiler.getInstance().isAvailable();
    assertEquals(first, second,
        "isAvailable() should return the same value on repeated calls");
  }

  @Test
  public void testSimplePassthrough() {
    // Passthrough works whether or not GraalPy is available — original SQL is returned
    String sql = "SELECT * FROM t";
    String result = SqlTranspiler.getInstance().transpile(sql, "drill", "drill");
    assertEquals(sql, result, "Simple SELECT should pass through unchanged");
  }

  @Test
  public void testMysqlToDrillCast() {
    assumeTrue(GRAALPY_SKIP_MSG, SqlTranspiler.getInstance().isAvailable());
    String mysqlSql = "SELECT CAST(x AS TEXT) FROM t";
    String result = SqlTranspiler.getInstance().transpile(mysqlSql, "mysql", "drill");
    assertNotNull(result);
    assertTrue(result.toUpperCase().contains("VARCHAR"),
        "MySQL CAST(x AS TEXT) should be transpiled to use VARCHAR for Drill, got: " + result);
  }

  @Test
  public void testMysqlToDrillBacktickQuoting() {
    assumeTrue(GRAALPY_SKIP_MSG, SqlTranspiler.getInstance().isAvailable());
    String mysqlSql = "SELECT `column_name` FROM `my_table`";
    String result = SqlTranspiler.getInstance().transpile(mysqlSql, "mysql", "drill");
    assertNotNull(result);
    assertTrue(result.contains("column_name"),
        "Backtick-quoted identifiers should be preserved, got: " + result);
    assertTrue(result.contains("my_table"),
        "Backtick-quoted table names should be preserved, got: " + result);
  }

  @Test
  public void testMysqlToDrillGroupConcat() {
    assumeTrue(GRAALPY_SKIP_MSG, SqlTranspiler.getInstance().isAvailable());
    String mysqlSql = "SELECT GROUP_CONCAT(name) FROM t";
    String result = SqlTranspiler.getInstance().transpile(mysqlSql, "mysql", "drill");
    assertNotNull(result, "GROUP_CONCAT transpilation should return a non-null result");
  }

  @Test
  public void testPostgresToDrill() {
    assumeTrue(GRAALPY_SKIP_MSG, SqlTranspiler.getInstance().isAvailable());
    String pgSql = "SELECT now()::date";
    String result = SqlTranspiler.getInstance().transpile(pgSql, "postgres", "drill");
    assertNotNull(result, "PostgreSQL transpilation should return a non-null result");
    assertTrue(result.length() > 0,
        "PostgreSQL transpilation should return a non-empty result");
  }

  @Test
  public void testInvalidSqlFallback() {
    // Works regardless of GraalPy availability — original SQL returned either way
    String invalidSql = "NOT VALID SQL @@## GARBAGE";
    String result = SqlTranspiler.getInstance().transpile(invalidSql, "mysql", "drill");
    assertEquals(invalidSql, result,
        "Invalid SQL should be returned unchanged as a graceful fallback");
  }

  @Test
  public void testEmptySqlPassthrough() {
    String result = SqlTranspiler.getInstance().transpile("", "mysql", "drill");
    assertEquals("", result, "Empty SQL input should return empty string");
  }

  @Test
  public void testNullDialectDefaults() {
    String sql = "SELECT 1";
    String result = SqlTranspiler.getInstance().transpile(sql, null, null);
    assertNotNull(result, "Passing null dialects should not crash");
  }

  @Test
  public void testSingletonIdentity() {
    SqlTranspiler first = SqlTranspiler.getInstance();
    SqlTranspiler second = SqlTranspiler.getInstance();
    assertSame(first, second, "getInstance() should return the same singleton instance");
  }

  @Test
  public void testFallbackWhenUnavailable() {
    // When GraalPy is not available, transpile should return original SQL
    if (!SqlTranspiler.getInstance().isAvailable()) {
      String sql = "SELECT CAST(x AS TEXT) FROM t";
      String result = SqlTranspiler.getInstance().transpile(sql, "mysql", "drill");
      assertEquals(sql, result,
          "When GraalPy is unavailable, original SQL should be returned unchanged");
    }
  }
}
