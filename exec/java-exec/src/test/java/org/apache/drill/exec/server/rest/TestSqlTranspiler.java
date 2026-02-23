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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.drill.test.BaseTest;
import org.junit.Test;

/**
 * Unit tests for the {@link SqlTranspiler} singleton.
 * Java sqlglot is always available, so no tests are conditionally skipped.
 */
public class TestSqlTranspiler extends BaseTest {

  @Test
  public void testIsAvailableAlwaysTrue() {
    assertTrue(SqlTranspiler.getInstance().isAvailable(),
        "isAvailable() should always return true with Java sqlglot");
  }

  @Test
  public void testIsAvailableReturnsConsistently() {
    boolean first = SqlTranspiler.getInstance().isAvailable();
    boolean second = SqlTranspiler.getInstance().isAvailable();
    assertEquals(first, second,
        "isAvailable() should return the same value on repeated calls");
  }

  @Test
  public void testSimplePassthrough() {
    String sql = "SELECT * FROM t";
    String result = SqlTranspiler.getInstance().transpile(sql, "drill", "drill");
    assertNotNull(result);
    assertTrue(result.toUpperCase().contains("SELECT"),
        "Simple SELECT should pass through, got: " + result);
  }

  @Test
  public void testMysqlToDrillCast() {
    String mysqlSql = "SELECT CAST(x AS TEXT) FROM t";
    String result = SqlTranspiler.getInstance().transpile(mysqlSql, "mysql", "drill");
    assertNotNull(result);
    assertTrue(result.toUpperCase().contains("CAST"),
        "MySQL CAST should be preserved in transpilation, got: " + result);
    assertTrue(result.toUpperCase().contains("SELECT"),
        "Transpiled SQL should contain SELECT, got: " + result);
  }

  @Test
  public void testMysqlToDrillBacktickQuoting() {
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
    String mysqlSql = "SELECT GROUP_CONCAT(name) FROM t";
    String result = SqlTranspiler.getInstance().transpile(mysqlSql, "mysql", "drill");
    assertNotNull(result, "GROUP_CONCAT transpilation should return a non-null result");
  }

  @Test
  public void testPostgresToDrill() {
    // PostgreSQL CURRENT_DATE is a simpler cross-dialect test
    String pgSql = "SELECT CURRENT_DATE FROM t";
    String result = SqlTranspiler.getInstance().transpile(pgSql, "postgres", "drill");
    assertNotNull(result, "PostgreSQL transpilation should return a non-null result");
    assertTrue(result.length() > 0,
        "PostgreSQL transpilation should return a non-empty result");
  }

  @Test
  public void testInvalidSqlFallback() {
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
  public void testFormatSql() {
    String sql = "SELECT a,b,c FROM t WHERE x=1";
    String result = SqlTranspiler.getInstance().formatSql(sql);
    assertNotNull(result);
    assertTrue(result.toUpperCase().contains("SELECT"),
        "Formatted SQL should still contain SELECT, got: " + result);
    assertTrue(result.toUpperCase().contains("WHERE"),
        "Formatted SQL should still contain WHERE, got: " + result);
  }

  @Test
  public void testConvertDataType() {
    String sql = "SELECT col FROM t";
    String result = SqlTranspiler.getInstance().convertDataType(sql, "col", "INTEGER", null);
    assertNotNull(result, "convertDataType should return non-null");
    assertTrue(result.toUpperCase().contains("CAST"),
        "Result should contain CAST, got: " + result);
    assertTrue(result.toUpperCase().contains("INTEGER"),
        "Result should contain INTEGER, got: " + result);
  }

  @Test
  public void testChangeTimeGrain() {
    String sql = "SELECT order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", null);
    assertNotNull(result, "changeTimeGrain should return non-null");
    assertTrue(result.toUpperCase().contains("DATE_TRUNC"),
        "Result should contain DATE_TRUNC, got: " + result);
    assertTrue(result.toUpperCase().contains("MONTH"),
        "Result should contain MONTH, got: " + result);
  }

  // ==================== formatSql tests ====================

  @Test
  public void testFormatSqlMultiClause() {
    String sql = "SELECT a, b FROM t WHERE x = 1 ORDER BY a";
    String result = SqlTranspiler.getInstance().formatSql(sql);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("SELECT"), "Should contain SELECT, got: " + result);
    assertTrue(upper.contains("WHERE"), "Should contain WHERE, got: " + result);
    assertTrue(upper.contains("ORDER BY"), "Should contain ORDER BY, got: " + result);
  }

  @Test
  public void testFormatSqlInvalidReturnsOriginal() {
    String garbage = "NOT VALID SQL @@## GARBAGE";
    String result = SqlTranspiler.getInstance().formatSql(garbage);
    assertEquals(garbage, result,
        "Invalid SQL should be returned unchanged");
  }

  @Test
  public void testFormatSqlNullAndEmpty() {
    assertNull(SqlTranspiler.getInstance().formatSql(null),
        "null input should return null");
    assertEquals("", SqlTranspiler.getInstance().formatSql(""),
        "empty input should return empty");
  }

  @Test
  public void testFormatSqlComplexJoin() {
    String sql = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id WHERE a.status = 'active'";
    String result = SqlTranspiler.getInstance().formatSql(sql);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("JOIN"), "Should contain JOIN, got: " + result);
    assertTrue(upper.contains("WHERE"), "Should contain WHERE, got: " + result);
  }

  // ==================== transpile tests ====================

  @Test
  public void testTranspileNullSqlReturnsNull() {
    String result = SqlTranspiler.getInstance().transpile(null, "mysql", "drill");
    assertNull(result, "null SQL should return null");
  }

  @Test
  public void testTranspileWithSchemas() {
    String sql = "SELECT col1 FROM my_table";
    String schemasJson = "[{\"name\":\"dfs\",\"tables\":[{\"name\":\"my_table\",\"columns\":[\"col1\"]}]}]";
    String result = SqlTranspiler.getInstance().transpile(sql, "drill", "drill", schemasJson);
    assertNotNull(result, "Result should not be null");
    assertTrue(result.toUpperCase().contains("SELECT"),
        "Result should contain SELECT, got: " + result);
    assertTrue(result.toUpperCase().contains("MY_TABLE"),
        "Result should preserve table name, got: " + result);
  }

  @Test
  public void testTranspileWithSchemasViewDrill() {
    String sql = "SELECT col1 FROM my_view";
    String schemasJson = "[{\"name\":\"dfs\",\"tables\":[{\"name\":\"my_view.view.drill\",\"columns\":[\"col1\"]}]}]";
    String result = SqlTranspiler.getInstance().transpile(sql, "drill", "drill", schemasJson);
    assertNotNull(result, "Result should not be null");
    assertTrue(result.toUpperCase().contains("SELECT"),
        "Result should contain SELECT, got: " + result);
    assertTrue(!result.contains(".view.drill"),
        ".view.drill extension should be stripped, got: " + result);
  }

  // ==================== convertDataType tests ====================

  @Test
  public void testConvertDataTypePreservesExistingAlias() {
    String sql = "SELECT cost AS price FROM orders";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "cost", "DOUBLE", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("CAST"), "Should contain CAST, got: " + result);
    assertTrue(upper.contains("DOUBLE"), "Should contain DOUBLE, got: " + result);
    assertTrue(upper.contains("PRICE"), "Should preserve alias 'price', got: " + result);
  }

  @Test
  public void testConvertDataTypeColumnInsideFunction() {
    String sql = "SELECT UPPER(name) AS uname FROM t";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "name", "VARCHAR", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("CAST"), "Should contain CAST, got: " + result);
    assertTrue(upper.contains("VARCHAR"), "Should contain VARCHAR, got: " + result);
  }

  @Test
  public void testConvertDataTypeStarQueryExpansion() {
    String sql = "SELECT * FROM t";
    String columnsJson = "{\"col1\":\"VARCHAR\",\"col2\":\"INTEGER\"}";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "col1", "DOUBLE", columnsJson);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("CAST"), "Should contain CAST for target column, got: " + result);
    assertTrue(upper.contains("DOUBLE"), "Should contain DOUBLE, got: " + result);
    assertTrue(!upper.contains("*"), "Star should be expanded, got: " + result);
  }

  @Test
  public void testConvertDataTypeStarQueryNoColumnsReturnsOriginal() {
    String sql = "SELECT * FROM t";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "col1", "INTEGER", null);
    assertNotNull(result, "Result should not be null");
    assertEquals(sql, result,
        "Star query without columns JSON should return original");
  }

  @Test
  public void testConvertDataTypeMultipleColumnsOnlyTargetWrapped() {
    String sql = "SELECT a, b, c FROM t";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "b", "INTEGER", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("CAST"), "Should contain CAST for column b, got: " + result);
    // Count CAST occurrences — should be exactly 1
    int castCount = upper.split("CAST", -1).length - 1;
    assertEquals(1, castCount,
        "Only the target column should be wrapped with CAST, got: " + result);
  }

  @Test
  public void testConvertDataTypeVarcharType() {
    String sql = "SELECT col FROM t";
    String result = SqlTranspiler.getInstance().convertDataType(
        sql, "col", "VARCHAR", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("CAST"), "Should contain CAST, got: " + result);
    assertTrue(upper.contains("VARCHAR"), "Should contain VARCHAR, got: " + result);
  }

  @Test
  public void testConvertDataTypeInvalidSqlReturnsNull() {
    String result = SqlTranspiler.getInstance().convertDataType(
        "NOT VALID SQL @@## GARBAGE", "col", "INTEGER", null);
    assertNull(result, "Invalid SQL should return null");
  }

  @Test
  public void testConvertDataTypeNullSqlReturnsNull() {
    String result = SqlTranspiler.getInstance().convertDataType(
        null, "col", "INTEGER", null);
    assertNull(result, "null SQL should return null");
  }

  @Test
  public void testConvertDataTypeEmptySqlReturnsNull() {
    String result = SqlTranspiler.getInstance().convertDataType(
        "", "col", "INTEGER", null);
    assertNull(result, "empty SQL should return null");
  }

  // ==================== changeTimeGrain tests ====================

  @Test
  public void testChangeTimeGrainAliasedColumn() {
    String sql = "SELECT order_date AS d FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("MONTH"), "Should contain MONTH, got: " + result);
    // Alias should be preserved
    assertTrue(upper.contains(" D") || upper.contains(" AS D") || upper.contains("AS D"),
        "Alias 'd' should be preserved, got: " + result);
  }

  @Test
  public void testChangeTimeGrainExistingDateTrunc() {
    String sql = "SELECT DATE_TRUNC('DAY', order_date) AS order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("MONTH"), "Should contain updated grain MONTH, got: " + result);
  }

  @Test
  public void testChangeTimeGrainColumnInsideFunction() {
    String sql = "SELECT CAST(order_date AS DATE) AS d FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("MONTH"), "Should contain MONTH, got: " + result);
  }

  @Test
  public void testChangeTimeGrainYearGrain() {
    String sql = "SELECT order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "YEAR", null);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("YEAR"), "Should contain YEAR, got: " + result);
  }

  @Test
  public void testChangeTimeGrainDayGrain() {
    String sql = "SELECT order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "DAY", null);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("DAY"), "Should contain DAY, got: " + result);
  }

  @Test
  public void testChangeTimeGrainQuarterGrain() {
    String sql = "SELECT order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "QUARTER", null);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("QUARTER"), "Should contain QUARTER, got: " + result);
  }

  @Test
  public void testChangeTimeGrainStarQueryExpansion() {
    String sql = "SELECT * FROM orders";
    String columnsJson = "[\"order_date\",\"amount\",\"status\"]";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", columnsJson);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"),
        "Should contain DATE_TRUNC for target column, got: " + result);
    assertTrue(!upper.contains("*"), "Star should be expanded, got: " + result);
  }

  @Test
  public void testChangeTimeGrainLowercaseGrainNormalized() {
    String sql = "SELECT order_date FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "month", null);
    assertNotNull(result);
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"), "Should contain DATE_TRUNC, got: " + result);
    assertTrue(upper.contains("MONTH"), "Should contain MONTH (normalized), got: " + result);
  }

  @Test
  public void testChangeTimeGrainMultipleColumnsOnlyTargetWrapped() {
    String sql = "SELECT a, order_date, c FROM orders";
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        sql, "order_date", "MONTH", null);
    assertNotNull(result, "Result should not be null");
    String upper = result.toUpperCase();
    assertTrue(upper.contains("DATE_TRUNC"),
        "Should contain DATE_TRUNC for target, got: " + result);
    int truncCount = upper.split("DATE_TRUNC", -1).length - 1;
    assertEquals(1, truncCount,
        "Only target column should have DATE_TRUNC, got: " + result);
  }

  @Test
  public void testChangeTimeGrainNullSqlReturnsNull() {
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        null, "col", "MONTH", null);
    assertNull(result, "null SQL should return null");
  }

  @Test
  public void testChangeTimeGrainEmptySqlReturnsEmpty() {
    String result = SqlTranspiler.getInstance().changeTimeGrain(
        "", "col", "MONTH", null);
    assertEquals("", result, "empty SQL should return empty");
  }

  // ==================== Thread safety ====================

  @Test
  public void testTranspileThreadSafety() throws Exception {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<String>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      final int idx = i;
      futures.add(executor.submit(() -> {
        String sql = "SELECT col" + idx + " FROM table" + idx;
        return SqlTranspiler.getInstance().transpile(sql, "mysql", "drill");
      }));
    }

    for (int i = 0; i < threadCount; i++) {
      String result = futures.get(i).get();
      assertNotNull(result, "Thread " + i + " result should not be null");
      assertTrue(result.toUpperCase().contains("SELECT"),
          "Thread " + i + " result should contain SELECT, got: " + result);
    }
    executor.shutdown();
  }
}
