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

import org.apache.drill.test.BaseTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for RestQueryRunner with metadata queries.
 * Focus on understanding why INFORMATION_SCHEMA queries return incomplete results.
 */
public class RestQueryRunnerMetadataTest extends BaseTest {
  private static final Logger logger = LoggerFactory.getLogger(RestQueryRunnerMetadataTest.class);

  /**
   * Test 1: Simple SELECT with 1 column
   */
  @Test
  public void testSimpleOneColumn() throws Exception {
    logger.info("=== TEST 1: Simple SELECT with 1 column ===");
    String sql = "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mysql.store' LIMIT 5";
    logger.info("SQL: {}", sql);
    testQuery(sql, 1);
  }

  /**
   * Test 2: SELECT with 2 columns
   */
  @Test
  public void testTwoColumns() throws Exception {
    logger.info("=== TEST 2: SELECT with 2 columns ===");
    String sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'mysql.store' LIMIT 5";
    logger.info("SQL: {}", sql);
    testQuery(sql, 2);
  }

  /**
   * Test 3: SELECT with 3 columns (the problematic query)
   */
  @Test
  public void testThreeColumns() throws Exception {
    logger.info("=== TEST 3: SELECT with 3 columns (PROBLEMATIC) ===");
    String sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql.store' LIMIT 5";
    logger.info("SQL: {}", sql);
    testQuery(sql, 3);
  }

  /**
   * Test 4: The exact columns query with IN clause
   */
  @Test
  public void testColumnsQueryWithIn() throws Exception {
    logger.info("=== TEST 4: Columns query with IN clause ===");
    String sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA IN ('mysql.store') ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION";
    logger.info("SQL: {}", sql);
    testQuery(sql, 3);
  }

  /**
   * Test 5: Tables query (known to work)
   */
  @Test
  public void testTablesQueryWorks() throws Exception {
    logger.info("=== TEST 5: Tables query (known to work) ===");
    String sql = "SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('mysql.store') ORDER BY TABLE_SCHEMA, TABLE_NAME";
    logger.info("SQL: {}", sql);
    testQuery(sql, 2);
  }

  /**
   * Test 6: SELECT * on COLUMNS
   */
  @Test
  public void testSelectStar() throws Exception {
    logger.info("=== TEST 6: SELECT * FROM INFORMATION_SCHEMA.COLUMNS ===");
    String sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql.store' LIMIT 5";
    logger.info("SQL: {}", sql);
    // Don't know exact column count for *, just verify we get results
    testQuery(sql, -1);
  }

  /**
   * Test 7: Different column ordering
   */
  @Test
  public void testDifferentColumnOrder() throws Exception {
    logger.info("=== TEST 7: Different column ordering ===");
    String sql = "SELECT COLUMN_NAME, TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql.store' LIMIT 5";
    logger.info("SQL: {}", sql);
    testQuery(sql, 3);
  }

  /**
   * Test 8: With ORDER BY ORDINAL_POSITION
   */
  @Test
  public void testWithOrdinalPosition() throws Exception {
    logger.info("=== TEST 8: With ORDER BY ORDINAL_POSITION ===");
    String sql = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'mysql.store' ORDER BY ORDINAL_POSITION";
    logger.info("SQL: {}", sql);
    testQuery(sql, 3);
  }

  /**
   * Helper method to test a query
   */
  private void testQuery(String sql, int expectedColumnCount) {
    logger.info("Testing query with expected columns: {}", expectedColumnCount);
    logger.info("Note: This test requires a running Drill instance");
    logger.info("To run manually:");
    logger.info("  1. Start Drill server");
    logger.info("  2. Run: mvn test -Dtest=RestQueryRunnerMetadataTest#testYourMethod");
    logger.info("Expected: {} columns, at least 1 row", expectedColumnCount);
  }
}
