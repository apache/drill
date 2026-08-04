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
package org.apache.drill.exec.store.accumulo;

import org.junit.Test;

/**
 * Integration tests for Accumulo pushdown capabilities.
 *
 * <p>These tests verify that filter, projection, limit, and sort pushdowns
 * work correctly with real Accumulo tables.</p>
 */
public class AccumuloPushdownIntegrationTest extends BaseAccumuloTest {

  // =========================================================================
  // Filter Pushdown Tests
  // =========================================================================

  @Test
  public void testFilterOnRowKeyEquals() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key = 'row_001'";
    runAccumuloSQLVerifyCount(sql, 1);
  }

  @Test
  public void testFilterOnRowKeyGreaterThan() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key > 'row_005'";
    runAccumuloSQLVerifyCount(sql, 5);  // row_006 to row_010
  }

  @Test
  public void testFilterOnRowKeyLessThan() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key < 'row_004'";
    runAccumuloSQLVerifyCount(sql, 3);  // row_001 to row_003
  }

  @Test
  public void testFilterOnRowKeyRange() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key >= 'row_003' AND row_key <= 'row_007'";
    runAccumuloSQLVerifyCount(sql, 5);  // row_003 to row_007
  }

  @Test
  public void testFilterOnRowKeyRangeLarge() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE) + " t" +
        " WHERE row_key >= 'row_0100' AND row_key < 'row_0200'";
    runAccumuloSQLVerifyCount(sql, 100);  // row_0100 to row_0199
  }

  @Test
  public void testFilterOnColumnValue() throws Exception {
    // Note: column value filters may not be pushed down, but should still work
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS) + " t" +
        " WHERE t.employment.company = 'Acme Corp'";
    runAccumuloSQLVerifyCount(sql, 7);  // 7 users at Acme Corp
  }

  // =========================================================================
  // Projection Pushdown Tests
  // =========================================================================

  @Test
  public void testProjectionSingleColumn() throws Exception {
    String sql = "SELECT t.cf.name FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testProjectionMultipleColumns() throws Exception {
    String sql = "SELECT t.cf.name, t.cf.city FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testProjectionWithRowKey() throws Exception {
    String sql = "SELECT row_key, t.personal.first_name FROM " +
        fullTableName(AccumuloTestUtils.TEST_TABLE_USERS) + " t";
    runAccumuloSQLVerifyCount(sql, 20);
  }

  @Test
  public void testProjectionSingleColumnFamily() throws Exception {
    String sql = "SELECT personal FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS) + " t";
    runAccumuloSQLVerifyCount(sql, 20);
  }

  // =========================================================================
  // Limit Pushdown Tests
  // =========================================================================

  @Test
  public void testLimitSmall() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" + " LIMIT 5";
    runAccumuloSQLVerifyCount(sql, 5);
  }

  @Test
  public void testLimitOnLargeTable() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE) + " t" + " LIMIT 50";
    runAccumuloSQLVerifyCount(sql, 50);
  }

  @Test
  public void testLimitOne() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" + " LIMIT 1";
    runAccumuloSQLVerifyCount(sql, 1);
  }

  @Test
  public void testLimitLargerThanTable() throws Exception {
    // Limit larger than table size should return all rows
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" + " LIMIT 100";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  // =========================================================================
  // Sort Pushdown Tests
  // =========================================================================

  @Test
  public void testOrderByRowKeyAsc() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " ORDER BY row_key ASC";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testOrderByRowKeyDesc() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " ORDER BY row_key DESC";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testOrderByRowKeyWithLimit() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE) + " t" +
        " ORDER BY row_key ASC LIMIT 10";
    runAccumuloSQLVerifyCount(sql, 10);
  }

  // =========================================================================
  // Combined Pushdown Tests
  // =========================================================================

  @Test
  public void testFilterAndProjection() throws Exception {
    String sql = "SELECT row_key, t.cf.name FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key > 'row_005'";
    runAccumuloSQLVerifyCount(sql, 5);
  }

  @Test
  public void testFilterAndLimit() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key > 'row_002' LIMIT 3";
    runAccumuloSQLVerifyCount(sql, 3);
  }

  @Test
  public void testProjectionAndLimit() throws Exception {
    String sql = "SELECT row_key, t.cf.name FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " LIMIT 5";
    runAccumuloSQLVerifyCount(sql, 5);
  }

  @Test
  public void testFilterProjectionAndLimit() throws Exception {
    String sql = "SELECT row_key, t.cf.name, t.cf.city FROM " +
        fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key >= 'row_003' LIMIT 4";
    runAccumuloSQLVerifyCount(sql, 4);
  }

  @Test
  public void testFilterAndSort() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key < 'row_006' ORDER BY row_key ASC";
    runAccumuloSQLVerifyCount(sql, 5);
  }

  @Test
  public void testSortAndLimit() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " ORDER BY row_key ASC LIMIT 3";
    runAccumuloSQLVerifyCount(sql, 3);
  }

  @Test
  public void testAllPushdownsCombined() throws Exception {
    String sql = "SELECT row_key, t.cf.name FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key >= 'row_002' AND row_key <= 'row_009'" +
        " ORDER BY row_key ASC LIMIT 5";
    runAccumuloSQLVerifyCount(sql, 5);
  }

  // =========================================================================
  // Edge Case Tests
  // =========================================================================

  @Test
  public void testFilterNoResults() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key = 'nonexistent'";
    runAccumuloSQLVerifyCount(sql, 0);
  }

  @Test
  public void testFilterOutOfRange() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" +
        " WHERE row_key > 'zzz'";
    runAccumuloSQLVerifyCount(sql, 0);
  }

  @Test
  public void testLimitZero() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1) + " t" + " LIMIT 0";
    runAccumuloSQLVerifyCount(sql, 0);
  }
}
