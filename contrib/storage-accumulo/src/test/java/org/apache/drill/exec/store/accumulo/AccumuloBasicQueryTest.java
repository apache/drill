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
 * Basic query integration tests for Accumulo storage plugin.
 *
 * <p>These tests verify basic SELECT queries work correctly against
 * real Accumulo tables via MiniAccumuloCluster.</p>
 */
public class AccumuloBasicQueryTest extends BaseAccumuloTest {

  @Test
  public void testSelectStarFromTable1() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1);
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testSelectSpecificColumnsFromTable1() throws Exception {
    String sql = "SELECT row_key, cf.name, cf.age FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1);
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testSelectRowKeyOnly() throws Exception {
    String sql = "SELECT row_key FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1);
    runAccumuloSQLVerifyCount(sql, 10);
  }

  @Test
  public void testSelectFromUsersTable() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS);
    runAccumuloSQLVerifyCount(sql, 20);
  }

  @Test
  public void testSelectMultipleColumnFamilies() throws Exception {
    String sql = "SELECT row_key, personal.first_name, personal.last_name, employment.company " +
        "FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS);
    runAccumuloSQLVerifyCount(sql, 20);
  }

  @Test
  public void testSelectFromLargeTable() throws Exception {
    String sql = "SELECT * FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_LARGE);
    runAccumuloSQLVerifyCount(sql, 1000);
  }

  @Test
  public void testCountStar() throws Exception {
    String sql = "SELECT COUNT(*) FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_1);
    runAccumuloSQLVerifyCount(sql, 1);
  }

  @Test
  public void testCountStarUsersTable() throws Exception {
    String sql = "SELECT COUNT(*) FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS);
    runAccumuloSQLVerifyCount(sql, 1);
  }

  @Test
  public void testDistinctCompany() throws Exception {
    String sql = "SELECT DISTINCT employment.company FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS);
    // Should have 3 distinct companies: Acme Corp, TechCo, DataInc
    runAccumuloSQLVerifyCount(sql, 3);
  }

  @Test
  public void testGroupByCompany() throws Exception {
    String sql = "SELECT employment.company, COUNT(*) as cnt " +
        "FROM " + fullTableName(AccumuloTestUtils.TEST_TABLE_USERS) +
        " GROUP BY employment.company";
    runAccumuloSQLVerifyCount(sql, 3);
  }
}
