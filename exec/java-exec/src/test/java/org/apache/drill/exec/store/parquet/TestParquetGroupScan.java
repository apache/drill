/**
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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestParquetGroupScan extends BaseTestQuery {

  private void prepareTables(final String tableName, boolean refreshMetadata) throws Exception {
    // first create some parquet subfolders
    testNoResult("CREATE TABLE dfs_test.tmp.`%s`      AS SELECT employee_id FROM cp.`employee.json` LIMIT 1", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/501`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 2", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/502`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 4", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/503`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 8", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/504`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 16", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/505`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 32", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/60`   AS SELECT employee_id FROM cp.`employee.json` LIMIT 64", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/602`  AS SELECT employee_id FROM cp.`employee.json` LIMIT 128", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/6031` AS SELECT employee_id FROM cp.`employee.json` LIMIT 256", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/6032` AS SELECT employee_id FROM cp.`employee.json` LIMIT 512", tableName);
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/6033` AS SELECT employee_id FROM cp.`employee.json` LIMIT 1024", tableName);

    // we need an empty subfolder `4376/20160401`
    // to do this we first create a table inside that subfolder
    testNoResult("CREATE TABLE dfs_test.tmp.`%s/6041/a` AS SELECT * FROM cp.`employee.json` LIMIT 1", tableName);
    // then we delete the table, leaving the parent subfolder empty
    testNoResult("DROP TABLE   dfs_test.tmp.`%s/6041/a`", tableName);

    if (refreshMetadata) {
      // build the metadata cache file
      testNoResult("REFRESH TABLE METADATA dfs_test.tmp.`%s`", tableName);
    }
  }

  @Test
  public void testFix4376() throws Exception {
    prepareTables("4376_1", true);

    testBuilder()
      .sqlQuery("SELECT COUNT(*) AS `count` FROM dfs_test.tmp.`4376_1/60*`")
      .ordered()
      .baselineColumns("count").baselineValues(1984L)
      .go();
  }

  @Test
  public void testWildCardEmptyWithCache() throws Exception {
    prepareTables("4376_2", true);

    try {
      runSQL("SELECT COUNT(*) AS `count` FROM dfs_test.tmp.`4376_2/604*`");
      fail("Query should've failed!");
    } catch (UserRemoteException uex) {
      final String expectedMsg = "The table you tried to query is empty";
      assertTrue(String.format("Error message should contain \"%s\" but was instead \"%s\"", expectedMsg,
        uex.getMessage()), uex.getMessage().contains(expectedMsg));
    }
  }

  @Test
  public void testWildCardEmptyNoCache() throws Exception {
    prepareTables("4376_3", false);

    try {
      runSQL("SELECT COUNT(*) AS `count` FROM dfs_test.tmp.`4376_3/604*`");
      fail("Query should've failed!");
    } catch (UserRemoteException uex) {
      final String expectedMsg = "Table 'dfs_test.tmp.4376_3/604*' not found";
      assertTrue(String.format("Error message should contain \"%s\" but was instead \"%s\"", expectedMsg,
        uex.getMessage()), uex.getMessage().contains(expectedMsg));
    }
  }

  @Test
  public void testSelectEmptyWithCache() throws Exception {
    prepareTables("4376_4", true);

    try {
      runSQL("SELECT COUNT(*) AS `count` FROM dfs_test.tmp.`4376_4/6041`");
      fail("Query should've failed!");
    } catch (UserRemoteException uex) {
      final String expectedMsg = "The table you tried to query is empty";
      assertTrue(String.format("Error message should contain \"%s\" but was instead \"%s\"", expectedMsg,
        uex.getMessage()), uex.getMessage().contains(expectedMsg));
    }
  }

  @Test
  public void testSelectEmptyNoCache() throws Exception {
    prepareTables("4376_5", false);
    try {
      runSQL("SELECT COUNT(*) AS `count` FROM dfs_test.tmp.`4376_5/6041`");
      fail("Query should've failed!");
    } catch (UserRemoteException uex) {
      final String expectedMsg = "Table 'dfs_test.tmp.4376_5/6041' not found";
      assertTrue(String.format("Error message should contain \"%s\" but was instead \"%s\"", expectedMsg,
        uex.getMessage()), uex.getMessage().contains(expectedMsg));
    }
  }
}
