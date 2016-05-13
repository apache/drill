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
package org.apache.drill;

import org.apache.drill.common.exceptions.UserException;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.Assert;


public class TestDropTable extends PlanTestBase {

  private static final String CREATE_SIMPLE_TABLE = "create table %s as select 1 from cp.`employee.json`";
  private static final String CREATE_SIMPLE_VIEW = "create view %s as select 1 from cp.`employee.json`";
  private static final String DROP_TABLE = "drop table %s";
  private static final String DROP_TABLE_IF_EXISTS = "drop table if exists %s";
  private static final String DROP_VIEW_IF_EXISTS = "drop view if exists %s";
  private static final String BACK_TICK = "`";

  @Test
  public void testDropJsonTable() throws Exception {
    test("use dfs_test.tmp");
    test("alter session set `store.format` = 'json'");

    final String tableName = "simple_json";
    // create a json table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    // drop the table
    final String dropSql = String.format(DROP_TABLE, tableName);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", tableName))
        .go();
  }

  @Test
  public void testDropParquetTable() throws Exception {
    test("use dfs_test.tmp");
    final String tableName = "simple_json";

    // create a parquet table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    // drop the table
    final String dropSql = String.format(DROP_TABLE, tableName);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", tableName))
        .go();
  }

  @Test
  public void testDropTextTable() throws Exception {
    test("use dfs_test.tmp");

    test("alter session set `store.format` = 'csv'");
    final String csvTable = "simple_csv";

    // create a csv table
    test(String.format(CREATE_SIMPLE_TABLE, csvTable));

    // drop the table
    String dropSql = String.format(DROP_TABLE, csvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", csvTable))
        .go();

    test("alter session set `store.format` = 'psv'");
    final String psvTable = "simple_psv";

    // create a psv table
    test(String.format(CREATE_SIMPLE_TABLE, psvTable));

    // drop the table
    dropSql = String.format(DROP_TABLE, psvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", psvTable))
        .go();

    test("alter session set `store.format` = 'tsv'");
    final String tsvTable = "simple_tsv";

    // create a tsv table
    test(String.format(CREATE_SIMPLE_TABLE, tsvTable));

    // drop the table
    dropSql = String.format(DROP_TABLE, tsvTable);
    testBuilder()
        .sqlQuery(dropSql)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", tsvTable))
        .go();
  }

  @Test
  public void testNonHomogenousDrop() throws Exception {
    test("use dfs_test.tmp");
    final String tableName = "homogenous_table";

    // create a parquet table
    test(String.format(CREATE_SIMPLE_TABLE, tableName));

    // create a json table within the same directory
    test("alter session set `store.format` = 'json'");
    final String nestedJsonTable = tableName + Path.SEPARATOR + "json_table";
    test(String.format(CREATE_SIMPLE_TABLE, BACK_TICK + nestedJsonTable + BACK_TICK));

    test("show files from " + tableName);

    boolean dropFailed = false;
    // this should fail, because the directory contains non-homogenous files
    try {
      test(String.format(DROP_TABLE, tableName));
    } catch (UserException e) {
      Assert.assertTrue(e.getMessage().contains("VALIDATION ERROR"));
      dropFailed = true;
    }

    Assert.assertTrue("Dropping of non-homogeneous table should have failed", dropFailed);

    // drop the individual json table
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE, BACK_TICK + nestedJsonTable + BACK_TICK))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", nestedJsonTable))
        .go();

    // Now drop should succeed
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE, tableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", tableName))
        .go();
  }

  @Test
  public void testDropOnImmutableSchema() throws Exception {
    boolean dropFailed = false;
    try {
      test("drop table dfs.`/tmp`");
    } catch (UserException e) {
      Assert.assertTrue(e.getMessage().contains("PARSE ERROR"));
      dropFailed = true;
    }

    Assert.assertTrue("Dropping table on immutable schema failed", dropFailed);
  }

  @Test // DRILL-4673
  public void testDropTableIfExistsWhileTableExists() throws Exception {
    final String existentTableName = "test_table";
    test("use dfs_test.tmp");

    // successful dropping of existent table
    test(String.format(CREATE_SIMPLE_TABLE, existentTableName));
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, existentTableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] dropped", existentTableName))
        .go();
  }

  @Test // DRILL-4673
  public void testDropTableIfExistsWhileTableDoesNotExist() throws Exception {
    final String nonExistentTableName = "test_table";
    test("use dfs_test.tmp");

    // dropping of non existent table without error
    testBuilder()
        .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, nonExistentTableName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Table [%s] not found", nonExistentTableName))
        .go();
  }

  @Test // DRILL-4673
  public void testDropTableIfExistsWhileItIsAView() throws Exception {
    final String viewName = "test_view";
    try{
      test("use dfs_test.tmp");

      // dropping of non existent table without error if the view with such name is existed
      test(String.format(CREATE_SIMPLE_VIEW, viewName));
      testBuilder()
          .sqlQuery(String.format(DROP_TABLE_IF_EXISTS, viewName))
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(true, String.format("Table [%s] not found", viewName))
          .go();
    } finally {
      test(String.format(DROP_VIEW_IF_EXISTS, viewName));
    }
  }
}
