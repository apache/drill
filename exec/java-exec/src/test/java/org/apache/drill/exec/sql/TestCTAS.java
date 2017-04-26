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
package org.apache.drill.exec.sql;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestCTAS extends BaseTestQuery {
  @Test // DRILL-2589
  public void withDuplicateColumnsInDef1() throws Exception {
    ctasErrorTestHelper("CREATE TABLE %s.%s AS SELECT region_id, region_id FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "region_id")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef2() throws Exception {
    ctasErrorTestHelper("CREATE TABLE %s.%s AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "sales_city")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef3() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, regionid) " +
            "AS SELECT region_id, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "regionid")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef4() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, salescity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "salescity")
    );
  }

  @Test // DRILL-2589
  public void withDuplicateColumnsInDef5() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity, SalesCity) " +
            "AS SELECT region_id, sales_city, sales_city FROM cp.`region.json`",
        String.format("Duplicate column name [%s]", "SalesCity")
    );
  }

  @Test // DRILL-2589
  public void whenInEqualColumnCountInTableDefVsInTableQuery() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) " +
            "AS SELECT region_id, sales_city, sales_region FROM cp.`region.json`",
        "table's field list and the table's query field list have different counts."
    );
  }

  @Test // DRILL-2589
  public void whenTableQueryColumnHasStarAndTableFiledListIsSpecified() throws Exception {
    ctasErrorTestHelper(
        "CREATE TABLE %s.%s(regionid, salescity) " +
            "AS SELECT region_id, * FROM cp.`region.json`",
        "table's query field list has a '*', which is invalid when table's field list is specified."
    );
  }

  @Test // DRILL-2422
  public void createTableWhenATableWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTableWhenTableAlreadyExists";

    try {
      final String ctasQuery =
          String.format("CREATE TABLE %s.%s AS SELECT * from cp.`region.json`", TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      errorMsgTestHelper(ctasQuery,
          String.format("A table or view with given name [%s] already exists in schema [%s]", newTblName, TEMP_SCHEMA));
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-2422
  public void createTableWhenAViewWithSameNameAlreadyExists() throws Exception{
    final String newTblName = "createTableWhenAViewWithSameNameAlreadyExists";

    try {
      test(String.format("CREATE VIEW %s.%s AS SELECT * from cp.`region.json`", TEMP_SCHEMA, newTblName));

      final String ctasQuery =
          String.format("CREATE TABLE %s.%s AS SELECT * FROM cp.`employee.json`", TEMP_SCHEMA, newTblName);

      errorMsgTestHelper(ctasQuery,
          String.format("A table or view with given name [%s] already exists in schema [%s]",
              newTblName, "dfs_test.tmp"));
    } finally {
      test(String.format("DROP VIEW %s.%s", TEMP_SCHEMA, newTblName));
    }
  }

  @Test
  public void ctasPartitionWithEmptyList() throws Exception {
    final String newTblName = "ctasPartitionWithEmptyList";

    try {
      final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY AS SELECT * from cp.`region.json`", TEMP_SCHEMA, newTblName);

      errorMsgTestHelper(ctasQuery,"PARSE ERROR: Encountered \"AS\"");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3377
  public void partitionByCtasColList() throws Exception {
    final String newTblName = "partitionByCtasColList";

    try {
      final String ctasQuery = String.format("CREATE TABLE %s.%s (cnt, rkey) PARTITION BY (cnt) " +
          "AS SELECT count(*), n_regionkey from cp.`tpch/nation.parquet` group by n_regionkey",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select cnt, rkey from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select count(*) as cnt, n_regionkey as rkey from cp.`tpch/nation.parquet` group by n_regionkey";
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3374
  public void partitionByCtasFromView() throws Exception {
    final String newTblName = "partitionByCtasColList";
    final String newView = "partitionByCtasColListView";
    try {
      final String viewCreate = String.format("create or replace view %s.%s (col_int, col_varchar)  " +
          "AS select cast(n_nationkey as int), cast(n_name as varchar(30)) from cp.`tpch/nation.parquet`",
          TEMP_SCHEMA, newView);

      final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (col_int) AS SELECT * from %s.%s",
          TEMP_SCHEMA, newTblName, TEMP_SCHEMA, newView);

      test(viewCreate);
      test(ctasQuery);

      final String baselineQuery = "select cast(n_nationkey as int) as col_int, cast(n_name as varchar(30)) as col_varchar " +
        "from cp.`tpch/nation.parquet`";
      final String selectFromCreatedTable = String.format("select col_int, col_varchar from %s.%s", TEMP_SCHEMA, newTblName);
      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .unOrdered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();

      final String viewDrop = String.format("DROP VIEW %s.%s", TEMP_SCHEMA, newView);
      test(viewDrop);
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-3382
  public void ctasWithQueryOrderby() throws Exception {
    final String newTblName = "ctasWithQueryOrderby";

    try {
      final String ctasQuery = String.format("CREATE TABLE %s.%s   " +
          "AS SELECT n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` order by n_nationkey",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select n_nationkey, n_name, n_comment from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select n_nationkey, n_name, n_comment from cp.`tpch/nation.parquet` order by n_nationkey";

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test // DRILL-4392
  public void ctasWithPartition() throws Exception {
    final String newTblName = "nation_ctas";

    try {
      final String ctasQuery = String.format("CREATE TABLE %s.%s   " +
          "partition by (n_regionkey) AS SELECT n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1",
          TEMP_SCHEMA, newTblName);

      test(ctasQuery);

      final String selectFromCreatedTable = String.format(" select * from %s.%s", TEMP_SCHEMA, newTblName);
      final String baselineQuery = "select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_nationkey limit 1";

      testBuilder()
          .sqlQuery(selectFromCreatedTable)
          .ordered()
          .sqlBaselineQuery(baselineQuery)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void testPartitionByForAllTypes() throws Exception {
    final String location = "partitioned_tables_with_nulls";
    final String ctasQuery = "create table %s partition by (%s) as %s";
    final String tablePath = "%s.`%s/%s_%s`";

    // key - new table suffix, value - data query
    final Map<String, String> variations = Maps.newHashMap();
    variations.put("required", "select * from cp.`parquet/alltypes_required.parquet`");
    variations.put("optional", "select * from cp.`parquet/alltypes_optional.parquet`");
    variations.put("nulls_only", "select * from cp.`parquet/alltypes_optional.parquet` where %s is null");

    try {
      final QueryDataBatch result = testSqlWithResults("select * from cp.`parquet/alltypes_required.parquet` limit 0").get(0);
      for (UserBitShared.SerializedField field : result.getHeader().getDef().getFieldList()) {
        final String fieldName = field.getNamePart().getName();

        for (Map.Entry<String, String> variation : variations.entrySet()) {
          final String table = String.format(tablePath, TEMP_SCHEMA, location, fieldName, variation.getKey());
          final String dataQuery = String.format(variation.getValue(), fieldName);
          test(ctasQuery, table, fieldName, dataQuery, fieldName);
          testBuilder()
              .sqlQuery("select * from %s", table)
              .unOrdered()
              .sqlBaselineQuery(dataQuery)
              .build()
              .run();
        }
      }
      result.release();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), location));
    }
  }

  @Test
  public void createTableWithCustomUmask() throws Exception {
    test("use %s", TEMP_SCHEMA);
    String tableName = "with_custom_permission";
    StorageStrategy storageStrategy = new StorageStrategy("000", false);
    try (FileSystem fs = FileSystem.get(new Configuration())) {
      test("alter session set `%s` = '%s'", ExecConstants.PERSISTENT_TABLE_UMASK, storageStrategy.getUmask());
      test("create table %s as select 'A' from (values(1))", tableName);
      Path tableLocation = new Path(getDfsTestTmpSchemaLocation(), tableName);
      assertEquals("Directory permission should match",
          storageStrategy.getFolderPermission(), fs.getFileStatus(tableLocation).getPermission());
      assertEquals("File permission should match",
          storageStrategy.getFilePermission(), fs.listLocatedStatus(tableLocation).next().getPermission());
    } finally {
      test("alter session reset `%s`", ExecConstants.PERSISTENT_TABLE_UMASK);
      test("drop table if exists %s", tableName);
    }
  }

  private static void ctasErrorTestHelper(final String ctasSql, final String expErrorMsg) throws Exception {
    final String createTableSql = String.format(ctasSql, TEMP_SCHEMA, "testTableName");
    errorMsgTestHelper(createTableSql, expErrorMsg);
  }
}
