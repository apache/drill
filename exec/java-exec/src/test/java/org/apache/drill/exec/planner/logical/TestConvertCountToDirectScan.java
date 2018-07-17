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
package org.apache.drill.exec.planner.logical;

import org.apache.drill.PlanTestBase;
import org.apache.drill.categories.PlannerTest;
import org.apache.drill.exec.ExecConstants;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

@Category(PlannerTest.class)
public class TestConvertCountToDirectScan extends PlanTestBase {

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.copyResourceToRoot(Paths.get("directcount.parquet"));
  }

  @Test
  public void ensureCaseDoesNotConvertToDirectScan() throws Exception {
    testPlanMatchingPatterns(
        "select count(case when n_name = 'ALGERIA' and n_regionkey = 2 then n_nationkey else null end) as cnt\n" +
            "from dfs.`directcount.parquet`", new String[]{"CASE"});
  }

  @Test
  public void ensureConvertSimpleCountToDirectScan() throws Exception {
    String sql = "select count(*) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(sql, new String[]{"DynamicPojoRecordReader"});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureConvertSimpleCountConstToDirectScan() throws Exception {
    String sql = "select count(100) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(sql, new String[]{"DynamicPojoRecordReader"});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureConvertSimpleCountConstExprToDirectScan() throws Exception {
    String sql = "select count(1 + 2) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(sql, new String[]{"DynamicPojoRecordReader"});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureDoesNotConvertForDirectoryColumns() throws Exception {
    String sql = "select count(dir0) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(sql, new String[]{"ParquetGroupScan"});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(0L)
        .go();
  }

  @Test
  public void ensureConvertForImplicitColumns() throws Exception {
    String sql = "select count(fqn) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(sql, new String[]{"DynamicPojoRecordReader"});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();
  }

  @Test
  public void ensureConvertForSeveralColumns() throws Exception {
    test("use dfs.tmp");
    String tableName = "parquet_table_counts";

    try {
      String newFqnColumnName = "new_fqn";
      test("alter session set `%s` = '%s'", ExecConstants.IMPLICIT_FQN_COLUMN_LABEL, newFqnColumnName);
      test("create table %s as select * from cp.`parquet/alltypes_optional.parquet`", tableName);
      test("refresh table metadata %s", tableName);

      String sql = String.format("select\n" +
          "count(%s) as implicit_count,\n" +
          "count(*) as star_count,\n" +
          "count(col_int) as int_column_count,\n" +
          "count(col_vrchr) as vrchr_column_count\n" +
          "from %s", newFqnColumnName, tableName);

      testPlanMatchingPatterns(sql, new String[]{"DynamicPojoRecordReader"});

      testBuilder()
          .sqlQuery(sql)
          .unOrdered()
          .baselineColumns("implicit_count", "star_count", "int_column_count", "vrchr_column_count")
          .baselineValues(6L, 6L, 2L, 3L)
          .go();

    } finally {
      test("alter session reset `%s`", ExecConstants.IMPLICIT_FQN_COLUMN_LABEL);
      test("drop table if exists %s", tableName);
    }
  }

  @Test
  public void ensureCorrectCountWithMissingStatistics() throws Exception {
    test("use dfs.tmp");
    String tableName = "wide_str_table";
    try {
      // table will contain two partitions: one - with null value, second - with non null value
      test("create table %s partition by (col_str) as select * from cp.`parquet/wide_string.parquet`", tableName);

      String query = String.format("select count(col_str) as cnt_str, count(*) as cnt_total from %s", tableName);

      // direct scan should not be applied since we don't have statistics
      testPlanMatchingPatterns(query, null, new String[]{"DynamicPojoRecordReader"});

      testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("cnt_str", "cnt_total")
        .baselineValues(1L, 2L)
        .go();
    } finally {
      test("drop table if exists %s", tableName);
    }
  }

}
