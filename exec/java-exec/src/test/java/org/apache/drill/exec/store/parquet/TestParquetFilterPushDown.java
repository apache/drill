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

package org.apache.drill.exec.store.parquet;

import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.BitControl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TestParquetFilterPushDown extends PlanTestBase {
  private static final String CTAS_TABLE = "order_ctas";
  private static FragmentContext fragContext;

  private static FileSystem fs;

  @BeforeClass
  public static void initFSAndCreateFragContext() throws Exception {
    fs = getLocalFileSystem();
    fragContext = new FragmentContext(bits[0].getContext(),
        BitControl.PlanFragment.getDefaultInstance(), null, bits[0].getContext().getFunctionImplementationRegistry());

    dirTestWatcher.copyResourceToRoot(Paths.get("parquetFilterPush"));
    dirTestWatcher.copyResourceToRoot(Paths.get("parquet", "multirowgroup.parquet"));
  }

  @AfterClass
  public static void teardown() throws IOException {
    fs.close();
  }

  @Rule
  public final TestWatcher ctasWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      deleteCtasTable();
    }

    @Override
    protected void starting(Description description) {
      deleteCtasTable();
    }

    @Override
    protected void finished(Description description) {
      deleteCtasTable();
    }

    private void deleteCtasTable() {
      FileUtils.deleteQuietly(new File(dirTestWatcher.getDfsTestTmpDir(), CTAS_TABLE));
    }
  };

  @Test
  // Test filter evaluation directly without go through SQL queries.
  public void testIntPredicateWithEval() throws Exception {
    // intTbl.parquet has only one int column
    //    intCol : [0, 100].
    final File file = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(Paths.get("parquetFilterPush", "intTbl", "intTbl.parquet"))
      .toFile();
    ParquetMetadata footer = getParquetMetaData(file);

    testParquetRowGroupFilterEval(footer, "intCol = 100", false);
    testParquetRowGroupFilterEval(footer, "intCol = 0", false);
    testParquetRowGroupFilterEval(footer, "intCol = 50", false);

    testParquetRowGroupFilterEval(footer, "intCol = -1", true);
    testParquetRowGroupFilterEval(footer, "intCol = 101", true);

    testParquetRowGroupFilterEval(footer, "intCol > 100", true);
    testParquetRowGroupFilterEval(footer, "intCol > 99", false);

    testParquetRowGroupFilterEval(footer, "intCol >= 100", false);
    testParquetRowGroupFilterEval(footer, "intCol >= 101", true);

    testParquetRowGroupFilterEval(footer, "intCol < 100", false);
    testParquetRowGroupFilterEval(footer, "intCol < 1", false);
    testParquetRowGroupFilterEval(footer, "intCol < 0", true);

    testParquetRowGroupFilterEval(footer, "intCol <= 100", false);
    testParquetRowGroupFilterEval(footer, "intCol <= 1", false);
    testParquetRowGroupFilterEval(footer, "intCol <= 0", false);
    testParquetRowGroupFilterEval(footer, "intCol <= -1", true);

    // "and"
    testParquetRowGroupFilterEval(footer, "intCol > 100 and intCol  < 200", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and intCol < 200", false);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and intCol > 200", true); // essentially, intCol > 200

    // "or"
    testParquetRowGroupFilterEval(footer, "intCol = 150 or intCol = 160", true);
    testParquetRowGroupFilterEval(footer, "intCol = 50 or intCol = 160", false);

    //"nonExistCol" does not exist in the table. "AND" with a filter on exist column
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol = 100", true); // since nonExistCol = 100 -> Unknown -> could drop.
    testParquetRowGroupFilterEval(footer, "nonExistCol = 100 and intCol > 50", true); // since nonExistCol = 100 -> Unknown -> could drop.
    testParquetRowGroupFilterEval(footer, "intCol > 100 and nonExistCol < 'abc'", true);
    testParquetRowGroupFilterEval(footer, "nonExistCol < 'abc' and intCol > 100", true); // nonExistCol < 'abc' hit NumberException and is ignored, but intCol >100 will say "drop".
    testParquetRowGroupFilterEval(footer, "intCol > 50 and nonExistCol < 'abc'", false); // because nonExistCol < 'abc' hit NumberException and is ignored.

    //"nonExistCol" does not exist in the table. "OR" with a filter on exist column
    testParquetRowGroupFilterEval(footer, "intCol > 100 or nonExistCol = 100", true); // nonExistCol = 100 -> could drop.
    testParquetRowGroupFilterEval(footer, "nonExistCol = 100 or intCol > 100", true); // nonExistCol = 100 -> could drop.
    testParquetRowGroupFilterEval(footer, "intCol > 50 or nonExistCol < 100", false);
    testParquetRowGroupFilterEval(footer, "nonExistCol < 100 or intCol > 50", false);

    // cast function on column side (LHS)
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 100", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 0", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 50", false);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = 101", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as bigint) = -1", true);

    // cast function on constant side (RHS)
    testParquetRowGroupFilterEval(footer, "intCol = cast(100 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(0 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(50 as bigint)", false);
    testParquetRowGroupFilterEval(footer, "intCol = cast(101 as bigint)", true);
    testParquetRowGroupFilterEval(footer, "intCol = cast(-1 as bigint)", true);

    // cast into float4/float8
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(101.0 as float4)", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(-1.0 as float4)", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float4) = cast(1.0 as float4)", false);

    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = 101.0", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = -1.0", true);
    testParquetRowGroupFilterEval(footer, "cast(intCol as float8) = 1.0", false);
  }

  @Test
  public void testIntPredicateAgainstAllNullColWithEval() throws Exception {
    // intAllNull.parquet has only one int column with all values being NULL.
    // column values statistics: num_nulls: 25, min/max is not defined
    final File file = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(Paths.get("parquetFilterPush", "intTbl", "intAllNull.parquet"))
      .toFile();
    ParquetMetadata footer = getParquetMetaData(file);

    testParquetRowGroupFilterEval(footer, "intCol = 100", true);
    testParquetRowGroupFilterEval(footer, "intCol = 0", true);
    testParquetRowGroupFilterEval(footer, "intCol = -100", true);

    testParquetRowGroupFilterEval(footer, "intCol > 10", true);
    testParquetRowGroupFilterEval(footer, "intCol >= 10", true);

    testParquetRowGroupFilterEval(footer, "intCol < 10", true);
    testParquetRowGroupFilterEval(footer, "intCol <= 10", true);
  }

  @Test
  public void testDatePredicateAgainstDrillCTAS1_8WithEval() throws Exception {
    // The parquet file is created on drill 1.8.0 with DRILL CTAS:
    //   create table dfs.tmp.`dateTblCorrupted/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';

    final File file = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(Paths.get("parquetFilterPush", "dateTblCorrupted", "t1", "0_0_0.parquet"))
      .toFile();
    ParquetMetadata footer = getParquetMetaData(file);

    testDatePredicateAgainstDrillCTASHelper(footer);
  }

  @Test
  public void testDatePredicateAgainstDrillCTASPost1_8WithEval() throws Exception {
    // The parquet file is created on drill 1.9.0-SNAPSHOT (commit id:03e8f9f3e01c56a9411bb4333e4851c92db6e410) with DRILL CTAS:
    //   create table dfs.tmp.`dateTbl1_9/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';

    final File file = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(Paths.get("parquetFilterPush", "dateTbl1_9", "t1", "0_0_0.parquet"))
      .toFile();
    ParquetMetadata footer = getParquetMetaData(file);

    testDatePredicateAgainstDrillCTASHelper(footer);
  }

  private void testDatePredicateAgainstDrillCTASHelper(ParquetMetadata footer) throws Exception{
    testParquetRowGroupFilterEval(footer, "o_orderdate = cast('1992-01-01' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate = cast('1991-12-31' as date)", true);

    testParquetRowGroupFilterEval(footer, "o_orderdate >= cast('1991-12-31' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate >= cast('1992-01-03' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate >= cast('1992-01-04' as date)", true);

    testParquetRowGroupFilterEval(footer, "o_orderdate > cast('1992-01-01' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate > cast('1992-01-03' as date)", true);

    testParquetRowGroupFilterEval(footer, "o_orderdate <= cast('1992-01-01' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate <= cast('1991-12-31' as date)", true);

    testParquetRowGroupFilterEval(footer, "o_orderdate < cast('1992-01-02' as date)", false);
    testParquetRowGroupFilterEval(footer, "o_orderdate < cast('1992-01-01' as date)", true);
  }

  @Test
  public void testTimeStampPredicateWithEval() throws Exception {
    // Table dateTblCorrupted is created by CTAS in drill 1.8.0.
    //    create table dfs.tmp.`tsTbl/t1` as select DATE_ADD(cast(o_orderdate as date), INTERVAL '0 10:20:30' DAY TO SECOND) as o_ordertimestamp from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';
    final File file = dirTestWatcher.getRootDir()
      .toPath()
      .resolve(Paths.get("parquetFilterPush", "tsTbl", "t1", "0_0_0.parquet"))
      .toFile();
    ParquetMetadata footer = getParquetMetaData(file);

    testParquetRowGroupFilterEval(footer, "o_ordertimestamp = cast('1992-01-01 10:20:30' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp = cast('1992-01-01 10:20:29' as timestamp)", true);

    testParquetRowGroupFilterEval(footer, "o_ordertimestamp >= cast('1992-01-01 10:20:29' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp >= cast('1992-01-03 10:20:30' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp >= cast('1992-01-03 10:20:31' as timestamp)", true);

    testParquetRowGroupFilterEval(footer, "o_ordertimestamp > cast('1992-01-03 10:20:29' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp > cast('1992-01-03 10:20:30' as timestamp)", true);

    testParquetRowGroupFilterEval(footer, "o_ordertimestamp <= cast('1992-01-01 10:20:30' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp <= cast('1992-01-01 10:20:29' as timestamp)", true);

    testParquetRowGroupFilterEval(footer, "o_ordertimestamp < cast('1992-01-01 10:20:31' as timestamp)", false);
    testParquetRowGroupFilterEval(footer, "o_ordertimestamp < cast('1992-01-01 10:20:30' as timestamp)", true);

  }

  @Test
  // Test against parquet files from Drill CTAS post 1.8.0 release.
  public void testDatePredicateAgaistDrillCTASPost1_8() throws  Exception {
    test("use dfs.tmp");
    test("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and " +
      "date '1992-01-03'", CTAS_TABLE);
    test("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and " +
      "date '1992-01-06'", CTAS_TABLE);
    test("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and " +
      "date '1992-01-09'", CTAS_TABLE);

    final String query1 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate = date '1992-01-01'";
    testParquetFilterPD(query1, 9, 1, false);

    final String query2 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate < date '1992-01-01'";
    testParquetFilterPD(query2, 0, 1, false);

    final String query3 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-03'";
    testParquetFilterPD(query3, 22, 1, false);

    final String query4 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-04'";
    testParquetFilterPD(query4, 33, 2, false);

    final String query5 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate between date '1992-01-01' and date '1992-01-06'";
    testParquetFilterPD(query5, 49, 2, false);

    final String query6 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate > date '1992-01-10'";
    testParquetFilterPD(query6, 0, 1, false);

    // Test parquet files with metadata cache files available.
    // Now, create parquet metadata cache files, and run the above queries again. Flag "usedMetadataFile" should be true.
    test(String.format("refresh table metadata %s", CTAS_TABLE));
    testParquetFilterPD(query1, 9, 1, true);
    testParquetFilterPD(query2, 0, 1, true);
    testParquetFilterPD(query3, 22, 1, true);
    testParquetFilterPD(query4, 33, 2, true);
    testParquetFilterPD(query5, 49, 2, true);
    testParquetFilterPD(query6, 0, 1, true);
  }

  @Test
  public void testParquetFilterPDOptionsDisabled() throws Exception {
    try {
      test("alter session set `%s` = false", PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_KEY);

      test("use dfs.tmp");
      test("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and " +
        "date '1992-01-03'", CTAS_TABLE);
      test("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and " +
        "date '1992-01-06'", CTAS_TABLE);
      test("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and " +
        "date '1992-01-09'", CTAS_TABLE);

      final String query1 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate = date '1992-01-01'";
      testParquetFilterPD(query1, 9, 3, false);

    } finally {
      resetSessionOption(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_KEY);
    }
  }

  @Test
  public void testParquetFilterPDOptionsThreshold() throws Exception {
    try {
      test("alter session set `" + PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD_KEY  + "` = 2 ");

      test("use dfs.tmp");
      test("create table `%s/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and " +
        "date '1992-01-03'", CTAS_TABLE);
      test("create table `%s/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and " +
        "date '1992-01-06'", CTAS_TABLE);
      test("create table `%s/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and " +
        "date '1992-01-09'", CTAS_TABLE);

      final String query1 = "select o_orderdate from dfs.tmp.order_ctas where o_orderdate = date '1992-01-01'";
      testParquetFilterPD(query1, 9, 3, false);

    } finally {
      resetSessionOption(PlannerSettings.PARQUET_ROWGROUP_FILTER_PUSHDOWN_PLANNING_THRESHOLD_KEY);
    }
  }

  @Test
  public void testDatePredicateAgainstCorruptedDateCol() throws Exception {
    // Table dateTblCorrupted is created by CTAS in drill 1.8.0. Per DRILL-4203, the date column is shifted by some value.
    // The CTAS are the following, then copy to drill test resource directory.
    //    create table dfs.tmp.`dateTblCorrupted/t1` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';
    //    create table dfs.tmp.`dateTblCorrupted/t2` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and date '1992-01-06';
    //    create table dfs.tmp.`dateTblCorrupted/t3` as select cast(o_orderdate as date) as o_orderdate from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and date '1992-01-09';

    final String query1 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate = date '1992-01-01'";
    testParquetFilterPD(query1, 9, 1, false);

    final String query2 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate < date '1992-01-01'";
    testParquetFilterPD(query2, 0, 1, false);

    final String query3 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-03'";
    testParquetFilterPD(query3, 22, 1, false);

    final String query4 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-04'";
    testParquetFilterPD(query4, 33, 2, false);

    final String query5 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate between date '1992-01-01' and date '1992-01-06'";
    testParquetFilterPD(query5, 49, 2, false);

    final String query6 = "select o_orderdate from dfs.`parquetFilterPush/dateTblCorrupted` where o_orderdate > date '1992-01-10'";

    testParquetFilterPD(query6, 0, 1, false);
  }

  @Test
  public void testTimeStampPredicate() throws Exception {
    // Table dateTblCorrupted is created by CTAS in drill 1.8.0.
    //    create table dfs.tmp.`tsTbl/t1` as select DATE_ADD(cast(o_orderdate as date), INTERVAL '0 10:20:30' DAY TO SECOND) as o_ordertimestamp from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-01' and date '1992-01-03';
    //    create table dfs.tmp.`tsTbl/t2` as select DATE_ADD(cast(o_orderdate as date), INTERVAL '0 10:20:30' DAY TO SECOND) as o_ordertimestamp from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-04' and date '1992-01-06';
    //    create table dfs.tmp.`tsTbl/t3` as select DATE_ADD(cast(o_orderdate as date), INTERVAL '0 10:20:30' DAY TO SECOND) as o_ordertimestamp from cp.`tpch/orders.parquet` where o_orderdate between date '1992-01-07' and date '1992-01-09';

    final String query1 = "select o_ordertimestamp from dfs.`parquetFilterPush/tsTbl` where o_ordertimestamp = timestamp '1992-01-01 10:20:30'";
    testParquetFilterPD(query1, 9, 1, false);

    final String query2 = "select o_ordertimestamp from dfs.`parquetFilterPush/tsTbl` where o_ordertimestamp < timestamp '1992-01-01 10:20:30'";
    testParquetFilterPD(query2, 0, 1, false);

    final String query3 = "select o_ordertimestamp from dfs.`parquetFilterPush/tsTbl` where o_ordertimestamp between timestamp '1992-01-01 00:00:00' and timestamp '1992-01-06 10:20:30'";
    testParquetFilterPD(query3, 49, 2, false);
  }

  @Test // DRILL-5359
  public void testFilterWithItemFlatten() throws Exception {
    final String sql = "select n_regionkey\n"
        + "from (select n_regionkey, \n"
        + "            flatten(nation.cities) as cities \n"
        + "      from cp.`tpch/nation.parquet` nation) as flattenedCities \n"
        + "where flattenedCities.cities.`zip` = '12345'";

    final String[] expectedPlan = {"(?s)Filter.*Flatten"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);

  }

  @Test
  public void testMultiRowGroup() throws Exception {
    // multirowgroup is a parquet file with 2 rowgroups inside. One with a = 1 and the other with a = 2;
    // FilterPushDown should be able to remove the rowgroup with a = 1 from the scan operator.
    final String sql = String.format("select * from dfs.`parquet/multirowgroup.parquet` where a > 1");
    final String[] expectedPlan = {"numRowGroups=1"};
    final String[] excludedPlan = {};
    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Some test helper functions.
  //////////////////////////////////////////////////////////////////////////////////////////////////

  private void testParquetFilterPD(final String query, int expectedRowCount, int expectedNumFiles, boolean usedMetadataFile) throws Exception{
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    String usedMetaPattern = "usedMetadataFile=" + usedMetadataFile;

    testPlanMatchingPatterns(query, new String[]{numFilesPattern, usedMetaPattern}, new String[] {});
  }

  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final String exprStr,
      boolean canDropExpected) throws Exception{
    final LogicalExpression filterExpr = parseExpr(exprStr);
    testParquetRowGroupFilterEval(footer, 0, filterExpr, canDropExpected);
  }

  private void testParquetRowGroupFilterEval(final ParquetMetadata footer, final int rowGroupIndex,
      final LogicalExpression filterExpr, boolean canDropExpected) throws Exception {
    boolean canDrop = ParquetRGFilterEvaluator.evalFilter(filterExpr, footer, rowGroupIndex,
        fragContext.getOptions(), fragContext);
    Assert.assertEquals(canDropExpected, canDrop);
  }

  private ParquetMetadata getParquetMetaData(File file) throws IOException{
    return ParquetFileReader.readFooter(new Configuration(fs.getConf()), new Path(file.toURI()));
  }
}
