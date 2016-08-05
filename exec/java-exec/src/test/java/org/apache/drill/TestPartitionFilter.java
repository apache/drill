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


import static org.junit.Assert.assertEquals;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPartitionFilter extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPartitionFilter.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  private static void testExcludeFilter(String query, int expectedNumFiles,
      String excludedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern}, new String[]{excludedFilterPattern});
  }

  private static void testIncludeFilter(String query, int expectedNumFiles,
      String includedFilterPattern, int expectedRowCount) throws Exception {
    int actualRowCount = testSql(query);
    assertEquals(expectedRowCount, actualRowCount);
    String numFilesPattern = "numFiles=" + expectedNumFiles;
    testPlanMatchingPatterns(query, new String[]{numFilesPattern, includedFilterPattern}, new String[]{});
  }

  @BeforeClass
  public static void createParquetTable() throws Exception {
    test("alter session set `planner.disable_exchanges` = true");
    test(String.format("create table dfs_test.tmp.parquet partition by (yr, qrtr) as select o_orderkey, o_custkey, " +
        "o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, cast(dir0 as int) yr, dir1 qrtr " +
        "from dfs_test.`%s/multilevel/parquet`", TEST_RES_PATH));
    test("alter session set `planner.disable_exchanges` = false");
  }

  @Test  //Parquet: basic test with dir0 and dir1 filters
  public void testPartitionFilter1_Parquet() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test  //Parquet: basic test with dir0 and dir1 filters
  public void testPartitionFilter1_Parquet_from_CTAS() throws Exception {
    String query = String.format("select yr, qrtr, o_custkey, o_orderdate from dfs_test.tmp.parquet where yr=1994 and qrtr='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test  //Json: basic test with dir0 and dir1 filters
  public void testPartitionFilter1_Json() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test  //Json: basic test with dir0 and dir1 filters
  public void testPartitionFilter1_JsonFileMixDir() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/jsonFileMixDir` where dir0=1995 and dir1='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test  //Json: basic test with dir0 = and dir1 is null filters
  public void testPartitionFilterIsNull_JsonFileMixDir() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/jsonFileMixDir` where dir0=1995 and dir1 is null", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 5);
  }

  @Test  //Json: basic test with dir0 = and dir1 is not null filters
  public void testPartitionFilterIsNotNull_JsonFileMixDir() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/jsonFileMixDir` where dir0=1995 and dir1 is not null", TEST_RES_PATH);
    testExcludeFilter(query, 4, "Filter", 40);
  }

  @Test  //CSV: basic test with dir0 and dir1 filters in
  public void testPartitionFilter1_Csv() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/csv` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test //Parquet: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Parquet() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 5);
  }

  @Test //Parquet: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Parquet_from_CTAS() throws Exception {
    String query = String.format("select yr, qrtr, o_custkey, o_orderdate from dfs_test.tmp.parquet where o_custkey < 1000 and yr=1994 and qrtr='Q1'", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 5);
  }

  @Test //Json: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Json() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 5);
  }

  @Test //CSV: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Csv() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/csv` where columns[1] < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 5);
  }

  @Test //Parquet: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Parquet() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    testIncludeFilter(query, 2, "Filter", 8);
  }
  @Test //Parquet: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Parquet_from_CTAS() throws Exception {
    String query = String.format("select * from dfs_test.tmp.parquet where (yr=1994 and qrtr='Q1' and o_custkey < 500) or (yr=1995 and qrtr='Q2' and o_custkey > 500)", TEST_RES_PATH);
    testIncludeFilter(query, 2, "Filter", 8);
  }

  @Test //Json: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Json() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/json` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    testIncludeFilter(query, 2, "Filter", 8);
  }

  @Test //CSV: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Csv() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/csv` where (dir0=1994 and dir1='Q1' and columns[1] < 500) or (dir0=1995 and dir1='Q2' and columns[1] > 500)", TEST_RES_PATH);
    testIncludeFilter(query, 2, "Filter", 8);
  }

  @Test //Parquet: filters contain join conditions and partition filters
  public void testPartitionFilter4_Parquet() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/parquet` t1, cp.`tpch/customer.parquet` t2 where t1.o_custkey = t2.c_custkey and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
    test(query1);
  }

  @Test //Parquet: filters contain join conditions and partition filters
  public void testPartitionFilter4_Parquet_from_CTAS() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.tmp.parquet t1, cp.`tpch/customer.parquet` t2 where t1.o_custkey = t2.c_custkey and t1.yr=1994 and t1.qrtr='Q1'", TEST_RES_PATH);
    test(query1);
  }

  @Test //Json: filters contain join conditions and partition filters
  public void testPartitionFilter4_Json() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/json` t1, cp.`tpch/customer.parquet` t2 where cast(t1.o_custkey as bigint) = cast(t2.c_custkey as bigint) and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
    test(query1);
  }

  @Test //CSV: filters contain join conditions and partition filters
  public void testPartitionFilter4_Csv() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.columns[1] as o_custkey, t1.columns[4] as o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/csv` t1, cp.`tpch/customer.parquet` t2 where cast(t1.columns[1] as bigint) = cast(t2.c_custkey as bigint) and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
    test(query1);
  }

  @Test // Parquet: IN filter
  public void testPartitionFilter5_Parquet() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir0 in (1995, 1996)", TEST_RES_PATH);
    testExcludeFilter(query, 8, "Filter", 80);
  }

  @Test // Parquet: IN filter
  public void testPartitionFilter5_Parquet_from_CTAS() throws Exception {
    String query = String.format("select yr, qrtr, o_custkey, o_orderdate from dfs_test.tmp.parquet where yr in (1995, 1996)", TEST_RES_PATH);
    testExcludeFilter(query, 8, "Filter", 80);
  }

  @Test // Json: IN filter
  public void testPartitionFilter5_Json() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir0 in (1995, 1996)", TEST_RES_PATH);
    testExcludeFilter(query, 8, "Filter", 80);
  }

  @Test // CSV: IN filter
  public void testPartitionFilter5_Csv() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/csv` where dir0 in (1995, 1996)", TEST_RES_PATH);
    testExcludeFilter(query, 8, "Filter", 80);
  }

  @Test // Parquet: one side of OR has partition filter only, other side has both partition filter and non-partition filter
  public void testPartitionFilter6_Parquet() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where (dir0=1995 and o_totalprice < 40000) or dir0=1996", TEST_RES_PATH);
    testIncludeFilter(query, 8, "Filter", 46);
  }

  @Test // Parquet: one side of OR has partition filter only, other side has both partition filter and non-partition filter
  public void testPartitionFilter6_Parquet_from_CTAS() throws Exception {
    String query = String.format("select * from dfs_test.tmp.parquet where (yr=1995 and o_totalprice < 40000) or yr=1996", TEST_RES_PATH);
    testIncludeFilter(query, 8, "Filter", 46);
  }

  @Test // Parquet: trivial case with 1 partition filter
  public void testPartitionFilter7_Parquet() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where dir0=1995", TEST_RES_PATH);
    testExcludeFilter(query, 4, "Filter", 40);
  }

  @Test // Parquet: trivial case with 1 partition filter
  public void testPartitionFilter7_Parquet_from_CTAS() throws Exception {
    String query = String.format("select * from dfs_test.tmp.parquet where yr=1995", TEST_RES_PATH);
    testExcludeFilter(query, 4, "Filter", 40);
  }

  @Test // Parquet: partition filter on subdirectory only
  public void testPartitionFilter8_Parquet() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where dir1 in ('Q1','Q4')", TEST_RES_PATH);
    testExcludeFilter(query, 6, "Filter", 60);
  }

  @Test
  public void testPartitionFilter8_Parquet_from_CTAS() throws Exception {
    String query = String.format("select * from dfs_test.tmp.parquet where qrtr in ('Q1','Q4')", TEST_RES_PATH);
    testExcludeFilter(query, 6, "Filter", 60);
  }

  @Test // Parquet: partition filter on subdirectory only plus non-partition filter
  public void testPartitionFilter9_Parquet() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where dir1 in ('Q1','Q4') and o_totalprice < 40000", TEST_RES_PATH);
    testIncludeFilter(query, 6, "Filter", 9);
  }

  @Test
  public void testPartitionFilter9_Parquet_from_CTAS() throws Exception {
    String query = String.format("select * from dfs_test.tmp.parquet where qrtr in ('Q1','Q4') and o_totalprice < 40000", TEST_RES_PATH);
    testIncludeFilter(query, 6, "Filter", 9);
  }

  @Test
  public void testPartitoinFilter10_Parquet() throws Exception {
    String query = String.format("select max(o_orderprice) from dfs_test.`%s/multilevel/parquet` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 1);
  }

  @Test
  public void testPartitoinFilter10_Parquet_from_CTAS() throws Exception {
    String query = String.format("select max(o_orderprice) from dfs_test.tmp.parquet where yr=1994 and qrtr='Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 1);
  }

  @Test // see DRILL-2712
  public void testMainQueryFalseCondition() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query = String.format("select * from (select dir0, o_custkey from dfs_test.`%s` where dir0='1994') t where 1 = 0", root);
    // the 1 = 0 becomes limit 0, which will require to read only one parquet file, in stead of 4 for year '1994'.
    testExcludeFilter(query, 1, "Filter", 0);
  }

  @Test // see DRILL-2712
  public void testMainQueryTrueCondition() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query =  String.format("select * from (select dir0, o_custkey from dfs_test.`%s` where dir0='1994' ) t where 0 = 0", root);
    testExcludeFilter(query, 4, "Filter", 40);
  }

  @Test // see DRILL-2712
  public void testMainQueryFilterRegularColumn() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query =  String.format("select * from (select dir0, o_custkey from dfs_test.`%s` where dir0='1994' and o_custkey = 10) t limit 0", root);
    testIncludeFilter(query, 4, "Filter", 0);
  }

  @Test // see DRILL-2852 and DRILL-3591
  public void testPartitionFilterWithCast() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query = String.format("select myyear, myquarter, o_totalprice from (select cast(dir0 as varchar(10)) as myyear, "
        + " cast(dir1 as varchar(10)) as myquarter, o_totalprice from dfs_test.`%s`) where myyear = cast('1995' as varchar(10)) "
        + " and myquarter = cast('Q2' as varchar(10)) and o_totalprice < 40000.0 order by o_totalprice", root);

    testIncludeFilter(query, 1, "Filter", 3);
  }

  @Test
  public void testPPWithNestedExpression() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query = String.format("select * from dfs_test.`%s` where dir0 not in(1994) and o_orderpriority = '2-HIGH'",
        root);
    testIncludeFilter(query, 8, "Filter", 24);
  }

  @Test
  public void testPPWithCase() throws Exception {
    String root = FileUtils.getResourceAsFile("/multilevel/parquet").toURI().toString();
    String query = String.format("select 1 from " +
            "(select  CASE WHEN '07' = '13' THEN '13' ELSE CAST(dir0 as VARCHAR(4)) END as YEAR_FILTER from dfs_test.`%s` where o_orderpriority = '2-HIGH') subq" +
            " where subq.YEAR_FILTER not in('1994')", root);
    testIncludeFilter(query, 8, "Filter", 24);
  }

  @Test // DRILL-3702
  public void testPartitionFilterWithNonNullabeFilterExpr() throws Exception {
    String query = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where concat(dir0, '') = '1994' and concat(dir1, '') = 'Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test // DRILL-2748
  public void testPartitionFilterAfterPushFilterPastAgg() throws Exception {
    String query = String.format("select dir0, dir1, cnt from (select dir0, dir1, count(*) cnt from dfs_test.`%s/multilevel/parquet` group by dir0, dir1) where dir0 = '1994' and dir1 = 'Q1'", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 1);
  }

  // Coalesce filter is on the non directory column. DRILL-4071
  @Test
  public void testPartitionWithCoalesceFilter_1() throws Exception {
    String query = String.format("select 1 from dfs_test.`%s/multilevel/parquet` where dir0=1994 and dir1='Q1' and coalesce(o_custkey, 0) = 890", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 1);
  }

  // Coalesce filter is on the directory column
  @Test
  public void testPartitionWithCoalesceFilter_2() throws Exception {
    String query = String.format("select 1 from dfs_test.`%s/multilevel/parquet` where dir0=1994 and o_custkey = 890 and coalesce(dir1, 'NA') = 'Q1'", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 1);
  }

  @Test  //DRILL-4021: Json with complex type and nested flatten functions: dir0 and dir1 filters plus filter involves filter refering to output from nested flatten functions.
  public void testPartitionFilter_Json_WithFlatten() throws Exception {
    // this query expects to have the partition filter pushded.
    // With partition pruning, we will end with one file, and one row returned from the query.
    final String query = String.format(
        " select dir0, dir1, o_custkey, o_orderdate, provider from " +
        "   ( select dir0, dir1, o_custkey, o_orderdate, flatten(items['providers']) as provider " +
            " from (" +
            "   select dir0, dir1, o_custkey, o_orderdate, flatten(o_items) items " +
            "     from dfs_test.`%s/multilevel/jsoncomplex`) ) " +
            " where dir0=1995 " +   // should be pushed down and used as partitioning filter
            "   and dir1='Q1' " +   // should be pushed down and used as partitioning filter
            "   and provider = 'BestBuy'", // should NOT be pushed down.
        TEST_RES_PATH);

    testIncludeFilter(query, 1, "Filter", 1);
  }

  @Test
  public void testLogicalDirPruning() throws Exception {
    // 1995/Q1 contains one valid parquet, while 1996/Q1 contains bad format parquet.
    // If dir pruning happens in logical, the query will run fine, since the bad parquet has been pruned before we build ParquetGroupScan.
    String query = String.format("select dir0, o_custkey from dfs_test.`%s/multilevel/parquetWithBadFormat` where dir0=1995", TEST_RES_PATH);
    testExcludeFilter(query, 1, "Filter", 10);
  }

  @Test
  public void testLogicalDirPruning2() throws Exception {
    // 1995/Q1 contains one valid parquet, while 1996/Q1 contains bad format parquet.
    // If dir pruning happens in logical, the query will run fine, since the bad parquet has been pruned before we build ParquetGroupScan.
    String query = String.format("select dir0, o_custkey from dfs_test.`%s/multilevel/parquetWithBadFormat` where dir0=1995 and o_custkey > 0", TEST_RES_PATH);
    testIncludeFilter(query, 1, "Filter", 10);
  }

  @Test  //DRILL-4665: Partition pruning should occur when LIKE predicate on non-partitioning column
  public void testPartitionFilterWithLike() throws Exception {
    // Also should be insensitive to the order of the predicates
    String query1 = "select yr, qrtr from dfs_test.tmp.parquet where yr=1994 and o_custkey LIKE '%5%'";
    String query2 = "select yr, qrtr from dfs_test.tmp.parquet where o_custkey LIKE '%5%' and yr=1994";
    testIncludeFilter(query1, 4, "Filter", 9);
    testIncludeFilter(query2, 4, "Filter", 9);
    // Test when LIKE predicate on partitioning column
    String query3 = "select yr, qrtr from dfs_test.tmp.parquet where yr LIKE '%1995%' and o_custkey LIKE '%3%'";
    String query4 = "select yr, qrtr from dfs_test.tmp.parquet where o_custkey LIKE '%3%' and yr LIKE '%1995%'";
    testIncludeFilter(query3, 4, "Filter", 16);
    testIncludeFilter(query4, 4, "Filter", 16);
  }

  @Test //DRILL-3710 Partition pruning should occur with varying IN-LIST size
  public void testPartitionFilterWithInSubquery() throws Exception {
    String query = String.format("select * from dfs_test.`%s/multilevel/parquet` where cast (dir0 as int) IN (1994, 1994, 1994, 1994, 1994, 1994)", TEST_RES_PATH);
    /* In list size exceeds threshold - no partition pruning since predicate converted to join */
    test("alter session set `planner.in_subquery_threshold` = 2");
    testExcludeFilter(query, 12, "Filter", 40);
    /* In list size does not exceed threshold - partition pruning */
    test("alter session set `planner.in_subquery_threshold` = 10");
    testExcludeFilter(query, 4, "Filter", 40);
  }


  @Test // DRILL-4825: querying same table with different filter in UNION ALL.
  public void testPruneSameTableInUnionAll() throws Exception {
    final String query = String.format("select count(*) as cnt from "
        + "( select dir0 from dfs_test.`%s/multilevel/parquet` where dir0 in ('1994') union all "
        + "  select dir0 from dfs_test.`%s/multilevel/parquet` where dir0 in ('1995', '1996') )",
        TEST_RES_PATH, TEST_RES_PATH);

    String [] excluded = {"Filter"};

    // verify plan that filter is applied in partition pruning.
    testPlanMatchingPatterns(query, null, excluded);

    // verify we get correct count(*).
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues((long)120)
        .build()
        .run();
  }

  @Test // DRILL-4825: querying same table with different filter in Join.
  public void testPruneSameTableInJoin() throws Exception {
    final String query = String.format("select *  from "
            + "( select sum(o_custkey) as x from dfs_test.`%s/multilevel/parquet` where dir0 in ('1994') ) join "
            + " ( select sum(o_custkey) as y from dfs_test.`%s/multilevel/parquet` where dir0 in ('1995', '1996')) "
            + " on x = y ",
        TEST_RES_PATH, TEST_RES_PATH);

    String [] excluded = {"Filter"};
    // verify plan that filter is applied in partition pruning.
    testPlanMatchingPatterns(query, null, excluded);

    // verify we get empty result.
    testBuilder()
        .sqlQuery(query)
        .expectsEmptyResultSet()
        .build()
        .run();

  }

}
