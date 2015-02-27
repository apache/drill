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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.Ignore;
import org.junit.Test;

public class TestExampleQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test // see DRILL-2328
  public void testConcatOnNull() throws Exception {
    try {
      test("use dfs.tmp");
      test("create view concatNull as (select * from cp.`customer.json` where customer_id < 5);");

      // Test Left Null
      testBuilder()
          .sqlQuery("select (mi || lname) as CONCATOperator, mi, lname, concat(mi, lname) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "mi", "lname","CONCAT")
          .baselineValues("A.Nowmer", "A.", "Nowmer", "A.Nowmer")
          .baselineValues("I.Whelply", "I.", "Whelply", "I.Whelply")
          .baselineValues(null, null, "Derry", "Derry")
          .baselineValues("J.Spence", "J.", "Spence", "J.Spence")
          .build().run();

      // Test Right Null
      testBuilder()
          .sqlQuery("select (lname || mi) as CONCATOperator, lname, mi, concat(lname, mi) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "lname", "mi", "CONCAT")
          .baselineValues("NowmerA.", "Nowmer", "A.", "NowmerA.")
          .baselineValues("WhelplyI.", "Whelply", "I.", "WhelplyI.")
          .baselineValues(null, "Derry", null, "Derry")
          .baselineValues("SpenceJ.", "Spence", "J.", "SpenceJ.")
          .build().run();

      // Test Two Sides
      testBuilder()
          .sqlQuery("select (mi || mi) as CONCATOperator, mi, mi, concat(mi, mi) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "mi", "mi0", "CONCAT")
          .baselineValues("A.A.", "A.", "A.", "A.A.")
          .baselineValues("I.I.", "I.", "I.", "I.I.")
          .baselineValues(null, null, null, "")
          .baselineValues("J.J.", "J.", "J.", "J.J.")
          .build().run();

      testBuilder()
          .sqlQuery("select (cast(null as varchar(10)) || lname) as CONCATOperator, " +
              "cast(null as varchar(10)) as NullColumn, lname, concat(cast(null as varchar(10)), lname) as CONCAT from concatNull")
          .ordered()
          .baselineColumns("CONCATOperator", "NullColumn", "lname", "CONCAT")
          .baselineValues(null, null, "Nowmer", "Nowmer")
          .baselineValues(null, null, "Whelply", "Whelply")
          .baselineValues(null, null, "Derry", "Derry")
          .baselineValues(null, null, "Spence", "Spence")
          .build().run();
    } finally {
      test("drop view concatNull;");
    }
  }

  @Test // see DRILL-2054
  public void testConcatOperator() throws Exception {
    testBuilder()
        .sqlQuery("select n_nationkey || '+' || n_name || '=' as CONCAT, n_nationkey, '+' as PLUS, n_name from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperator.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.INT, TypeProtos.MinorType.VARCHAR, TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT", "n_nationkey", "PLUS", "n_name")
        .build().run();

    testBuilder()
        .sqlQuery("select (n_nationkey || n_name) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();


    testBuilder()
        .sqlQuery("select (n_nationkey || cast(n_name as varchar(30))) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();

    testBuilder()
        .sqlQuery("select (cast(n_nationkey as varchar(30)) || n_name) as CONCAT from cp.`tpch/nation.parquet`")
        .ordered()
        .csvBaselineFile("testframework/testExampleQueries/testConcatOperatorInputTypeCombination.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("CONCAT")
        .build().run();
  }

  @Test // see DRILL-985
  public void testViewFileName() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view as select * from cp.`tpch/nation.parquet`;");
    test("select * from dfs.tmp.`nation_view.view.drill`");
    test("drop view nation_view");
  }

  @Test
  public void testTextInClasspathStorage() throws Exception {
    test("select * from cp.`/store/text/classpath_storage_csv_test.csv`");
  }

  @Test
  public void testParquetComplex() throws Exception {
    test("select recipe from cp.`parquet/complex.parquet`");
    test("select * from cp.`parquet/complex.parquet`");
    test("select recipe, c.inventor.name as name, c.inventor.age as age from cp.`parquet/complex.parquet` c");
  }

  @Test // see DRILL-553
  public void testQueryWithNullValues() throws Exception {
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
    test("select count(*) from cp.`customer.json` limit 1");
  }

  @Test
  public void testJoinMerge() throws Exception{
    test("alter session set `planner.enable_hashjoin` = false");
    test("select count(*) \n" +
        "  from (select l.l_orderkey as x, c.c_custkey as y \n" +
        "  from cp.`tpch/lineitem.parquet` l \n" +
        "    left outer join cp.`tpch/customer.parquet` c \n" +
        "      on l.l_orderkey = c.c_custkey) as foo\n" +
        "  where x < 10000\n" +
        "");
    test("alter session set `planner.enable_hashjoin` = true");
  }

  @Test
  public void testJoinExpOn() throws Exception{
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a join cp.`tpch/region.parquet` b on a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey;");
  }

  @Test
  public void testJoinExpWhere() throws Exception{
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a , cp.`tpch/region.parquet` b where a.n_regionkey + 1 = b.r_regionkey and a.n_regionkey + 1 = b.r_regionkey;");
  }

  @Test
  public void testPushExpInJoinConditionInnerJoin() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey + 100  = b.r_regionkey + 200" +      // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
        "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionWhere() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a , cp.`tpch/region.parquet` b " + "" +
        " where a.n_regionkey + 100  = b.r_regionkey + 200" +      // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
        "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionLeftJoin() throws Exception {
    test("select a.n_nationkey, b.r_regionkey from cp.`tpch/nation.parquet` a left join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter
    //    "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') ") ;   // right filter
    //    "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionRightJoin() throws Exception {
    test("select a.n_nationkey, b.r_regionkey from cp.`tpch/nation.parquet` a right join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') ");  // left filter
     //   "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
     //   "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testCaseReturnValueVarChar() throws Exception{
    test("select case when employee_id < 1000 then 'ABC' else 'DEF' end from cp.`employee.json` limit 5");
  }

  @Test
  public void testCaseReturnValueBigInt() throws Exception{
    test("select case when employee_id < 1000 then 1000 else 2000 end from cp.`employee.json` limit 5" );
  }

  @Test
  public void testHashPartitionSV2 () throws Exception{
    test("select count(n_nationkey) from cp.`tpch/nation.parquet` where n_nationkey > 8 group by n_regionkey");
  }

  @Test
  public void testHashPartitionSV4 () throws Exception{
    test("select count(n_nationkey) as cnt from cp.`tpch/nation.parquet` group by n_regionkey order by cnt");
  }

  @Test
  public void testSelectWithLimit() throws Exception{
    test("select employee_id,  first_name, last_name from cp.`employee.json` limit 5 ");
  }

  @Test
  public void testSelectWithLimit2() throws Exception{
    test("select l_comment, l_orderkey from cp.`tpch/lineitem.parquet` limit 10000 ");
  }

  @Test
  public void testSVRV4() throws Exception{
    test("select employee_id,  first_name from cp.`employee.json` order by employee_id ");
  }

  @Test
  public void testSVRV4MultBatch() throws Exception{
    test("select l_orderkey from cp.`tpch/lineitem.parquet` order by l_orderkey limit 10000 ");
  }

  @Test
  public void testSVRV4Join() throws Exception{
    test("select count(*) from cp.`tpch/lineitem.parquet` l, cp.`tpch/partsupp.parquet` ps \n" +
        " where l.l_partkey = ps.ps_partkey and l.l_suppkey = ps.ps_suppkey ;");
  }

  @Test
  public void testText() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select * from dfs_test.`%s`", root);
    test(query);
  }

  @Test
  public void testFilterOnArrayTypes() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select columns[0] from dfs_test.`%s` " +
        " where cast(columns[0] as int) > 1 and cast(columns[1] as varchar(20))='ASIA'", root);
    test(query);
  }

  @Test
  public void testTextPartitions() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/").toURI().toString();
    String query = String.format("select * from dfs_test.`%s`", root);
    test(query);
  }

  @Test
  public void testJoin() throws Exception{
    test("alter session set `planner.enable_hashjoin` = false");
    test("SELECT\n" +
        "  nations.N_NAME,\n" +
        "  regions.R_NAME\n" +
        "FROM\n" +
        "  cp.`tpch/nation.parquet` nations\n" +
        "JOIN\n" +
        "  cp.`tpch/region.parquet` regions\n" +
        "  on nations.N_REGIONKEY = regions.R_REGIONKEY where 1 = 0");
  }


  @Test
  public void testWhere() throws Exception{
    test("select * from cp.`employee.json` ");
  }

  @Test
  public void testGroupBy() throws Exception{
    test("select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainPhysical() throws Exception{
    test("explain plan for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testExplainLogical() throws Exception{
    test("explain plan without implementation for select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status");
  }

  @Test
  public void testGroupScanRowCountExp1() throws Exception {
    test("EXPLAIN plan for select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount1() throws Exception {
    test("select count(n_nationkey) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testColunValueCnt() throws Exception {
    test("select count( 1 + 2) from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCountExp2() throws Exception {
    test("EXPLAIN plan for select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` ");
  }

  @Test
  public void testGroupScanRowCount2() throws Exception {
    test("select count(*) as mycnt, count(*) + 2 * count(*) as mycnt2 from cp.`tpch/nation.parquet` where 1 < 2");
  }

  @Test
  // cast non-exist column from json file. Should return null value.
  public void testDrill428() throws Exception {
    test("select cast(NON_EXIST_COL as varchar(10)) from cp.`employee.json` limit 2; ");
  }

  @Test  // Bugs DRILL-727, DRILL-940
  public void testOrderByDiffColumn() throws Exception {
    test("select r_name from cp.`tpch/region.parquet` order by r_regionkey");
    test("select r_name from cp.`tpch/region.parquet` order by r_name, r_regionkey");
    test("select cast(r_name as varchar(20)) from cp.`tpch/region.parquet` order by r_name");
  }

  @Test  // tests with LIMIT 0
  @Ignore("DRILL-1866")
  public void testLimit0_1() throws Exception {
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0");
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` limit 0 offset 5");
    test("select n_nationkey, n_name from cp.`tpch/nation.parquet` order by n_nationkey limit 0");
    test("select * from cp.`tpch/nation.parquet` limit 0");
    test("select n.n_nationkey from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey limit 0");
    test("select n_regionkey, count(*) from cp.`tpch/nation.parquet` group by n_regionkey limit 0");
  }

  @Test
  public void testTextJoin() throws Exception {
    String root = FileUtils.getResourceAsFile("/store/text/data/nations.csv").toURI().toString();
    String root1 = FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select t1.columns[1] from dfs_test.`%s` t1,  dfs_test.`%s` t2 where t1.columns[0] = t2.columns[0]", root, root1);
    test(query);
  }

  @Test // DRILL-811
  public void testDRILL_811View() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view as select * from cp.`tpch/nation.parquet`;");

    test("select n.n_nationkey, n.n_name, n.n_regionkey from nation_view n where n.n_nationkey > 8 order by n.n_regionkey");

    test("select n.n_regionkey, count(*) as cnt from nation_view n where n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");

    test("drop view nation_view ");
  }

  @Test  // DRILL-811
  public void testDRILL_811ViewJoin() throws Exception {
    test("use dfs.tmp");
    test("create view nation_view as select * from cp.`tpch/nation.parquet`;");
    test("create view region_view as select * from cp.`tpch/region.parquet`;");

    test("select n.n_nationkey, n.n_regionkey, r.r_name from region_view r , nation_view n where r.r_regionkey = n.n_regionkey ");

    test("select n.n_regionkey, count(*) as cnt from region_view r , nation_view n where r.r_regionkey = n.n_regionkey and n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");

    test("select n.n_regionkey, count(*) as cnt from region_view r join nation_view n on r.r_regionkey = n.n_regionkey and n.n_nationkey > 8 group by n.n_regionkey order by n.n_regionkey");

    test("drop view region_view ");
    test("drop view nation_view ");
  }

  @Test  // DRILL-811
  public void testDRILL_811Json() throws Exception {
    test("use dfs.tmp");
    test("create view region_view as select * from cp.`region.json`;");
    test("select sales_city, sales_region from region_view where region_id > 50 order by sales_country; ");
    test("drop view region_view ");
  }

  @Test
  public void testCase() throws Exception {
    test("select case when n_nationkey > 0 and n_nationkey < 2 then concat(n_name, '_abc') when n_nationkey >=2 and n_nationkey < 4 then '_EFG' else concat(n_name,'_XYZ') end from cp.`tpch/nation.parquet` ;");
  }

  @Test // tests join condition that has different input types
  public void testJoinCondWithDifferentTypes() throws Exception {
    test("select t1.department_description from cp.`department.json` t1, cp.`employee.json` t2 where (cast(t1.department_id as double)) = t2.department_id");
    test("select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 where cast(t1.department_id as double) = t2.department_id and cast(t1.position_id as bigint) = t2.department_id");
    test("select t1.full_name from cp.`employee.json` t1, cp.`department.json` t2 where t1.department_id = t2.department_id and t1.position_id = t2.department_id");
  }

  @Test
  public void testTopNWithSV2() throws Exception {
    int actualRecordCount = testSql("select N_NATIONKEY from cp.`tpch/nation.parquet` where N_NATIONKEY < 10 order by N_NATIONKEY limit 5");
    int expectedRecordCount = 5;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testTextQueries() throws Exception {
    test("select cast('285572516' as int) from cp.`tpch/nation.parquet` limit 1");
  }

  @Test // DRILL-1544
  public void testLikeEscape() throws Exception {
    int actualRecordCount = testSql("select id, name from cp.`jsoninput/specialchar.json` where name like '%#_%' ESCAPE '#'");
    int expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

  }

  @Test
  public void testSimilarEscape() throws Exception {
    int actualRecordCount = testSql("select id, name from cp.`jsoninput/specialchar.json` where name similar to '(N|S)%#_%' ESCAPE '#'");
    int expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test
  public void testImplicitDownwardCast() throws Exception {
    int actualRecordCount = testSql("select o_totalprice from cp.`tpch/orders.parquet` where o_orderkey=60000 and o_totalprice=299402");
    int expectedRecordCount = 0;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1470
  public void testCastToVarcharWithLength() throws Exception {
    // cast from varchar with unknown length to a fixed length.
    int actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(first_name as varchar(2)) = 'Sh'");
    int expectedRecordCount = 27;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // cast from varchar with unknown length to varchar(5), then to varchar(10), then to varchar(2). Should produce the same result as the first query.
    actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(cast(cast(first_name as varchar(5)) as varchar(10)) as varchar(2)) = 'Sh'");
    expectedRecordCount = 27;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);


    // this long nested cast expression should be essentially equal to substr(), meaning the query should return every row in the table.
    actualRecordCount = testSql("select first_name from cp.`employee.json` where cast(cast(cast(first_name as varchar(5)) as varchar(10)) as varchar(2)) = substr(first_name, 1, 2)");
    expectedRecordCount = 1155;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // cast is applied to a column from parquet file.
    actualRecordCount = testSql("select n_name from cp.`tpch/nation.parquet` where cast(n_name as varchar(2)) = 'UN'");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1488
  public void testIdentifierMaxLength() throws  Exception {
    // use long column alias name (approx 160 chars)
    test("select employee_id as  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from cp.`employee.json` limit 1");

    // use long table alias name  (approx 160 chars)
    test("select employee_id from cp.`employee.json` as aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa limit 1");

  }

  @Test // DRILL-1846  (this tests issue with SimpleMergeExchange)
  public void testOrderByDiffColumnsInSubqAndOuter() throws Exception {
    String query = "select n.n_nationkey from  (select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` order by n_regionkey) n  order by n.n_nationkey";
    // set slice_target = 1 to force exchanges
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1846  (this tests issue with UnionExchange)
  @Ignore("DRILL-1866")
  public void testLimitInSubqAndOrderByOuter() throws Exception {
    String query = "select t2.n_nationkey from (select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` t1 group by n_nationkey, n_regionkey limit 10) t2 order by t2.n_nationkey";
    // set slice_target = 1 to force exchanges
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1788
  public void testCaseInsensitiveJoin() throws Exception {
    test("select n3.n_name from (select n2.n_name from cp.`tpch/nation.parquet` n1, cp.`tpch/nation.parquet` n2 where n1.N_name = n2.n_name) n3 " +
          " join cp.`tpch/nation.parquet` n4 on n3.n_name = n4.n_name");
  }

  @Test // DRILL-1561
  public void test2PhaseAggAfterOrderBy() throws Exception {
    String query = "select count(*) from (select o_custkey from cp.`tpch/orders.parquet` order by o_custkey)";
    // set slice_target = 1 to force exchanges and 2-phase aggregation
    test("alter session set `planner.slice_target` = 1; " + query);
  }

  @Test // DRILL-1867
  public void testCaseInsensitiveSubQuery() throws Exception {
    int actualRecordCount = 0, expectedRecordCount = 0;

    // source is JSON
    actualRecordCount = testSql("select EMPID from ( select employee_id as empid from cp.`employee.json` limit 2)");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select EMPLOYEE_ID from ( select employee_id from cp.`employee.json` where Employee_id is not null limit 2)");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select x.EMPLOYEE_ID from ( select employee_id from cp.`employee.json` limit 2) X");
    expectedRecordCount = 2;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    // source is PARQUET
    actualRecordCount = testSql("select NID from ( select n_nationkey as nid from cp.`tpch/nation.parquet`) where NID = 3");
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql("select x.N_nationkey from ( select n_nationkey from cp.`tpch/nation.parquet`) X where N_NATIONKEY = 3");
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

      // source is CSV
    String root = FileUtils.getResourceAsFile("/store/text/data/regions.csv").toURI().toString();
    String query = String.format("select rid, x.name from (select columns[0] as RID, columns[1] as NAME from dfs_test.`%s`) X where X.rid = 2", root);
    actualRecordCount = testSql(query);
    expectedRecordCount = 1;
    assertEquals(String.format("Received unexpected number of rows in output: expected=%d, received=%s",
        expectedRecordCount, actualRecordCount), expectedRecordCount, actualRecordCount);

  }

  @Test
  public void testMultipleCountDistinctWithGroupBy() throws Exception {
    String query = "select n_regionkey, count(distinct n_nationkey), count(distinct n_name) from cp.`tpch/nation.parquet` group by n_regionkey;";
    String hashagg_only = "alter session set `planner.enable_hashagg` = true; " +
                          "alter session set `planner.enable_streamagg` = false;";
    String streamagg_only = "alter session set `planner.enable_hashagg` = false; " +
                            "alter session set `planner.enable_streamagg` = true;";

    // hash agg and streaming agg with default slice target (single phase aggregate)
    test(hashagg_only + query);
    test(streamagg_only + query);

    // hash agg and streaming agg with lower slice target (multiphase aggregate)
    test("alter session set `planner.slice_target` = 1; " + hashagg_only + query);
    test("alter session set `planner.slice_target` = 1; " + streamagg_only + query);
  }

  @Test // DRILL-2019
  public void testFilterInSubqueryAndOutside() throws Exception {
    String query1 = "select r_regionkey from (select r_regionkey from cp.`tpch/region.parquet` o where r_regionkey < 2) where r_regionkey > 2";
    String query2 = "select r_regionkey from (select r_regionkey from cp.`tpch/region.parquet` o where r_regionkey < 4) where r_regionkey > 1";
    int actualRecordCount = 0;
    int expectedRecordCount = 0;

    actualRecordCount = testSql(query1);
    assertEquals(expectedRecordCount, actualRecordCount);

    expectedRecordCount = 2;
    actualRecordCount = testSql(query2);
    assertEquals(expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-1973
  public void testLimit0SubqueryWithFilter() throws Exception {
    String query1 = "select * from (select sum(1) as x from  cp.`tpch/region.parquet` limit 0) WHERE x < 10";
    String query2 = "select * from (select sum(1) as x from  cp.`tpch/region.parquet` limit 0) WHERE (0 = 1)";
    int actualRecordCount = 0;
    int expectedRecordCount = 0;

    actualRecordCount = testSql(query1);
    assertEquals(expectedRecordCount, actualRecordCount);

    actualRecordCount = testSql(query2);
    assertEquals(expectedRecordCount, actualRecordCount);
  }

  @Test // DRILL-2063
  public void testAggExpressionWithGroupBy() throws Exception {
    String query = "select l_suppkey, sum(l_extendedprice)/sum(l_quantity) as avg_price \n" +
           " from cp.`tpch/lineitem.parquet` where l_orderkey in \n" +
           " (select o_orderkey from cp.`tpch/orders.parquet` where o_custkey = 2) \n" +
           " and l_suppkey = 4 group by l_suppkey";

    testBuilder()
    .sqlQuery(query)
    .ordered()
    .baselineColumns("l_suppkey", "avg_price")
    .baselineValues(4, 1374.47)
    .build().run();

  }

  @Test // DRILL-1888
  public void testAggExpressionWithGroupByHaving() throws Exception {
    String query = "select l_suppkey, sum(l_extendedprice)/sum(l_quantity) as avg_price \n" +
        " from cp.`tpch/lineitem.parquet` where l_orderkey in \n" +
        " (select o_orderkey from cp.`tpch/orders.parquet` where o_custkey = 2) \n" +
        " group by l_suppkey having sum(l_extendedprice)/sum(l_quantity) > 1850.0";

    testBuilder()
    .sqlQuery(query)
    .ordered()
    .baselineColumns("l_suppkey", "avg_price")
    .baselineValues(98, 1854.95)
    .build().run();
  }

  @Test
  public void testExchangeRemoveForJoinPlan() throws Exception {
    final String WORKING_PATH = TestTools.getWorkingPath();
    final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

    String sql = String.format("select t2.n_nationkey from dfs_test.`%s/tpchmulti/region` t1 join dfs_test.`%s/tpchmulti/nation` t2 on t2.n_regionkey = t1.r_regionkey", TEST_RES_PATH, TEST_RES_PATH);

    testBuilder()
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 10; alter session set `planner.join.row_count_estimate_factor` = 0.1")  // Enforce exchange will be inserted.
        .sqlQuery(sql)
        .optionSettingQueriesForBaseline("alter session set `planner.slice_target` = 100000; alter session set `planner.join.row_count_estimate_factor` = 1.0") // Use default option setting.
        .sqlBaselineQuery(sql)
        .build().run();

  }

  @Test //DRILL-2163
  public void testNestedTypesPastJoinReportsValidResult() throws Exception {
    final String query = "select t1.uid, t1.events, t1.events[0].evnt_id as event_id, t2.transactions, " +
        "t2.transactions[0] as trans, t1.odd, t2.even from cp.`project/complex/a.json` t1, " +
        "cp.`project/complex/b.json` t2 where t1.uid = t2.uid";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .jsonBaselineFile("project/complex/drill-2163-result.json")
        .build()
        .run();
  }

  @Test
  public void testSimilar() throws Exception {
    String query = "select n_nationkey " +
        "from cp.`tpch/nation.parquet` " +
        "where n_name similar to 'CHINA' " +
        "order by n_regionkey";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .optionSettingQueriesForTestQuery("alter session set `planner.slice_target` = 1")
        .baselineColumns("n_nationkey")
        .baselineValues(18)
        .go();

    test("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
  }

}
