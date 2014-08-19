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

import org.apache.drill.common.util.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

public class TestExampleQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

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
  public void testSelStarOrderBy() throws Exception{
    test("select * from cp.`employee.json` order by last_name");
  }

  @Test
  public void testSelStarOrderByLimit() throws Exception{
    test("select * from cp.`employee.json` order by employee_id limit 2;");
  }

  @Test
  public void testSelStarPlusRegCol() throws Exception{
    test("select *, n_nationkey from cp.`tpch/nation.parquet` limit 2;");
  }

  @Test
  public void testSelStarWhereOrderBy() throws Exception{
    test("select * from cp.`employee.json` where first_name = 'James' order by employee_id");
  }

  @Test
  public void testSelStarJoin() throws Exception {
    test("select * from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name;");
  }

  @Test
  public void testSelLeftStarJoin() throws Exception {
    test("select n.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name;");
  }

  @Test
  public void testSelRightStarJoin() throws Exception {
    test("select r.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name;");
  }

  @Test
  public void testSelStarRegColConstJoin() throws Exception {
    test("select *, n.n_nationkey, 1 + 2 as constant from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name;");
  }

  @Test
  public void testSelStarBothSideJoin() throws Exception {
    test("select n.*, r.* from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey;");
  }

  @Test
  public void testSelStarJoinSameColName() throws Exception {
    test("select * from cp.`tpch/nation.parquet` n1, cp.`tpch/nation.parquet` n2 where n1.n_nationkey = n2.n_nationkey;");
  }

  @Test // DRILL-1293
  public void testStarView1() throws Exception { 
    test("use dfs.tmp");  
    test("create view vt1 as select * from cp.`tpch/region.parquet` r, cp.`tpch/nation.parquet` n where r.r_regionkey = n.n_regionkey");
    test("select * from vt1");
    test("drop view vt1");
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
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a left join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
        "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
  }

  @Test
  public void testPushExpInJoinConditionRightJoin() throws Exception {
    test("select a.n_nationkey from cp.`tpch/nation.parquet` a right join cp.`tpch/region.parquet` b " + "" +
        " on a.n_regionkey +100 = b.r_regionkey +200 " +        // expressions in both sides of equal join filter
        "   and (substr(a.n_name,1,3)= 'L1' or substr(a.n_name,2,2) = 'L2') " +  // left filter
        "   and (substr(b.r_name,1,3)= 'R1' or substr(b.r_name,2,2) = 'R2') " +  // right filter
        "   and (substr(a.n_name,2,3)= 'L3' or substr(b.r_name,3,2) = 'R3');");  // non-equal join filter
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

}
