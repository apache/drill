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
import org.apache.drill.exec.rpc.RpcException;
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

  @Test
  public void testSelStarSubQJson2() throws Exception {
    test("select v.first_name from (select * from cp.`employee.json`) v limit 2" );
  }

  // Select * in SubQuery,  View  or CTE (With clause)
  @Test  // Select * in SubQuery : regular columns appear in select clause, where, group by, order by.
  public void testSelStarSubQPrefix() throws Exception {
    test("select t.n_nationkey, t.n_name, t.n_regionkey from (select * from cp.`tpch/nation.parquet`) t where t.n_regionkey > 1 order by t.n_name" );

    test("select n.n_regionkey, count(*) as cnt from ( select * from ( select * from cp.`tpch/nation.parquet`) t where t.n_nationkey < 10 ) n where n.n_nationkey >1 group by n.n_regionkey order by n.n_regionkey ; ");

    test("select t.n_regionkey, count(*) as cnt from (select * from cp.`tpch/nation.parquet`) t where t.n_nationkey > 1 group by t.n_regionkey order by t.n_regionkey;" );
  }

  @Test  // Select * in SubQuery : regular columns appear in select clause, where, group by, order by.
  public void testSelStarSubQNoPrefix() throws Exception {
    test("select n_nationkey, n_name, n_regionkey from (select * from cp.`tpch/nation.parquet`)  where n_regionkey > 1 order by n_name" );

    test("select n_regionkey, count(*) as cnt from ( select * from ( select * from cp.`tpch/nation.parquet`)  where n_nationkey < 10 ) where n_nationkey >1 group by n_regionkey order by n_regionkey ; ");

    test("select n_regionkey, count(*) as cnt from (select * from cp.`tpch/nation.parquet`) t where n_nationkey > 1 group by n_regionkey order by n_regionkey;" );
  }

  @Test  // join two SubQuery, each having select * : regular columns appear in the select , where and on clause, group by, order by.
  public void testSelStarSubQJoin() throws Exception {
    // select clause, where.
    test(" select n.n_nationkey, n.n_name, n.n_regionkey, r.r_name \n" +
         " from (select * from cp.`tpch/nation.parquet`) n, \n" +
         "      (select * from cp.`tpch/region.parquet`) r \n" +
         " where n.n_regionkey = r.r_regionkey " );

    // select clause, where, group by, order by
    test(" select n.n_regionkey, count(*) as cnt \n" +
         " from (select * from cp.`tpch/nation.parquet`) n  \n" +
         "    , (select * from cp.`tpch/region.parquet`) r  \n" +
         " where n.n_regionkey = r.r_regionkey and n.n_nationkey > 10 \n" +
         " group by n.n_regionkey \n" +
         " order by n.n_regionkey; " );

    // select clause, where, on, group by, order by.
    test(" select n.n_regionkey, count(*) as cnt \n" +
         " from   (select * from cp.`tpch/nation.parquet`) n  \n" +
         "   join (select * from cp.`tpch/region.parquet`) r  \n" +
         " on n.n_regionkey = r.r_regionkey \n" +
         " where n.n_nationkey > 10 \n" +
         " group by n.n_regionkey \n" +
         " order by n.n_regionkey; " );

    // Outer query use select *. Join condition in where clause.
    test(" select *  \n" +
         " from (select * from cp.`tpch/nation.parquet`) n \n" +
         "    , (select * from cp.`tpch/region.parquet`) r \n" +
         " where n.n_regionkey = r.r_regionkey " );

    // Outer query use select *. Join condition in on clause.
    test(" select *  \n" +
         " from (select * from cp.`tpch/nation.parquet`) n \n" +
         "    join (select * from cp.`tpch/region.parquet`) r \n" +
         " on n.n_regionkey = r.r_regionkey " );
  }

  @Test // DRILL-595 : Select * in CTE WithClause : regular columns appear in select clause, where, group by, order by.
  public void testDRILL_595WithClause() throws Exception {
    test(" with x as (select * from cp.`region.json`) \n" +
         " select x.region_id, x.sales_city \n" +
         " from x where x.region_id > 10 limit 5;");

    test(" with x as (select * from cp.`region.json`) \n" +
        " select region_id, sales_city \n" +
        " from x where region_id > 10 limit 5;");

    test(" with x as (select * from cp.`tpch/nation.parquet`) \n" +
         " select x.n_regionkey, count(*) as cnt \n" +
         " from x \n" +
         " where x.n_nationkey > 5 \n" +
         " group by x.n_regionkey \n" +
         " order by cnt limit 5; ");

  }

  @Test // DRILL-595 : Join two CTE, each having select * : regular columns appear in the select , where and on clause, group by, order by.
  public void testDRILL_595WithClauseJoin() throws Exception {
    test("with n as (select * from cp.`tpch/nation.parquet`), \n " +
        "     r as (select * from cp.`tpch/region.parquet`) \n" +
        "select n.n_nationkey, n.n_name, n.n_regionkey, r.r_name \n" +
        "from  n, r \n" +
        "where n.n_regionkey = r.r_regionkey ;" );

    test("with n as (select * from cp.`tpch/nation.parquet`), \n " +
        "     r as (select * from cp.`tpch/region.parquet`) \n" +
        "select n.n_regionkey, count(*) as cnt \n" +
        "from  n, r \n" +
        "where n.n_regionkey = r.r_regionkey  and n.n_nationkey > 5 \n" +
        "group by n.n_regionkey \n" +
        "order by cnt;" );
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

  @Test(expected = RpcException.class)  // Should get "At line 1, column 8: Column 'n_nationkey' is ambiguous"
  public void testSelStarAmbiguousJoin() throws Exception {
    test("select x.n_nationkey, x.n_name, x.n_regionkey, x.r_name from (select * from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey) x " ) ;
  }

  @Test  // select star for a SchemaTable.
  public void testSelStarSubQSchemaTable() throws Exception {
    test("select name, kind, type from (select * from sys.options);");
  }

  @Test  // Join a select star of SchemaTable, with a select star of Schema-less table.
  public void testSelStarJoinSchemaWithSchemaLess() throws Exception {
    test("select t1.name, t1.kind, t2.n_nationkey from (select * from sys.options) t1 join (select * from cp.`tpch/nation.parquet`) t2 on t1.name = t2.n_name;");
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
}
