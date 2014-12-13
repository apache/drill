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

import org.junit.Test;
import org.apache.drill.exec.rpc.RpcException;

public class TestStarQueries extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestStarQueries.class);

  @Test
  public void testSelStarOrderBy() throws Exception{
    test("select * from cp.`employee.json` order by last_name");
  }

  @Test
  public void testSelStarOrderByLimit() throws Exception{
    test("select * from cp.`employee.json` order by last_name limit 2;");
  }

  @Test
  public void testSelStarPlusRegCol() throws Exception{
    test("select *, n_nationkey from cp.`tpch/nation.parquet` limit 2;");
  }

  @Test
  public void testSelStarWhereOrderBy() throws Exception{
    test("select * from cp.`employee.json` where first_name = 'James' order by last_name");
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
    test("select *, n.n_nationkey as n_nationkey0, 1 + 2 as constant from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey order by n.n_name;");
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

  @Test  // select star for a SchemaTable.
  public void testSelStarSubQSchemaTable() throws Exception {
    test("select name, kind, type from (select * from sys.options);");
  }

  @Test  // Join a select star of SchemaTable, with a select star of Schema-less table.
  public void testSelStarJoinSchemaWithSchemaLess() throws Exception {
    test("select t1.name, t1.kind, t2.n_nationkey from (select * from sys.options) t1 join (select * from cp.`tpch/nation.parquet`) t2 on t1.name = t2.n_name;");
  }

  @Test // see DRILL-1811
  public void testSelStarDifferentColumnOrder() throws Exception {
    test("select first_name, * from cp.`employee.json`;");
    test("select *, first_name, *, last_name from cp.`employee.json`;");
  }

  @Test(expected = RpcException.class)  // Should get "At line 1, column 8: Column 'n_nationkey' is ambiguous"
  public void testSelStarAmbiguousJoin() throws Exception {
    try {
      test("select x.n_nationkey, x.n_name, x.n_regionkey, x.r_name from (select * from cp.`tpch/nation.parquet` n, cp.`tpch/region.parquet` r where n.n_regionkey = r.r_regionkey) x " ) ;
    } catch (RpcException e) {
      logger.info("***** Test resulted in expected failure: " + e.getMessage());
      throw e;
    }
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

  @Test
  public void testSelectStartSubQueryJoinWithWhereClause() throws Exception {
    // select clause, where, on, group by, order by.
    test(" select n.n_regionkey, count(*) as cnt \n" +
        " from   (select * from cp.`tpch/nation.parquet`) n  \n" +
        "   join (select * from cp.`tpch/region.parquet`) r  \n" +
        " on n.n_regionkey = r.r_regionkey \n" +
        " where n.n_nationkey > 10 \n" +
        " group by n.n_regionkey \n" +
        " order by n.n_regionkey; " );
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

}
