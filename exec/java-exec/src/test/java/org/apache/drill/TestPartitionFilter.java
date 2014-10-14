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

import org.apache.drill.common.util.TestTools;
import org.junit.Test;

public class TestPartitionFilter extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPartitionFilter.class);

  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test  //Parquet: basic test with dir0 and dir1 filters in different orders
  public void testPartitionFilter1_Parquet() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir1='Q1' and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test  //Json: basic test with dir0 and dir1 filters in different orders
  public void testPartitionFilter1_Json() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir1='Q1' and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test  //CSV: basic test with dir0 and dir1 filters in different orders
  public void testPartitionFilter1_Csv() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/csv` where dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select * from dfs_test.`%s/multilevel/csv` where dir1='Q1' and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test //Parquet: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Parquet() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir1='Q1' and o_custkey < 1000 and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test //Json: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Json() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where o_custkey < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir1='Q1' and o_custkey < 1000 and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test //CSV: partition filters are combined with regular columns in an AND
  public void testPartitionFilter2_Csv() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/csv` where columns[1] < 1000 and dir0=1994 and dir1='Q1'", TEST_RES_PATH);
    String query2 = String.format("select * from dfs_test.`%s/multilevel/csv` where dir1='Q1' and columns[1] < 1000 and dir0=1994", TEST_RES_PATH);
    test(query1);
    test(query2);
  }

  @Test //Parquet: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Parquet() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/parquet` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    test(query1);
  }

  @Test //Json: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Json() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/json` where (dir0=1994 and dir1='Q1' and o_custkey < 500) or (dir0=1995 and dir1='Q2' and o_custkey > 500)", TEST_RES_PATH);
    test(query1);
  }

  @Test //CSV: partition filters are ANDed and belong to a top-level OR
  public void testPartitionFilter3_Csv() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/csv` where (dir0=1994 and dir1='Q1' and columns[1] < 500) or (dir0=1995 and dir1='Q2' and columns[1] > 500)", TEST_RES_PATH);
    test(query1);
  }

  @Test //Parquet: filters contain join conditions and partition filters
  public void testPartitionFilter4_Parquet() throws Exception {
    String query1 = String.format("select t1.dir0, t1.dir1, t1.o_custkey, t1.o_orderdate, cast(t2.c_name as varchar(10)) from dfs_test.`%s/multilevel/parquet` t1, cp.`tpch/customer.parquet` t2 where t1.o_custkey = t2.c_custkey and t1.dir0=1994 and t1.dir1='Q1'", TEST_RES_PATH);
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
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/parquet` where dir0 in (1995, 1996)", TEST_RES_PATH);
    test(query1);
  }

  @Test // Json: IN filter
  public void testPartitionFilter5_Json() throws Exception {
    String query1 = String.format("select dir0, dir1, o_custkey, o_orderdate from dfs_test.`%s/multilevel/json` where dir0 in (1995, 1996)", TEST_RES_PATH);
    test(query1);
  }

  @Test // CSV: IN filter
  public void testPartitionFilter5_Csv() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/csv` where dir0 in (1995, 1996)", TEST_RES_PATH);
    test(query1);
  }

}
