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

public class TestUnionAll extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestUnionAll.class);
  static final String WORKING_PATH = TestTools.getWorkingPath();
  static final String TEST_RES_PATH = WORKING_PATH + "/src/test/resources";

  @Test    // Simple Union-All over two scans
  public void testUnionAll1() throws Exception {
    test("select n_regionkey from cp.`tpch/nation.parquet` union all select r_regionkey from cp.`tpch/region.parquet`");
  }

  @Test  // Union-All over inner joins
  public void testUnionAll2() throws Exception {
    test("select n1.n_nationkey from cp.`tpch/nation.parquet` n1 inner join cp.`tpch/region.parquet` r1 on n1.n_regionkey = r1.r_regionkey where n1.n_nationkey in (1, 2)  union all select n2.n_nationkey from cp.`tpch/nation.parquet` n2 inner join cp.`tpch/region.parquet` r2 on n2.n_regionkey = r2.r_regionkey where n2.n_nationkey in (3, 4)");
  }

  @Test  // Union-All over grouped aggregates
  public void testUnionAll3() throws Exception {
    test("select n1.n_nationkey from cp.`tpch/nation.parquet` n1 where n1.n_nationkey in (1, 2) group by n1.n_nationkey union all select r1.r_regionkey from cp.`tpch/region.parquet` r1 group by r1.r_regionkey");
  }

  @Test    // Chain of Union-Alls
  public void testUnionAll4() throws Exception {
    test("select n_regionkey from cp.`tpch/nation.parquet` union all select r_regionkey from cp.`tpch/region.parquet` union all select n_nationkey from cp.`tpch/nation.parquet` union all select c_custkey from cp.`tpch/customer.parquet` where c_custkey < 5");
  }

  @Test  // Union-All of all columns in the table
  public void testUnionAll5() throws Exception {
    test("select * from cp.`tpch/region.parquet` r1 union all select * from cp.`tpch/region.parquet` r2");
  }

  @Test // Union-All where same column is projected twice in right child
  public void testUnionAll6() throws Exception {
    test("select n_nationkey, n_regionkey from cp.`tpch/nation.parquet` where n_regionkey = 1 union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet` where r_regionkey = 2");
  }

  @Test // Union-All where same column is projected twice in left and right child
  public void testUnionAll6_1() throws Exception {
    test("select n_nationkey, n_nationkey from cp.`tpch/nation.parquet` union all select r_regionkey, r_regionkey from cp.`tpch/region.parquet`");
  }

  @Test  // Union-all of two string literals of different lengths
  public void testUnionAll7() throws Exception {
    test("select 'abc' from cp.`tpch/region.parquet` union all select 'abcdefgh' from cp.`tpch/region.parquet`");
  }

  @Test  // Union-all of two character columns of different lengths
  public void testUnionAll8() throws Exception {
    test("select n_name from cp.`tpch/nation.parquet` union all select r_comment from cp.`tpch/region.parquet`");
  }

  @Test // DRILL-1905: Union-all of * column from JSON files in different directories
  public void testUnionAll9() throws Exception {
    String query1 = String.format("select * from dfs_test.`%s/multilevel/json/1994/Q1/orders_94_q1.json` " +
             " union all select * from dfs_test.`%s/multilevel/json/1995/Q1/orders_95_q1.json`",
             TEST_RES_PATH, TEST_RES_PATH);
    test(query1);
  }

}
