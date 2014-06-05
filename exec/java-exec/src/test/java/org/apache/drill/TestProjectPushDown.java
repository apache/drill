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

import org.junit.Ignore;
import org.junit.Test;

// Test the optimizer plan in terms of project pushdown.
// When a query refers to a subset of columns in a table, optimizer should push the list
// of refereed columns to the SCAN operator, so that SCAN operator would only retrieve
// the column values in the subset of columns.

public class TestProjectPushDown extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(TestProjectPushDown.class);

  @Test
  @Ignore
  public void testGroupBy() throws Exception {
    String expectedColNames = " \"columns\" : [ \"`marital_status`\" ]";
    testPhysicalPlan(
        "select marital_status, COUNT(1) as cnt from cp.`employee.json` group by marital_status",
        expectedColNames);
  }

  @Test
  @Ignore
  public void testOrderBy() throws Exception {
    String expectedColNames = "\"columns\" : [ \"`employee_id`\", \"`full_name`\", \"`first_name`\", \"`last_name`\" ]";
    testPhysicalPlan("select employee_id , full_name, first_name , last_name "
        + "from cp.`employee.json` order by first_name, last_name",
        expectedColNames);
  }

  @Test
  @Ignore
  public void testExprInSelect() throws Exception {
    String expectedColNames = "\"columns\" : [ \"`employee_id`\", \"`full_name`\", \"`first_name`\", \"`last_name`\" ]";
    testPhysicalPlan(
        "select employee_id + 100, full_name, first_name , last_name "
            + "from cp.`employee.json` order by first_name, last_name",
            expectedColNames);
  }

  @Test
  @Ignore
  public void testExprInWhere() throws Exception {
    String expectedColNames = "\"columns\" : [ \"`employee_id`\", \"`full_name`\", \"`first_name`\", \"`last_name`\" ]";
    testPhysicalPlan(
        "select employee_id + 100, full_name, first_name , last_name "
            + "from cp.`employee.json` where employee_id + 500 < 1000 ",
            expectedColNames);
  }

  @Test
  public void testJoin() throws Exception {
    String expectedColNames1 = "\"columns\" : [ \"`N_REGIONKEY`\", \"`N_NAME`\" ]";
    String expectedColNames2 = "\"columns\" : [ \"`R_REGIONKEY`\", \"`R_NAME`\" ]";

    testPhysicalPlan("SELECT\n" + "  nations.N_NAME,\n" + "  regions.R_NAME\n"
        + "FROM\n"
        + "  dfs.`[WORKING_PATH]/../../sample-data/nation.parquet` nations\n"
        + "JOIN\n"
        + "  dfs.`[WORKING_PATH]/../../sample-data/region.parquet` regions\n"
        + "  on nations.N_REGIONKEY = regions.R_REGIONKEY", expectedColNames1,
        expectedColNames2);
  }

  @Test
  @Ignore  // InfoSchema do not support project pushdown currently.
  public void testFromInfoSchema() throws Exception {
    String expectedColNames = " \"columns\" : [ \"`CATALOG_DESCRIPTION`\" ]";
    testPhysicalPlan(
        "select count(CATALOG_DESCRIPTION) from INFORMATION_SCHEMA.CATALOGS",
        expectedColNames);
  }

  @Test
  public void testTPCH1() throws Exception {
    String expectedColNames = " \"columns\" : [ \"`l_returnflag`\", \"`l_linestatus`\", \"`l_shipdate`\", \"`l_quantity`\", \"`l_extendedprice`\", \"`l_discount`\", \"`l_tax`\" ]";
    testPhysicalPlanFromFile("queries/tpch/01.sql", expectedColNames);
  }

  @Test
  public void testTPCH3() throws Exception {
    String expectedColNames1 = "\"columns\" : [ \"`c_mktsegment`\", \"`c_custkey`\" ]";
    String expectedColNames2 = " \"columns\" : [ \"`o_orderdate`\", \"`o_shippriority`\", \"`o_custkey`\", \"`o_orderkey`\" ";
    String expectedColNames3 = "\"columns\" : [ \"`l_orderkey`\", \"`l_shipdate`\", \"`l_extendedprice`\", \"`l_discount`\" ]";
    testPhysicalPlanFromFile("queries/tpch/03.sql", expectedColNames1, expectedColNames2, expectedColNames3);
  }

  private void testPhysicalPlanFromFile(String fileName, String... expectedSubstrs)
      throws Exception {
    String query = getFile(fileName);
    String[] queries = query.split(";");
    for (String q : queries) {
      if (q.trim().isEmpty())
        continue;
      testPhysicalPlan(q, expectedSubstrs);
    }
  }

}
