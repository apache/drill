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
package com.mapr.drill.maprdb.tests.json;

import java.util.List;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.mapr.drill.maprdb.tests.MaprDBTestsSuite;
import com.mapr.tests.annotations.ClusterTest;

@Category(ClusterTest.class)
public class TestSimpleJson extends BaseTestQuery {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    MaprDBTestsSuite.setupTests();
    MaprDBTestsSuite.createPluginAndGetConf(getDrillbitContext());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MaprDBTestsSuite.cleanupTests();
  }

  @Test
  public void testMe() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business";
    runSQLAndVerifyCount(sql, 10);
  }

  @Test
  public void testPushdownStringEqual() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " name = 'Sprint'"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(name = \"Sprint\"\\)"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushdownStringLike() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " name LIKE 'S%'"
        ;
    runSQLAndVerifyCount(sql, 3);

    final String[] expectedPlan = {"condition=\\(name MATCHES \"\\^\\\\\\\\QS\\\\\\\\E\\.\\*\\$\"\\)"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushdownStringNotEqual() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " name <> 'Sprint'"
        ;
    runSQLAndVerifyCount(sql, 9);

    final String[] expectedPlan = {"condition=\\(name != \"Sprint\"\\)"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushdownLongEqual() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " zip = 85260"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(zip = \\{\"\\$numberLong\":85260\\}\\)"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testCompositePredicate() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " zip = 85260\n"
        + " OR\n"
        + " city = 'Las Vegas'"
        ;
    runSQLAndVerifyCount(sql, 4);

    final String[] expectedPlan = {"condition=\\(\\(zip = \\{\"\\$numberLong\":85260\\}\\) or \\(city = \"Las Vegas\"\\)\\)"};
    final String[] excludedPlan = {};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPruneScanRange() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " _id = 'jFTZmywe7StuZ2hEjxyA'"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(_id = \"jFTZmywe7StuZ2hEjxyA\"\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPruneScanRangeAndPushDownCondition() throws Exception {
    // XXX/TODO:
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, categories, full_address\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " _id = 'jFTZmywe7StuZ2hEjxyA' AND\n"
        + " name = 'Subway'"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(\\(_id = \"jFTZmywe7StuZ2hEjxyA\"\\) and \\(name = \"Subway\"\\)\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownOnSubField1() throws Exception {
    setColumnWidths(new int[] {25, 120, 20});
    final String sql = "SELECT\n"
        + "  _id, name, b.attributes.Ambience.touristy attributes\n"
        + "FROM\n"
        + "  hbase.`business` b\n"
        + "WHERE\n"
        + " b.`attributes.Ambience.casual` = false"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(attributes.Ambience.casual = false\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownOnSubField2() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, b.attributes.Attire attributes\n"
        + "FROM\n"
        + "  hbase.`business` b\n"
        + "WHERE\n"
        + " b.`attributes.Attire` = 'casual'"
        ;
    runSQLAndVerifyCount(sql, 4);

    final String[] expectedPlan = {"condition=\\(attributes.Attire = \"casual\"\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }
  @Test
  public void testPushDownIsNull() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});

    final String sql = "SELECT\n"
        + "  _id, name, attributes\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`attributes.Ambience.casual` IS NULL"
        ;
    runSQLAndVerifyCount(sql, 7);

    final String[] expectedPlan = {"condition=\\(attributes.Ambience.casual = null\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownIsNotNull() throws Exception {
    setColumnWidths(new int[] {25, 75, 75, 50});

    final String sql = "SELECT\n"
        + "  _id, name, b.attributes.Parking\n"
        + "FROM\n"
        + "  hbase.`business` b\n"
        + "WHERE\n"
        + " b.`attributes.Ambience.casual` IS NOT NULL"
        ;
    runSQLAndVerifyCount(sql, 3);

    final String[] expectedPlan = {"condition=\\(attributes.Ambience.casual != null\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownOnSubField3() throws Exception {
    setColumnWidths(new int[] {25, 40, 40, 40});
    final String sql = "SELECT\n"
        + "  _id, name, b.attributes.`Accepts Credit Cards` attributes\n"
        + "FROM\n"
        + "  hbase.`business` b\n"
        + "WHERE\n"
        + " b.`attributes.Accepts Credit Cards` IS NULL"
        ;
    runSQLAndVerifyCount(sql, 3);

    final String[] expectedPlan = {"condition=\\(attributes.Accepts Credit Cards = null\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownLong() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " stars > 4.0"
        ;
    runSQLAndVerifyCount(sql, 2);

    final String[] expectedPlan = {"condition=\\(stars > 4\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  @Test
  public void testPushDownSubField4() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`attributes.Good For.lunch` = true AND"
        + " stars > 4.1"
        ;
    runSQLAndVerifyCount(sql, 1);

    final String[] expectedPlan = {"condition=\\(\\(attributes.Good For.lunch = true\\) and \\(stars > 4.1\\)\\)"};
    final String[] excludedPlan ={};

    PlanTestBase.testPlanMatchingPatterns(sql, expectedPlan, excludedPlan);
  }

  /*
  @Test
  public void testPushDownSubField5() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`hours.Tuesday.open` < TIME '10:30:00'"
        ;
    runSQLAndVerifyCount(sql, 1);
  }

  @Test
  public void testPushDownSubField6() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`hours.Sunday.close` > TIME '20:30:00'"
        ;
    runSQLAndVerifyCount(sql, 4);
  }

  @Test
  public void testPushDownSubField7() throws Exception {
    setColumnWidths(new int[] {25, 40, 25, 45});
    final String sql = "SELECT\n"
        + "  _id, name, start_date, last_update\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`start_date` = DATE '2012-07-14'"
        ;
    runSQLAndVerifyCount(sql, 1);
  }

  @Test
  public void testPushDownSubField8() throws Exception {
    setColumnWidths(new int[] {25, 40, 25, 45});
    final String sql = "SELECT\n"
        + "  _id, name, start_date, last_update\n"
        + "FROM\n"
        + "  hbase.`business` business\n"
        + "WHERE\n"
        + " business.`last_update` = TIMESTAMP '2012-10-20 07:42:46'"
        ;
    runSQLAndVerifyCount(sql, 1);
  }
  */

  protected List<QueryDataBatch> runHBaseSQLlWithResults(String sql) throws Exception {
    System.out.println("Running query:\n" + sql);
    return testSqlWithResults(sql);
  }

  protected void runSQLAndVerifyCount(String sql, int expectedRowCount) throws Exception{
    List<QueryDataBatch> results = runHBaseSQLlWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }

}
