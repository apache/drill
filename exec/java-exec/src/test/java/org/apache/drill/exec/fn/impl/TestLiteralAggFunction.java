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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for LITERAL_AGG support introduced in Calcite 1.35.
 * LITERAL_AGG is an internal aggregate function that Calcite uses to optimize
 * queries with constant values in the SELECT list of an aggregate query.
 *
 * These tests verify that queries with constants in aggregate contexts work correctly.
 * The LITERAL_AGG optimization may or may not be used depending on Calcite's decisions,
 * but when it IS used (as in TPCH queries), our implementation must handle it correctly.
 */
@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestLiteralAggFunction extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testConstantInAggregateQuery() throws Exception {
    // Test that constant values in aggregate queries work correctly
    // Calcite 1.35+ may use LITERAL_AGG internally for optimization
    String query = "SELECT department_id, 42 as const_val, COUNT(*) as cnt " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id = 1 " +
                   "GROUP BY department_id";

    // Verify query returns the correct constant value
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department_id", "const_val", "cnt")
        .baselineValues(1L, 42, 7L)
        .go();

    // Verify the plan contains expected operations
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
  }

  @Test
  public void testMultipleConstantsInAggregate() throws Exception {
    // Test multiple constants with different types
    String query = "SELECT " +
                   "department_id, " +
                   "100 as int_const, " +
                   "'test' as str_const, " +
                   "COUNT(*) as cnt " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id = 1 " +
                   "GROUP BY department_id";

    // Verify all constant values are correct
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department_id", "int_const", "str_const", "cnt")
        .baselineValues(1L, 100, "test", 7L)
        .go();

    // Verify the plan is valid
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
  }

  @Test
  public void testConstantWithoutGroupBy() throws Exception {
    // Test constant in aggregate query without GROUP BY
    String query = "SELECT 999 as const_val, COUNT(*) as cnt " +
                   "FROM cp.`employee.json`";

    // Verify the query executes successfully and returns correct values
    long result = queryBuilder()
        .sql(query)
        .run()
        .recordCount();

    assertEquals("Should return 1 row (no GROUP BY means single aggregate)", 1, result);

    // Verify constant value is correct
    int constVal = queryBuilder().sql(query).singletonInt();
    assertEquals("Constant value should be 999", 999, constVal);

    // Verify the plan contains aggregate or scan operation
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate or scan operation",
        plan.toLowerCase().contains("aggregate") ||
        plan.toLowerCase().contains("hashagg") ||
        plan.toLowerCase().contains("scan"));
  }

  @Test
  public void testExplainPlanWithConstant() throws Exception {
    // Check that EXPLAIN works correctly for queries with constants
    String query = "SELECT department_id, 'constant' as val, COUNT(*) " +
                   "FROM cp.`employee.json` " +
                   "GROUP BY department_id";

    // Verify the explain plan executes and contains expected elements
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
    assertTrue("Plan should reference employee.json",
        plan.toLowerCase().contains("employee"));
  }

  @Test
  public void testConstantNullValue() throws Exception {
    // Test NULL constant in aggregate
    String query = "SELECT department_id, CAST(NULL AS INTEGER) as null_val, COUNT(*) as cnt " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id = 1 " +
                   "GROUP BY department_id";

    // Verify the query executes and NULL is handled correctly
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department_id", "null_val", "cnt")
        .baselineValues(1L, null, 7L)
        .go();

    // Verify the plan is valid
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
  }

  @Test
  public void testConstantExpression() throws Exception {
    // Test constant expression (not just literal) in aggregate
    String query = "SELECT department_id, 10 + 32 as expr_val, COUNT(*) as cnt " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id IN (1, 2) " +
                   "GROUP BY department_id " +
                   "ORDER BY department_id";

    // Verify the constant expression evaluates correctly
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("department_id", "expr_val", "cnt")
        .baselineValues(1L, 42, 7L)
        .baselineValues(2L, 42, 5L)
        .go();

    // Verify the plan contains expected operations
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
  }

  @Test
  public void testMixedAggregatesAndConstants() throws Exception {
    // Test mixing regular aggregates with constants
    String query = "SELECT " +
                   "department_id, " +
                   "COUNT(*) as cnt, " +
                   "'dept' as label, " +
                   "SUM(employee_id) as sum_id, " +
                   "100 as version " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id = 1 " +
                   "GROUP BY department_id";

    // Verify constants are correct alongside real aggregates
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department_id", "cnt", "label", "sum_id", "version")
        .baselineValues(1L, 7L, "dept", 75L, 100)
        .go();

    // Verify the plan contains aggregate operations
    String plan = queryBuilder().sql(query).explainText();
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
    assertTrue("Plan should contain SUM operation",
        plan.toLowerCase().contains("sum"));
  }

  @Test
  public void testQueryPlanWithConstants() throws Exception {
    // Verify that queries with constants produce valid execution plans
    String query = "SELECT department_id, 42 as const_val, COUNT(*) as cnt " +
                   "FROM cp.`employee.json` " +
                   "WHERE department_id = 1 " +
                   "GROUP BY department_id";

    String plan = queryBuilder().sql(query).explainText();

    // Verify the plan contains expected components
    assertTrue("Plan should contain aggregate operation",
        plan.toLowerCase().contains("aggregate") || plan.toLowerCase().contains("hashagg"));
    assertTrue("Plan should reference employee.json",
        plan.toLowerCase().contains("employee"));
    assertTrue("Plan should contain department_id",
        plan.toLowerCase().contains("department_id"));

    // Verify the query executes correctly and returns expected values
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("department_id", "const_val", "cnt")
        .baselineValues(1L, 42, 7L)
        .go();
  }
}
