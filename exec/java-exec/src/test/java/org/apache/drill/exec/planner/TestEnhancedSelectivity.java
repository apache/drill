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
package org.apache.drill.exec.planner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.cost.DrillRelDefaultMdSelectivity;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * Tests enhanced filter selectivity. See DRILL-5254.
 */

public class TestEnhancedSelectivity {

  // Compute the expected probabilities according to the
  // rules outlined in DRILL-5254.

  public static final double PROB_A_EQ_B = DrillRelDefaultMdSelectivity.PROB_A_EQ_B;
  public static final double PROB_A_NE_B = 1 - PROB_A_EQ_B;
  public static final double PROB_A_LT_B = PROB_A_NE_B / 2;
  public static final double PROB_A_GT_B = PROB_A_NE_B / 2;
  public static final double PROB_A_LE_B = PROB_A_LT_B + PROB_A_EQ_B;
  public static final double PROB_A_GE_B = PROB_A_GT_B + PROB_A_EQ_B;
  public static final double PROB_A_LIKE_B = DrillRelDefaultMdSelectivity.PROB_A_LIKE_B;
  public static final double PROB_A_NOT_LIKE_B = 1 - PROB_A_LIKE_B;
  public static final double PROB_A_IS_NULL = DrillRelDefaultMdSelectivity.PROB_A_IS_NULL;
  public static final double PROB_A_NOT_NULL = 1 - PROB_A_IS_NULL;
  public static final double PROB_A_IN_B = PROB_A_EQ_B;
  public static final double PROB_A_BETWEEN_B_C = PROB_A_LE_B * PROB_A_GE_B;
  public static final double PROB_A_IS_TRUE = DrillRelDefaultMdSelectivity.PROB_A_IS_TRUE;
  public static final double PROB_A_IS_FALSE = 1 - PROB_A_IS_TRUE;
  public static final double PROB_A_NOT_TRUE = PROB_A_IS_FALSE + PROB_A_IS_NULL;
  public static final double PROB_A_NOT_FALSE = PROB_A_IS_TRUE + PROB_A_IS_NULL;
  public static final double DEFAULT_PROB = DrillRelDefaultMdSelectivity.DEFAULT_PROB;

  /**
   * Verify the default enhanced selectivity rules.
   *
   * @throws Exception
   */
  @Test
  public void testEnhancedDefaults() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_ENABLE, true)
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//        .configProperty(ExecConstants.QUERY_PROFILE_OPTION, "sync") // Temporary until DRILL-5257 is available
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      verifyBooleanBehavior(client);
      verifyReduction(client, "id_i = 10", PROB_A_EQ_B);
      verifyReduction(client, "id_i <> 10", PROB_A_NE_B);
      verifyReduction(client, "id_i < 10", PROB_A_LT_B);
      verifyReduction(client, "id_i > 10", PROB_A_GT_B);
      verifyReduction(client, "id_i <= 10", PROB_A_LE_B);
      verifyReduction(client, "id_i >= 10", PROB_A_GE_B);
      verifyReduction(client, "id_i IN (10)", PROB_A_IN_B);
      verifyReduction(client, "id_i IN (10, 20)", 2 * PROB_A_IN_B - Math.pow(PROB_A_IN_B, 2));
      verifyReduction(client, "id_i IN (11, 12, 13, 14, 15, 16, 16, 18, 19, 20)", DrillRelDefaultMdSelectivity.MAX_OR_FACTOR);
      verifyReduction(client, "name_s20 LIKE 'foo'", PROB_A_LIKE_B);
      verifyReduction(client, "name_s20 NOT LIKE 'foo'", PROB_A_NOT_LIKE_B);
      verifyReduction(client, "name_s20 IS NULL", PROB_A_IS_NULL);
      verifyReduction(client, "name_s20 IS NOT NULL", PROB_A_NOT_NULL);
      verifyReduction(client, "NOT ( id_i = 10 )", 1.0 - PROB_A_EQ_B);
      verifyReduction(client, "flag_b IS TRUE", PROB_A_IS_TRUE);
      verifyReduction(client, "flag_b IS FALSE", PROB_A_IS_FALSE);
      verifyReduction(client, "flag_b IS NOT TRUE", PROB_A_NOT_TRUE);
      verifyReduction(client, "flag_b IS NOT FALSE", PROB_A_NOT_FALSE);
      verifyReduction(client, "id_i BETWEEN 10 AND 20", PROB_A_BETWEEN_B_C);
      verifyReduction(client, "id_i NOT BETWEEN 10 AND 20", 1 - PROB_A_BETWEEN_B_C);
      verifyReduction(client, "NOT ( id_i BETWEEN 10 AND 20 )", 1 - PROB_A_BETWEEN_B_C);
      verifyReduction(client, "NOT ( id_i = 10 )", 1.0 - PROB_A_EQ_B);
      testSample1Enhanced(client);
    }
  }

  /**
   * Tests a sanitized version of the production query that triggered
   * the revision to the rules.
   *
   * @param client
   * @throws Exception
   */
  private void testSample1Enhanced(ClientFixture client) throws Exception {
    // col1_s20 in ('Value1','Value2','Value3','Value4',\n" +
    //              'Value5','Value6','Value7','Value8','Value9') -- min( .15 * 9, 0.5 ) = 50%
    //  AND col2_i <=3 -- 45%
    //  AND col3_s1 = 'Y'\n -- 15%
    //  AND col4_s1 = 'Y'\n -- 15%
    //  AND col5_s6 not like '%str1%' -- 85%
    //  AND col5_s6 not like '%str2%' -- 85%
    //  AND col5_s6 not like '%str3%' -- 85%
    //  AND col5_s6 not like '%str4%' -- 85%
    double expected = DrillRelDefaultMdSelectivity.MAX_OR_FACTOR *
        PROB_A_LE_B *
        Math.pow(PROB_A_EQ_B, 2) *
        Math.pow(PROB_A_NOT_LIKE_B, 4);
    testSample1(client, expected);
  }

  private void testSample1(ClientFixture client, double expected) throws Exception {
    String where = "col1_s20 in ('Value1','Value2','Value3','Value4',\n" +
        "                        'Value5','Value6','Value7','Value8','Value9')\n" + // min( .15 * 9, 0.5 ) = 50%
        "AND col2_i <=3\n" + // 45%
        "AND col3_s1 = 'Y'\n" + // 15%
        "AND col4_s1 = 'Y'\n" + // 15%
        "AND col5_s6 not like '%str1%'\n" + // 85%
        "AND col5_s6 not like '%str2%'\n" + // 85%
        "AND col5_s6 not like '%str3%'\n" + // 85%
        "AND col5_s6 not like '%str4%'\n"; // 85%

    // Need many rows because selectivity is very low for default rules. Sorry!

    String sql = "SELECT col1_s20, col2_i, col3_s1, col4_s1, col6_s6 FROM `mock`.`example_100K`\n" +
                 "WHERE " + where;
//    System.out.println("Sample 1 reduction: " + expected);
    verifyReductionQuery(client, sql, "sample 1", expected);
  }

  /**
   * Enhanced selectivity rules for NOT TRUE and NOT FALSE model the fact
   * that NOT TRUE means (IS FALSE OR IS NULL) and so on. Verify this
   * behavior.
   *
   * @param client
   * @throws Exception
   */
  private void verifyBooleanBehavior(ClientFixture client) throws Exception {
    String base = "SELECT * FROM `cp`.`planner/tf.json`";
    QuerySummary summary = client.queryBuilder().sql(base).run();
    assertEquals(4, summary.recordCount());
    summary = client.queryBuilder().sql(base + " WHERE col IS TRUE").run();
    assertEquals(1, summary.recordCount());
    summary = client.queryBuilder().sql(base + " WHERE col IS NULL").run();
    assertEquals(2, summary.recordCount());
    summary = client.queryBuilder().sql(base + " WHERE col IS FALSE").run();
    assertEquals(1, summary.recordCount());
    summary = client.queryBuilder().sql(base + " WHERE NOT (col IS TRUE)").run();
    assertEquals(3, summary.recordCount());
    summary = client.queryBuilder().sql(base + " WHERE NOT (col IS FALSE)").run();
    assertEquals(3, summary.recordCount());
  }

  private void verifyReduction(ClientFixture client, String where, double expected) throws Exception {
    String sql = "SELECT id_i, name_s20, flag_b FROM `mock`.`customer_10K`";
    if (where != null) {
      sql += " WHERE " + where;
    }
    verifyReductionQuery(client, sql, where, expected);
  }

  private void verifyReductionQuery(ClientFixture client, String sql, String label, double expected) throws Exception {
//    System.out.println(client.queryBuilder().sql(sql).explainText());

    // Sad: must run the query to get the estimates. The above EXPLAIN PLAN
    // does not fully explain the plan...

    ClientFixture cf = client;
    QuerySummary summary = cf.queryBuilder().sql(sql).run();

    // Temporary until the test framework enhancements are submitted.

    Thread.sleep(500); // Wait for profile to be written. Temporary until DRILL-5257 is available
    File profileFile = client.getProfileFile(summary.queryIdString());
    String text = Files.toString(profileFile, Charsets.UTF_8);
    Pattern p = Pattern.compile("MockScanEntry \\[records=(\\d+),");
    Matcher m = p.matcher(text);
    assertTrue(m.find());
    long scanRows = Long.parseLong(m.group(1));
    p = Pattern.compile("Filter\\(condition=\\[[^\\]]+\\]\\) : rowType = RecordType\\([^)]+\\): rowcount = ([^,]+),");
    m = p.matcher(text);
    assertTrue(m.find());
    double filterRows = Double.parseDouble(m.group(1));
    double reduction = filterRows / scanRows;
    if (filterRows == 1 && expected < reduction) {
      System.out.println( label + ": Truncating expected result to " + reduction);
      expected = reduction;
    }

    // Delete the above and use the following once the feature is
    // available in the test framework.

//    ProfileParser profile = client.parseProfile(summary.queryIdString());
//    profile.printPlan();

//    OpDefInfo scan = profile.getOpDefn("Scan").get(0);
//    OpDefInfo filter = profile.getOpDefn("Filter").get(0);
//    double reduction = filter.estRows / scan.estRows;

    // Enable the following to see the details for each test
//    System.out.println(label + " - " + reduction + ", ex: " + expected);

    // Verify with a 2.5% margin of error.
    assertEquals(label, expected, reduction, expected / 40);
  }

  /**
   * Verify that we understand the Calcite defaults. Values determined by
   * inspection and testing.
   *
   * @throws Exception
   */
  @Test
  public void testCalciteRules() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_ENABLE, false)
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//      .configProperty(ExecConstants.QUERY_PROFILE_OPTION, "sync") // Temporary until DRILL-5257 is available
        .systemOption(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getOptionName(), 0.000)
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      verifyReduction(client, "id_i = 10", 0.15);
      verifyReduction(client, "id_i <> 10", 0.5);
      verifyReduction(client, "id_i < 10", 0.5);
      verifyReduction(client, "id_i > 10", 0.5);
      verifyReduction(client, "id_i <= 10", 0.5);
      verifyReduction(client, "id_i >= 10", 0.5);
      verifyReduction(client, "id_i IN (10)", 0.15);
      verifyReduction(client, "id_i IN (10, 20)", 0.25);
      verifyReduction(client, "id_i IN (11, 12, 13, 14, 15, 16, 16, 18, 19, 20)", 0.25);
      verifyReduction(client, "name_s20 LIKE 'foo'", 0.25);
      verifyReduction(client, "name_s20 NOT LIKE 'foo'", 0.25);
      verifyReduction(client, "name_s20 IS NULL", 0.25);
      verifyReduction(client, "name_s20 IS NOT NULL", 0.9);
      verifyReduction(client, "NOT ( id_i = 10 )", 0.25);
      verifyReduction(client, "flag_b IS TRUE", 0.25);
      verifyReduction(client, "flag_b IS FALSE", 0.25);
      verifyReduction(client, "flag_b IS NOT TRUE", 0.25);
      verifyReduction(client, "flag_b IS NOT FALSE", 0.25);
      verifyReduction(client, "id_i BETWEEN 10 AND 20", 0.25);
      verifyReduction(client, "id_i NOT BETWEEN 10 AND 20", 0.25);
      verifyReduction(client, "NOT ( id_i BETWEEN 10 AND 20 )", 0.25);
      verifyReduction(client, "NOT ( id_i = 10 )", 0.25);
      testSample1Calcite(client);

      // Demonstrate that selectivity limits work to refine overall estimates.
      // Apply limit at session level for this client.

      client.alterSession(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getOptionName(), 0.005);
      testSample1(client,0.005);
      try (ClientFixture client2 = cluster.clientBuilder().build()) {

        // Apply at system level using this client.

        client.alterSystem(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getOptionName(), 0.004);

        // Should impact another client.

        testSample1(client2,0.004);
      } finally {

        // Put things back so we don't break anything.

        client.alterSystem(PlannerSettings.FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR.getOptionName(), 0.000);
      }
    }
  }

  private void testSample1Calcite(ClientFixture client) throws Exception {
    // col1_s20 in ('Value1','Value2','Value3','Value4',\n" +
    //              'Value5','Value6','Value7','Value8','Value9') -- 25%
    //  AND col2_i <=3 -- 50%
    //  AND col3_s1 = 'Y'\n -- 15%
    //  AND col4_s1 = 'Y'\n -- 15%
    //  AND col5_s6 not like '%str1%' -- 25%
    //  AND col5_s6 not like '%str2%' -- 25%
    //  AND col5_s6 not like '%str3%' -- 25%
    //  AND col5_s6 not like '%str4%' -- 25%
    double expected = 0.25 * 0.50 *
        Math.pow(0.15, 2) *
        Math.pow(0.25, 4);
//    System.out.println( "Calcite sample 1: " + expected );
    testSample1(client, expected);
  }

  /**
   * Verify that custom base probabilities can be set in the
   * config file and used by the planner.
   * @throws Exception
   */

  @Test
  public void testCustomRules() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder()
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_ENABLE, true)
        .configProperty(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
//      .configProperty(ExecConstants.QUERY_PROFILE_OPTION, "sync") // Temporary until DRILL-5257 is available
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_PROB_EQ, 0.10)
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_PROB_LIKE, 0.20)
        .configProperty(ExecConstants.OPTIMIZER_ENHANCED_DEFAULTS_PROB_NULL, 0.05)
        .maxParallelization(1);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      verifyBooleanBehavior(client);
      verifyReduction(client, "id_i = 10", 0.10);
      verifyReduction(client, "id_i <> 10", 0.90);
      verifyReduction(client, "id_i < 10", 0.90 / 2);
      verifyReduction(client, "id_i > 10", 0.90 / 2);
      verifyReduction(client, "id_i IN (10)", 0.10);
      verifyReduction(client, "name_s20 LIKE 'foo'", 0.20);
      verifyReduction(client, "name_s20 NOT LIKE 'foo'", 0.80);
      verifyReduction(client, "name_s20 IS NULL", 0.05);
      verifyReduction(client, "name_s20 IS NOT NULL", 0.95);
     }
  }
}
