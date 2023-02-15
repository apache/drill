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
package org.apache.drill.exec.planner.sql;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.drill.exec.physical.impl.join.JoinUtils.FAILED_TO_PLAN_CARTESIAN_JOIN;
import static org.hamcrest.CoreMatchers.containsString;

@Category(SqlTest.class)
public class CrossJoinTest extends ClusterTest {

  private static int NATION_TABLE_RECORDS_COUNT = 25;

  private static int EXPECTED_COUNT = NATION_TABLE_RECORDS_COUNT * NATION_TABLE_RECORDS_COUNT;

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @After
  public void tearDown() {
    client.resetSession(PlannerSettings.NLJOIN_FOR_SCALAR.getOptionName());
  }

  @Test
  public void testCrossJoinFailsForEnabledOption() throws Exception {
    enableNlJoinForScalarOnly();
    String sql = "SELECT l.n_name, r.n_name " +
      "FROM cp.`tpch/nation.parquet` l " +
      "CROSS JOIN cp.`tpch/nation.parquet` r";

    UserRemoteException UserRemoteException = Assert.assertThrows(UserRemoteException.class, () ->  queryBuilder().sql(sql).run());
    MatcherAssert.assertThat(UserRemoteException.getMessage(), containsString(FAILED_TO_PLAN_CARTESIAN_JOIN));
  }

  @Test
  public void testCrossJoinSucceedsForDisabledOption() throws Exception {
    disableNlJoinForScalarOnly();
    client.testBuilder().sqlQuery(
        "SELECT l.n_name,r.n_name " +
            "FROM cp.`tpch/nation.parquet` l " +
            "CROSS JOIN cp.`tpch/nation.parquet` r")
        .expectsNumRecords(EXPECTED_COUNT)
        .go();
  }

  @Test
  public void testCommaJoinFailsForEnabledOption() throws Exception {
    enableNlJoinForScalarOnly();

    String sql = "SELECT l.n_name,r.n_name " +
      "FROM cp.`tpch/nation.parquet` l, cp.`tpch/nation.parquet` r";

    UserException UserRemoteException = Assert.assertThrows(UserRemoteException.class, () -> queryBuilder().sql(sql).run());
    MatcherAssert.assertThat(UserRemoteException.getMessage(), containsString(FAILED_TO_PLAN_CARTESIAN_JOIN));
  }

  @Test
  public void testCommaJoinSucceedsForDisabledOption() throws Exception {
    disableNlJoinForScalarOnly();
    client.testBuilder().sqlQuery(
        "SELECT l.n_name,r.n_name " +
            "FROM cp.`tpch/nation.parquet` l, cp.`tpch/nation.parquet` r")
        .expectsNumRecords(EXPECTED_COUNT)
        .go();
  }

  @Test
  public void testSubSelectCrossJoinFailsForEnabledOption() throws Exception {
    enableNlJoinForScalarOnly();

    String sql = "SELECT COUNT(*) c " +
      "FROM (" +
      "SELECT l.n_name,r.n_name " +
      "FROM cp.`tpch/nation.parquet` l " +
      "CROSS JOIN cp.`tpch/nation.parquet` r" +
      ")";

    UserException UserRemoteException = Assert.assertThrows(UserRemoteException.class, () -> queryBuilder().sql(sql).run());
    MatcherAssert.assertThat(UserRemoteException.getMessage(), containsString(FAILED_TO_PLAN_CARTESIAN_JOIN));
  }

  @Test
  public void testSubSelectCrossJoinSucceedsForDisabledOption() throws Exception {
    disableNlJoinForScalarOnly();

    client.testBuilder()
        .sqlQuery(
            "SELECT COUNT(*) c " +
                "FROM (SELECT l.n_name,r.n_name " +
                "FROM cp.`tpch/nation.parquet` l " +
                "CROSS JOIN cp.`tpch/nation.parquet` r)")
        .unOrdered()
        .baselineColumns("c")
        .baselineValues((long) EXPECTED_COUNT)
        .go();
  }

  @Test
  public void textCrossAndCommaJoinFailsForEnabledOption() throws Exception {
    enableNlJoinForScalarOnly();

    String sql = "SELECT * " +
      "FROM cp.`tpch/nation.parquet` a, cp.`tpch/nation.parquet` b " +
      "CROSS JOIN cp.`tpch/nation.parquet` c";

    UserException UserRemoteException = Assert.assertThrows(UserRemoteException.class, () -> queryBuilder().sql(sql).run());
    MatcherAssert.assertThat(UserRemoteException.getMessage(), containsString(FAILED_TO_PLAN_CARTESIAN_JOIN));
  }

  @Test
  public void textCrossAndCommaJoinSucceedsForDisabledOption() throws Exception {
    disableNlJoinForScalarOnly();

    client.testBuilder().sqlQuery(
        "SELECT * " +
            "FROM cp.`tpch/nation.parquet` a, cp.`tpch/nation.parquet` b " +
            "CROSS JOIN cp.`tpch/nation.parquet` c")
        .expectsNumRecords(NATION_TABLE_RECORDS_COUNT * EXPECTED_COUNT)
        .go();
  }

  @Test
  public void testCrossApplyFailsForEnabledOption() throws Exception {
    enableNlJoinForScalarOnly();

    String sql =  "SELECT * " +
      "FROM cp.`tpch/nation.parquet` l " +
      "CROSS APPLY cp.`tpch/nation.parquet` r";

    UserException UserRemoteException = Assert.assertThrows(UserRemoteException.class, () -> queryBuilder().sql(sql).run());
    MatcherAssert.assertThat(UserRemoteException.getMessage(), containsString(FAILED_TO_PLAN_CARTESIAN_JOIN));
  }

  @Test
  public void testCrossApplySucceedsForDisabledOption() throws Exception {
    disableNlJoinForScalarOnly();

    client.testBuilder().sqlQuery(
        "SELECT * " +
            "FROM cp.`tpch/nation.parquet` l " +
            "CROSS APPLY cp.`tpch/nation.parquet` r")
        .expectsNumRecords(EXPECTED_COUNT)
        .go();
  }

  @Test
  public void testCrossJoinSucceedsForEnabledOptionAndScalarInput() throws Exception {
    enableNlJoinForScalarOnly();

    client.testBuilder().sqlQuery(
        "SELECT * " +
            "FROM cp.`tpch/nation.parquet` l " +
            "CROSS JOIN (SELECT * FROM cp.`tpch/nation.parquet` r LIMIT 1)")
        .expectsNumRecords(NATION_TABLE_RECORDS_COUNT)
        .go();
  }

  private static void disableNlJoinForScalarOnly() {
    client.alterSession(PlannerSettings.NLJOIN_FOR_SCALAR.getOptionName(), false);
  }

  private static void enableNlJoinForScalarOnly() {
    client.alterSession(PlannerSettings.NLJOIN_FOR_SCALAR.getOptionName(), true);
  }
}
