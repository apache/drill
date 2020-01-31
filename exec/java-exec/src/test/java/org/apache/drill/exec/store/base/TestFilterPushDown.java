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
package org.apache.drill.exec.store.base;

import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the Filter push-down helper classes as part of the
 * "Base" storage plugin to be used for add-on plugins. Uses a
 * "test mule" ("Dummy") plug-in which goes through the paces of
 * planning push down, but then glosses over the details at run time.
 * The focus here are plans: the tests plan a query then compare the
 * actual plan against and expected ("golden") plan.
 */
public class TestFilterPushDown extends ClusterTest {

  private static final String BASE_SQL = "SELECT a, b FROM dummy.myTable";
  private static final String BASE_WHERE = BASE_SQL +  " WHERE ";
  private static final PlanVerifier verifier = new PlanVerifier("/store/base/");

  // Uncomment the next line to save failing plans to /tmp/drill/test
  // static { verifier.saveResults(true); }

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher);
    startCluster(builder);

    DummyStoragePluginConfig config =
        new DummyStoragePluginConfig(true, true, false);
    cluster.defineStoragePlugin("dummy", config);
  }

  //-------------------------------------------------
  // Unsupported filter push-down cases

  // No predicates

  @Test
  public void testNoPushDown() throws Exception {
    verifyPlan(BASE_SQL, "noPushDown.json");
  }

  // Predicate mismatch on type (id should be INT, dummy does
  // not try to convert)
  @Test
  public void testTypeMismatch() throws Exception {
    verifyPlan(BASE_WHERE + "id = 'foo'", "typeMismatch.json");
  }

  // Unsupported relop type (dummy supports limited set)
  @Test
  public void testUnsupportedOp() throws Exception {
    verifyPlan(BASE_WHERE + "a <> 'foo'", "unsupportedOp.json");
  }

  // Column reference rather than constant
  @Test
  public void testNonConst() throws Exception {
   verifyPlan(BASE_WHERE + "a = b", "nonConstPred.json");
  }

  // Unknown column (dummy only knows columns a and b)
  @Test
  public void testUnsupportedCol() throws Exception {
    verifyPlan(BASE_WHERE + "c = 'foo'", "unsupportedColPred.json");
  }

  // Not simple col = const
  @Test
  public void testComplexPred() throws Exception {
    verifyPlan(BASE_WHERE + "id + 10 = 20", "complexPred.json");
  }

  // Complex schema paths
  @Test
  public void testComplexCols() throws Exception {
    verifyPlan(BASE_WHERE + "a[10] = 'foo' AND myTable.b.c = 'bar'", "complexCols.json");
  }

  // OR, can't push
  @Test
  public void testGenericOr() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar' OR id = 10", "or.json");
  }

  // Listener rejects one of the expressions within an OR,
  // must reject the entire OR clause. (Dummy rejects >.)
  @Test
  public void testRejectOneOrExpr() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar' OR a > 'foo'", "rejectOneOrExpr.json");
  }

  // Or clause expressions accepted, but whole of OR is rejected
  // because it is not all = operators. (Dummy accepts <.)
  @Test
  public void testNonEqOr() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar' OR a < 'foo'", "nonEqOr.json");
  }

  @Test
  public void testDoubleOr() throws Exception {
    verifyPlan(BASE_WHERE + "(a = 'x' OR a = 'y') AND (a = 'a' OR a = 'b')", "doubleOr.json");
  }

  //-------------------------------------------------
  // Supported filter push-down cases

  // Single matching predicate
  @Test
  public void testSingleCol() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar'", "singleCol.json");
  }

  // Two matching predicates, one is implicit (not in project list)
  @Test
  public void testTwoCols() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar' AND id = 10", "twoCols.json");
  }

  // Pushed and unpushed conditions
  @Test
  public void testMixedPreds() throws Exception {
    verifyPlan(BASE_WHERE + "a = 'bar' AND id = 10 AND c > 20", "mixedPreds.json");
  }

  // Reversed predicate
  @Test
  public void testReversed() throws Exception {
    verifyPlan(BASE_WHERE + "'bar' > a", "reversed.json");
  }

  // Join, same project list from both scans. (Calcite does filter push down
  // only once into a scan node shared by both scans.)
  @Test
  @Ignore("DRILL-7457")
  public void testSimpleJoin() throws Exception {
    String sql =
        "SELECT t1.a, t1.b, t2.b FROM dummy.myTable t1, dummy.myTable t2 WHERE t1.a = t2.a AND t1.a = 'bar'";
    verifyPlan(sql, "join1.json");
  }

  // Similar case, but different project lists. So, two scan nodes and two
  // sets of filter push downs. (Difference is: t2.b --> t2.c)
  @Test
  @Ignore("DRILL-7457")
  public void testComplexJoin() throws Exception {
    String sql =
        "SELECT t1.a, t1.b, t2.c FROM dummy.myTable t1, dummy.myTable t2 WHERE t1.a = t2.a AND t1.a = 'bar'";
    verifyPlan(sql, "join2.json");
  }

  // IS NULL and IS NOT NULL. Has only one argument.
  @Test
  public void testIsNull() throws Exception {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a IS NULL AND b IS NOT NULL";
    verifyPlan(sql, "isNull.json");
  }

  // BETWEEN. Calcite rewrites a BETWEEN x AND y into
  // a >= x AND a <= y.
  // Since the dummy plug-in only handles <=, the => is left in the query.
  @Test
  public void testBetween() throws Exception {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a BETWEEN 'bar' AND 'foo'";
    verifyPlan(sql, "between.json");
  }

  // IN clause, handled as a = 'bar' OR a = 'foo'.
  // Presumption is that this turns into multiple scans
  @Test
  public void testIn() throws Exception {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a IN('bar', 'foo')";
    verifyPlan(sql, "in.json");
  }

  // Equivalent to the above
  @Test
  public void testInLikeOr() throws Exception {
    String sql = "SELECT a, b FROM dummy.myTable WHERE a = 'bar' OR a = 'foo'";
    verifyPlan(sql, "in.json");
  }

  /**
   * Test constant value conversion: Dummy will convert a to VARCHAR
   */
  @Test
  public void testTypeConversion() throws Exception {
    verifyPlan(BASE_WHERE + "a = 10", "typeConversion.json");
  }

  /**
   * Test all constant data types. The Dummy will convert all to VARCHAR.
   */
  @Test
  // Fails due to an unexplained difference.
  // Expected: "ref" : "`T[0]¦¦**`",
  // Actual:   "ref" : "`T[252]¦¦**`",
  // Only fails in Maven, not in the IDE. Are internal tables
  // numbered per session?
  @Ignore
  public void testAllTypes() throws Exception {
    String sql = "SELECT * FROM dummy.allTypes WHERE\n" +
        "    v = 'varchar'\n" +
        "AND b = true\n" +
        "AND i = 10\n" +
        "AND l = 5000000000\n" + // 5,000,000,000, too big for INT
        "AND de = 123.45\n" + // All float-likes are converted to DECIMAL
        // See http://drill.apache.org/docs/date-time-and-timestamp/
        "AND da = DATE '2008-2-23'\n" +
        "AND ti = TIME '12:23:34'\n" +
        "AND ts = TIMESTAMP '2008-2-23 12:23:34.456'\n" +
        "AND iy = INTERVAL '1' YEAR\n" +
        "AND ids = INTERVAL '1 10:20:30' DAY TO SECOND";
     verifyPlan(sql, "allTypes.json");
  }

  protected void verifyPlan(String sql, String expectedFile) throws Exception {
    verifier.verifyPlan(client, sql, expectedFile);
  }
}

