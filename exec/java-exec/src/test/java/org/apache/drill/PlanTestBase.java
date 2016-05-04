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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.calcite.sql.SqlExplain.Depth;
import org.apache.calcite.sql.SqlExplainLevel;

import com.google.common.base.Strings;

public class PlanTestBase extends BaseTestQuery {

  protected static final String OPTIQ_FORMAT = "text";
  protected static final String JSON_FORMAT = "json";
  protected static final String EXPECTED_NOT_FOUND = "Did not find expected pattern in plan: ";
  protected static final String UNEXPECTED_FOUND = "Found unwanted pattern in plan: ";

  // TODO - enhance this to support regex, and excluded patterns like the
  // check method below for optiq format plans
  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in json
   * format. Then check the physical plan against the list expected substrs.
   * Verify all the expected strings are contained in the physical plan string.
   */
  public static void testPhysicalPlan(String sql, String... expectedSubstrs)
      throws Exception {
    sql = "EXPLAIN PLAN for " + QueryTestUtil.normalizeQuery(sql);

    final String planStr = getPlanInString(sql, JSON_FORMAT);

    for (final String colNames : expectedSubstrs) {
      assertTrue(String.format("Unable to find expected string %s in plan: %s!", colNames, planStr),
          planStr.contains(colNames));
    }
  }

  /**
   * Runs an explain plan query and check for expected regex patterns (in optiq
   * text format), also ensure excluded patterns are not found. Either list can
   * be empty or null to skip that part of the check.
   *
   * See the convenience methods for passing a single string in either the
   * excluded list, included list or both.
   *
   * @param query - an explain query, this method does not add it for you
   * @param expectedPatterns - list of patterns that should appear in the plan
   * @param excludedPatterns - list of patterns that should not appear in the plan
   * @throws Exception - if an inclusion or exclusion check fails, or the
   *                     planning process throws an exception
   */
  public static void testPlanMatchingPatterns(String query, String[] expectedPatterns, String[] excludedPatterns)
      throws Exception {
    final String plan = getPlanInString("EXPLAIN PLAN for " + QueryTestUtil.normalizeQuery(query), OPTIQ_FORMAT);

    // Check and make sure all expected patterns are in the plan
    if (expectedPatterns != null) {
      for (final String s : expectedPatterns) {
        final Pattern p = Pattern.compile(s);
        final Matcher m = p.matcher(plan);
        assertTrue(EXPECTED_NOT_FOUND + s, m.find());
      }
    }

    // Check and make sure all excluded patterns are not in the plan
    if (excludedPatterns != null) {
      for (final String s : excludedPatterns) {
        final Pattern p = Pattern.compile(s);
        final Matcher m = p.matcher(plan);
        assertFalse(UNEXPECTED_FOUND + s, m.find());
      }
    }
  }

  /**
   * Runs an explain plan query and check for expected substring patterns (in optiq
   * text format), also ensure excluded patterns are not found. Either list can
   * be empty or null to skip that part of the check.
   *
   * This is different from testPlanMatchingPatterns in that this one use substring contains,
   * in stead of regex pattern matching. This one is useful when the pattern contains
   * many regex reserved chars, and you do not want to put the escape char.
   *
   * See the convenience methods for passing a single string in either the
   * excluded list, included list or both.
   *
   * @param query - an explain query, this method does not add it for you
   * @param expectedPatterns - list of patterns that should appear in the plan
   * @param excludedPatterns - list of patterns that should not appear in the plan
   * @throws Exception - if an inclusion or exclusion check fails, or the
   *                     planning process throws an exception
   */
  public static void testPlanSubstrPatterns(String query, String[] expectedPatterns, String[] excludedPatterns)
      throws Exception {
    final String plan = getPlanInString("EXPLAIN PLAN for " + QueryTestUtil.normalizeQuery(query), OPTIQ_FORMAT);

    // Check and make sure all expected patterns are in the plan
    if (expectedPatterns != null) {
      for (final String s : expectedPatterns) {
        assertTrue(EXPECTED_NOT_FOUND + s + ", Output plan: " + plan, plan.contains(s));
      }
    }

    // Check and make sure all excluded patterns are not in the plan
    if (excludedPatterns != null) {
      for (final String s : excludedPatterns) {
        assertFalse(UNEXPECTED_FOUND + s + ", Output plan: " + plan, plan.contains(s));
      }
    }
  }

  public static void testPlanOneExpectedPatternOneExcluded(
      String query, String expectedPattern, String excludedPattern) throws Exception {
    testPlanMatchingPatterns(query, new String[]{expectedPattern}, new String[]{excludedPattern});
  }

  public static void testPlanOneExpectedPattern(String query, String expectedPattern) throws Exception {
    testPlanMatchingPatterns(query, new String[]{expectedPattern}, new String[]{});
  }

  public static void testPlanOneExcludedPattern(String query, String excludedPattern) throws Exception {
    testPlanMatchingPatterns(query, new String[]{}, new String[]{excludedPattern});
  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public static void testRelLogicalJoinOrder(String sql, String... expectedSubstrs) throws Exception {
    final String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.LOGICAL);
    final String prefixJoinOrder = getLogicalPrefixJoinOrderFromPlan(planStr);
    System.out.println(" prefix Join order = \n" + prefixJoinOrder);
    for (final String substr : expectedSubstrs) {
      assertTrue(String.format("Expected string %s is not in the prefixJoinOrder %s!", substr, prefixJoinOrder),
          prefixJoinOrder.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public static void testRelPhysicalJoinOrder(String sql, String... expectedSubstrs) throws Exception {
    final String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.PHYSICAL);
    final String prefixJoinOrder = getPhysicalPrefixJoinOrderFromPlan(planStr);
    System.out.println(" prefix Join order = \n" + prefixJoinOrder);
    for (final String substr : expectedSubstrs) {
      assertTrue(String.format("Expected string %s is not in the prefixJoinOrder %s!", substr, prefixJoinOrder),
          prefixJoinOrder.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in
   * Optiq RelNode format. Then check the physical plan against the list
   * expected substrs. Verify all the expected strings are contained in the
   * physical plan string.
   */
  public static void testRelPhysicalPlanLevDigest(String sql, String... expectedSubstrs)
      throws Exception {
    final String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.DIGEST_ATTRIBUTES, Depth.PHYSICAL);
    for (final String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public static void testRelLogicalPlanLevDigest(String sql, String... expectedSubstrs)
      throws Exception {
    final String planStr = getDrillRelPlanInString(sql,
        SqlExplainLevel.DIGEST_ATTRIBUTES, Depth.LOGICAL);

    for (final String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in
   * Optiq RelNode format. Then check the physical plan against the list
   * expected substrs. Verify all the expected strings are contained in the
   * physical plan string.
   */
  public static void testRelPhysicalPlanLevExplain(String sql, String... expectedSubstrs) throws Exception {
    final String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.PHYSICAL);

    for (final String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public static void testRelLogicalPlanLevExplain(String sql, String... expectedSubstrs) throws Exception {
    final String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.LOGICAL);

    for (final String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /*
   * This will get the plan (either logical or physical) in Optiq RelNode
   * format, based on SqlExplainLevel and Depth.
   */
  private static String getDrillRelPlanInString(String sql, SqlExplainLevel level,
      Depth depth) throws Exception {
    String levelStr = " ", depthStr = " ";

    switch (level) {
    case NO_ATTRIBUTES:
      levelStr = "EXCLUDING ATTRIBUTES";
      break;
    case EXPPLAN_ATTRIBUTES:
      levelStr = "INCLUDING ATTRIBUTES";
      break;
    case ALL_ATTRIBUTES:
      levelStr = "INCLUDING ALL ATTRIBUTES";
      break;
    default:
      break;
    }

    switch (depth) {
    case TYPE:
      depthStr = "WITH TYPE";
      break;
    case LOGICAL:
      depthStr = "WITHOUT IMPLEMENTATION";
      break;
    case PHYSICAL:
      depthStr = "WITH IMPLEMENTATION";
      break;
    default:
      throw new UnsupportedOperationException();
    }

    sql = "EXPLAIN PLAN " + levelStr + " " + depthStr + "  for "
        + QueryTestUtil.normalizeQuery(sql);

    return getPlanInString(sql, OPTIQ_FORMAT);
  }

  /*
   * This will submit an "EXPLAIN" statement, and return the column value which
   * contains the plan's string.
   */
  protected static String getPlanInString(String sql, String columnName)
      throws Exception {
    final List<QueryDataBatch> results = testSqlWithResults(sql);
    final RecordBatchLoader loader = new RecordBatchLoader(getDrillbitContext().getAllocator());
    final StringBuilder builder = new StringBuilder();
    final boolean silent = config != null && config.getBoolean(QueryTestUtil.TEST_QUERY_PRINTING_SILENT);

    for (final QueryDataBatch b : results) {
      if (!b.hasData()) {
        continue;
      }

      loader.load(b.getHeader().getDef(), b.getData());

      final VectorWrapper<?> vw;
      try {
          vw = loader.getValueAccessorById(
              NullableVarCharVector.class,
              loader.getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldIds());
      } catch (Throwable t) {
        throw new Exception("Looks like you did not provide an explain plan query, please add EXPLAIN PLAN FOR to the beginning of your query.");
      }

      if (!silent) {
        System.out.println(vw.getValueVector().getField().getPath());
      }
      final ValueVector vv = vw.getValueVector();
      for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
        final Object o = vv.getAccessor().getObject(i);
        builder.append(o);
        if (!silent) {
          System.out.println(o);
        }
      }
      loader.clear();
      b.release();
    }

    return builder.toString();
  }

  private static String getLogicalPrefixJoinOrderFromPlan(String plan) {
    return getPrefixJoinOrderFromPlan(plan, "DrillJoinRel", "DrillScanRel");
  }

  private static String getPhysicalPrefixJoinOrderFromPlan(String plan) {
    return getPrefixJoinOrderFromPlan(plan, "JoinPrel", "ScanPrel");
  }

  private static String getPrefixJoinOrderFromPlan(String plan, String joinKeyWord, String scanKeyWord) {
    final StringBuilder builder = new StringBuilder();
    final String[] planLines = plan.split("\n");
    int cnt = 0;
    final Stack<Integer> s = new Stack<>();

    for (final String line : planLines) {
      if (line.trim().isEmpty()) {
        continue;
      }
      if (line.contains(joinKeyWord)) {
        builder.append(Strings.repeat(" ", 2 * s.size()));
        builder.append(joinKeyWord + "\n");
        cnt++;
        s.push(cnt);
        cnt = 0;
        continue;
      }

      if (line.contains(scanKeyWord)) {
        cnt++;
        builder.append(Strings.repeat(" ", 2 * s.size()));
        builder.append(line.trim() + "\n");

        if (cnt == 2) {
          cnt = s.pop();
        }
      }
    }

    return builder.toString();
  }
}
