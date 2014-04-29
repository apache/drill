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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.eigenbase.sql.SqlExplain.Depth;
import org.eigenbase.sql.SqlExplainLevel;

public class PlanTestBase extends BaseTestQuery {

  protected static final String OPTIQ_FORMAT = "text";
  protected static final String JSON_FORMAT = "json";

  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in json
   * format. Then check the physical plan against the list expected substrs.
   * Verify all the expected strings are contained in the physical plan string.
   */
  public void testPhysicalPlan(String sql, String... expectedSubstrs)
      throws Exception {
    sql = "EXPLAIN PLAN for " + sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());

    String planStr = getPlanInString(sql, JSON_FORMAT);

    for (String colNames : expectedSubstrs) {
      assertTrue(planStr.contains(colNames));
    }
  }

  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in
   * Optiq RelNode format. Then check the physical plan against the list
   * expected substrs. Verify all the expected strings are contained in the
   * physical plan string.
   */
  public void testRelPhysicalPlanLevDigest(String sql, String... expectedSubstrs)
      throws Exception {
    String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.DIGEST_ATTRIBUTES, Depth.PHYSICAL);

    for (String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }

  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public void testRelLogicalPlanLevDigest(String sql, String... expectedSubstrs)
      throws Exception {
    String planStr = getDrillRelPlanInString(sql,
        SqlExplainLevel.DIGEST_ATTRIBUTES, Depth.LOGICAL);

    for (String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the PHYSICAL plan in
   * Optiq RelNode format. Then check the physical plan against the list
   * expected substrs. Verify all the expected strings are contained in the
   * physical plan string.
   */
  public void testRelPhysicalPlanLevExplain(String sql, String... expectedSubstrs) throws Exception {
    String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.PHYSICAL);

    for (String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }

  /**
   * This method will take a SQL string statement, get the LOGICAL plan in Optiq
   * RelNode format. Then check the physical plan against the list expected
   * substrs. Verify all the expected strings are contained in the physical plan
   * string.
   */
  public void testRelLogicalPlanLevExplain(String sql, String... expectedSubstrs) throws Exception {
    String planStr = getDrillRelPlanInString(sql, SqlExplainLevel.EXPPLAN_ATTRIBUTES, Depth.LOGICAL);

    for (String substr : expectedSubstrs) {
      assertTrue(planStr.contains(substr));
    }
  }


  /*
   * This will get the plan (either logical or physical) in Optiq RelNode
   * format, based on SqlExplainLevel and Depth.
   */
  private String getDrillRelPlanInString(String sql, SqlExplainLevel level,
      Depth depth) throws Exception {
    String levelStr, depthStr;
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
      throw new UnsupportedOperationException();
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
        + sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());

    return getPlanInString(sql, OPTIQ_FORMAT);
  }

  /*
   * This will submit an "EXPLAIN" statement, and return the column value which
   * contains the plan's string.
   */
  private String getPlanInString(String sql, String columnName)
      throws Exception {
    List<QueryResultBatch> results = testSqlWithResults(sql);

    RecordBatchLoader loader = new RecordBatchLoader(bit.getContext()
        .getAllocator());
    StringBuilder builder = new StringBuilder();

    for (QueryResultBatch b : results) {
      if (!b.hasData())
        continue;

      loader.load(b.getHeader().getDef(), b.getData());

      VectorWrapper<?> vw = loader.getValueAccessorById(loader
          .getValueVectorId(SchemaPath.getSimplePath(columnName)).getFieldId(),
          NullableVarCharVector.class);

      System.out.println(vw.getValueVector().getField().toExpr());
      ValueVector vv = vw.getValueVector();
      for (int i = 0; i < vv.getAccessor().getValueCount(); i++) {
        Object o = vv.getAccessor().getObject(i);
        builder.append(o);
        System.out.println(vv.getAccessor().getObject(i));
      }
    }

    return builder.toString();
  }

}
