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
package org.apache.drill.exec.expr;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserBitShared;
import org.junit.Ignore;
import org.junit.Test;

public class TestSchemaPathMaterialization extends BaseTestQuery {

  @Test
  public void testSingleProjectionFromMultiLevelRepeatedList() throws Exception {
    final String query = "select t.odd[2][0][0] v1 " +
        " from cp.`complex/json/repeated_list.json` t";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("v1")
        .baselineValues(5L)
        .go();
  }

  @Test
  public void testMultiProjectionFromMultiLevelRepeatedListWhenFieldsExist() throws Exception {
    final String query = "select t.odd[0][0][0] v1, t.odd[0][1][0] v2, t.odd[0][2][0] v3 " +
        " from cp.`complex/json/repeated_list.json` t";

    testRunAndPrint(UserBitShared.QueryType.SQL, query);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("v1", "v2", "v3")
        .baselineValues(1L, null, 3L)
        .go();
  }

  @Test
  @Ignore("Ignored until DRILL-2539 is fixed")
  public void testProjectionFromMultiLevelRepeatedList() throws Exception {
    final String query = "select t.odd[0][1][0] v1, t.odd[0][1][0] v2, t.odd[0][2][0] v3, " +
        " t.odd[1] v4, t.odd[2][0][0] v5, t.odd[2][1][0] v6" +
        " from cp.`complex/json/repeated_list.json` t";

    testRunAndPrint(UserBitShared.QueryType.SQL, query);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("v1", "v2", "v3", "v4", "v5", "v6")
        .baselineValues(1L, null, 3L, null, 5L, null)
        .go();
  }

  @Test
  @Ignore("Ignored until DRILL-2539 is fixed")
  public void testProjectionFromMultiLevelRepeatedListMap() throws Exception {
    final String query = "select t.odd[0][0].val[0] v1, t.odd[0][0].val[0] v2, " +
        " from cp.`complex/json/repeated_list_map.json` t";

    testRunAndPrint(UserBitShared.QueryType.SQL, query);
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("v1", "v2")
        .baselineValues(1L, 3L)
        .go();
  }

  @Test //DRILL-1962
  public void testProjectionMultipleFiles() throws Exception {
    final String query="select t.oooa.oa.oab.oabc[1].rowValue1 rowValue from dfs.`${WORKING_PATH}/src/test/resources/complex/json/multiple/*.json` t";

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .jsonBaselineFile("complex/drill-1962-result.json")
      .go();
  }

}
