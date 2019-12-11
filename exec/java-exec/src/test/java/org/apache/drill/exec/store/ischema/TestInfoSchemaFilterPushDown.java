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
package org.apache.drill.exec.store.ischema;

import org.apache.drill.PlanTestBase;
import org.junit.Test;

public class TestInfoSchemaFilterPushDown extends PlanTestBase {

  @Test
  public void testFilterPushdown_Equal() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA='INFORMATION_SCHEMA'";
    final String scan = "Scan.*groupscan=\\[TABLES, filter=equal\\(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_NonEqual() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'";
    final String scan = "Scan.*groupscan=\\[TABLES, filter=not_equal\\(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_Like() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA LIKE '%SCH%'";
    final String scan = "Scan.*groupscan=\\[TABLES, filter=like\\(Field=TABLE_SCHEMA,Literal=%SCH%\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_LikeWithEscape() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA LIKE '%\\\\SCH%' ESCAPE '\\'";
    final String scan = "Scan.*groupscan=\\[TABLES, filter=like\\(Field=TABLE_SCHEMA,Literal=%\\\\\\\\SCH%,Literal=\\\\\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_And() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME <> 'version'";
    final String scan = "Scan.*groupscan=\\[COLUMNS, filter=booleanand\\(equal\\(Field=TABLE_SCHEMA,Literal=sys\\)," +
        "not_equal\\(Field=TABLE_NAME,Literal=version\\)\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_Or() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' OR " +
        "TABLE_NAME <> 'version' OR " +
        "TABLE_SCHEMA like '%sdfgjk%'";
    final String scan = "Scan.*groupscan=\\[COLUMNS, filter=booleanor\\(equal\\(Field=TABLE_SCHEMA,Literal=sys\\)," +
        "not_equal\\(Field=TABLE_NAME,Literal=version\\),like\\(Field=TABLE_SCHEMA,Literal=%sdfgjk%\\)\\)";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Equal() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA'";
    final String scan = "Scan.*groupscan=\\[COLUMNS, filter=equal\\(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA\\)";
    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_NotEqual() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME <> 'TABLES'";
    final String scan = "Scan.*groupscan=\\[COLUMNS, filter=not_equal\\(Field=TABLE_NAME,Literal=TABLES\\)";
    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Like() throws Exception {
    final String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME LIKE '%BL%'";
    final String scan = "Scan.*groupscan=\\[COLUMNS, filter=like\\(Field=TABLE_NAME,Literal=%BL%\\)";
    testHelper(query, scan, false);
  }

  @Test
  public void testPartialFilterPushDownWithProject() throws Exception {
    final String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME = 'version' AND " +
        "COLUMN_NAME like 'commit%s' AND " +
        "IS_NULLABLE = 'YES'"; // this is not expected to pushdown into scan
    final String scan = "Scan.*groupscan=\\[COLUMNS, " +
        "filter=booleanand\\(equal\\(Field=TABLE_SCHEMA,Literal=sys\\),equal\\(Field=TABLE_NAME,Literal=version\\)," +
        "like\\(Field=COLUMN_NAME,Literal=commit%s\\)\\)";

    testHelper(query, scan, true);
  }

  private void testHelper(final String query, String filterInScan, boolean filterPrelExpected) throws Exception {
    String[] expectedPatterns;
    String[] excludedPatterns;
    if (filterPrelExpected) {
      expectedPatterns = new String[] {filterInScan, "Filter"};
      excludedPatterns = new String[] {};
    } else {
      expectedPatterns = new String[] {filterInScan};
      excludedPatterns = new String[] {"Filter"};
    }

    // check plan
    testPlanMatchingPatterns(query, expectedPatterns, excludedPatterns);

    // run the query
    test(query);
  }
}
