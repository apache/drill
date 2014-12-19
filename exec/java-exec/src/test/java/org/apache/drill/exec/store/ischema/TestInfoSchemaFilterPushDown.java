/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
    String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA='INFORMATION_SCHEMA'";
    String scan = "Scan(groupscan=[TABLES, filter=equal(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA)])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_NonEqual() throws Exception {
    String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'";
    String scan = "Scan(groupscan=[TABLES, filter=not_equal(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA)])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_Like() throws Exception {
    String query = "SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_SCHEMA LIKE '%SCH%'";
    String scan = "Scan(groupscan=[TABLES, filter=like(Field=TABLE_SCHEMA,Literal=%SCH%)])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_And() throws Exception {
    String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME <> 'version'";
    String scan = "Scan(groupscan=[COLUMNS, filter=booleanand(equal(Field=TABLE_SCHEMA,Literal=sys)," +
        "not_equal(Field=TABLE_NAME,Literal=version))])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushdown_Or() throws Exception {
    String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' OR " +
        "TABLE_NAME <> 'version' OR " +
        "TABLE_SCHEMA like '%sdfgjk%'";
    String scan = "Scan(groupscan=[COLUMNS, filter=booleanor(equal(Field=TABLE_SCHEMA,Literal=sys)," +
        "not_equal(Field=TABLE_NAME,Literal=version),like(Field=TABLE_SCHEMA,Literal=%sdfgjk%))])";

    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Equal() throws Exception {
    String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA'";
    String scan = "Scan(groupscan=[COLUMNS, filter=equal(Field=TABLE_SCHEMA,Literal=INFORMATION_SCHEMA)])";
    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_NotEqual() throws Exception {
    String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME <> 'TABLES'";
    String scan = "Scan(groupscan=[COLUMNS, filter=not_equal(Field=TABLE_NAME,Literal=TABLES)])";
    testHelper(query, scan, false);
  }

  @Test
  public void testFilterPushDownWithProject_Like() throws Exception {
    String query = "SELECT COLUMN_NAME from INFORMATION_SCHEMA.`COLUMNS` WHERE TABLE_NAME LIKE '%BL%'";
    String scan = "Scan(groupscan=[COLUMNS, filter=like(Field=TABLE_NAME,Literal=%BL%)])";
    testHelper(query, scan, false);
  }

  @Test
  public void testPartialFilterPushDownWithProject() throws Exception {
    String query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
        "TABLE_SCHEMA = 'sys' AND " +
        "TABLE_NAME = 'version' AND " +
        "COLUMN_NAME like 'commit%s'";
    String scan = "Scan(groupscan=[COLUMNS, filter=booleanand(equal(Field=TABLE_SCHEMA,Literal=sys)," +
        "equal(Field=TABLE_NAME,Literal=version))])";

    testHelper(query, scan, true);
  }

  private void testHelper(String query, String filterInScan, boolean filterPrelExpected) throws Exception {
    String plan = getPlanInString("EXPLAIN PLAN FOR " + query, OPTIQ_FORMAT);

    if (!filterPrelExpected) {
      // If filter prel is not expected, make sure it is not in plan
      assert !plan.contains("Filter(");
    } else {
      assert plan.contains("Filter(");
    }

    // Check for filter pushed into scan.
    assert plan.contains(filterInScan);

    // run the query
    test(query);
  }
}
