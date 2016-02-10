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
package org.apache.drill.exec.store.jdbc;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.proto.UserBitShared;

import org.joda.time.DateTime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * JDBC storage plugin tests against Derby.
 */
public class TestJdbcPluginWithDerbyIT extends PlanTestBase {

  @Test
  public void testCrossSourceMultiFragmentJoin() throws Exception {
    testNoResult("USE derby");
    testNoResult("SET `planner.slice_target` = 1");
    String query = "select x.person_id, y.salary from DRILL_DERBY_TEST.PERSON x "
        + "join dfs.`${WORKING_PATH}/src/test/resources/jdbcmulti/` y on x.person_id = y.person_id ";
    test(query);
  }

  @Test
  public void validateResult() throws Exception {

    // Skip date, time, and timestamp types since derby mangles these due to improper timezone support.
    testBuilder()
            .sqlQuery(
                    "select PERSON_ID, FIRST_NAME, LAST_NAME, ADDRESS, CITY, STATE, ZIP, JSON, BIGINT_FIELD, SMALLINT_FIELD, " +
                            "NUMERIC_FIELD, BOOLEAN_FIELD, DOUBLE_FIELD, FLOAT_FIELD, REAL_FIELD, TIME_FIELD, TIMESTAMP_FIELD, " +
                            "DATE_FIELD, CLOB_FIELD from derby.DRILL_DERBY_TEST.PERSON")
            .ordered()
            .baselineColumns("PERSON_ID", "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE", "ZIP", "JSON",
                    "BIGINT_FIELD", "SMALLINT_FIELD", "NUMERIC_FIELD", "BOOLEAN_FIELD", "DOUBLE_FIELD",
                    "FLOAT_FIELD", "REAL_FIELD", "TIME_FIELD", "TIMESTAMP_FIELD", "DATE_FIELD", "CLOB_FIELD")
            .baselineValues(1, "first_name_1", "last_name_1", "1401 John F Kennedy Blvd",   "Philadelphia",     "PA",
                            19107, "{ a : 5, b : 6 }",            123456L,         1, 10.01, false, 1.0, 1.1, 111.00,
                            new DateTime(1970, 1, 1, 13, 0, 1), new DateTime(2012, 2, 29, 13, 0, 1), new DateTime(2012, 2, 29, 0, 0, 0), "some clob data 1")
            .baselineValues(2, "first_name_2", "last_name_2", "One Ferry Building",         "San Francisco",    "CA",
                            94111, "{ foo : \"abc\" }",            95949L,         2, 20.02, true, 2.0, 2.1, 222.00,
                            new DateTime(1970, 1, 1, 23, 59, 59),  new DateTime(1999, 9, 9, 23, 59, 59), new DateTime(1999, 9, 9, 0, 0, 0), "some more clob data")
            .baselineValues(3, "first_name_3", "last_name_3", "176 Bowery",                 "New York",         "NY",
                            10012, "{ z : [ 1, 2, 3 ] }",           45456L,        3, 30.04, true, 3.0, 3.1, 333.00,
                            new DateTime(1970, 1, 1, 11, 34, 21),  new DateTime(2011, 10, 30, 11, 34, 21), new DateTime(2011, 10, 30, 0, 0, 0), "clobber")
            .baselineValues(4, null, null, "2 15th St NW", "Washington", "DC", 20007, "{ z : { a : 1, b : 2, c : 3 } " +
                    "}", -67L, 4, 40.04, false, 4.0, 4.1, 444.00, new DateTime(1970, 1, 1, 16, 0, 1), new DateTime
                    (2015, 6, 1, 16, 0, 1),  new DateTime(2015, 6, 1, 0, 0, 0), "xxx")
            .baselineValues(5, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                            null, null, null, null)
            .build().run();
  }

  @Test
  public void pushdownJoin() throws Exception {
    testNoResult("use derby");
    String query = "select x.person_id from (select person_id from DRILL_DERBY_TEST.PERSON) x "
            + "join (select person_id from DRILL_DERBY_TEST.PERSON) y on x.person_id = y.person_id ";
    testPlanMatchingPatterns(query, new String[]{}, new String[]{"Join"});
  }

  @Test
  public void pushdownJoinAndFilterPushDown() throws Exception {
    final String query = "select * from \n" +
            "derby.DRILL_DERBY_TEST.PERSON e\n" +
            "INNER JOIN \n" +
            "derby.DRILL_DERBY_TEST.PERSON s\n" +
            "ON e.FIRST_NAME = s.FIRST_NAME\n" +
            "WHERE e.LAST_NAME > 'hello'";

    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join", "Filter" });
  }

  @Test
  public void pushdownAggregation() throws Exception {
    final String query = "select count(*) from derby.DRILL_DERBY_TEST.PERSON";
    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Aggregate" });
  }

  @Test
  public void pushdownDoubleJoinAndFilter() throws Exception {
    final String query = "select * from \n" +
            "derby.DRILL_DERBY_TEST.PERSON e\n" +
            "INNER JOIN \n" +
            "derby.DRILL_DERBY_TEST.PERSON s\n" +
            "ON e.PERSON_ID = s.PERSON_ID\n" +
            "INNER JOIN \n" +
            "derby.DRILL_DERBY_TEST.PERSON ed\n" +
            "ON e.PERSON_ID = ed.PERSON_ID\n" +
            "WHERE s.FIRST_NAME > 'abc' and ed.FIRST_NAME > 'efg'";
    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join", "Filter" });
  }

  @Test
  public void showTablesDefaultSchema() throws Exception {
    testNoResult("use derby");
    assertEquals(1, testRunAndPrint(UserBitShared.QueryType.SQL, "show tables like 'PERSON'"));
  }

  @Test
  public void describe() throws Exception {
    testNoResult("use derby");
    assertEquals(19, testRunAndPrint(UserBitShared.QueryType.SQL, "describe PERSON"));
  }

  @Test
  public void ensureDrillFunctionsAreNotPushedDown() throws Exception {
    // This should verify that we're not trying to push CONVERT_FROM into the JDBC storage plugin. If were pushing
    // this function down, the SQL query would fail.
    testNoResult("select CONVERT_FROM(JSON, 'JSON') from derby.DRILL_DERBY_TEST.PERSON where PERSON_ID = 4");
  }

  @Test
  public void pushdownFilter() throws Exception {
    testNoResult("use derby");
    String query = "select * from DRILL_DERBY_TEST.PERSON where PERSON_ID = 1";
    testPlanMatchingPatterns(query, new String[]{}, new String[]{"Filter"});
  }
}
