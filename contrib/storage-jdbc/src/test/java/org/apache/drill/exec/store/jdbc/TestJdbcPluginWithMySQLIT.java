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

import org.joda.time.DateTime;

import org.junit.Test;


/**
 * JDBC storage plugin tests against MySQL.
 */
public class TestJdbcPluginWithMySQLIT extends PlanTestBase {

  @Test
  public void validateResult() throws Exception {

    testBuilder()
            .sqlQuery(
                    "select person_id, first_name, last_name, address, city, state, zip, json, date_field, datetime_field from mysql.`drill_mysql_test`.person")
            .ordered()
            .baselineColumns("person_id", "first_name", "last_name", "address", "city", "state", "zip", "json", "date_field", "datetime_field")
            .baselineValues(1, "first_name_1", "last_name_1", "1401 John F Kennedy Blvd", "Philadelphia", "PA", 19107, "{ a : 5, b : 6 }", new DateTime(2012, 2, 29, 0, 0, 0), new DateTime(2012, 2, 29, 13, 0, 1))
            .baselineValues(2, "first_name_2", "last_name_2", "One Ferry Building", "San Francisco", "CA", 94111, "{ foo : \"abc\" }", new DateTime(1999, 9, 9, 0, 0, 0), new DateTime(1999, 9, 9, 23, 59, 59))
            .baselineValues(3, "first_name_3", "last_name_3", "176 Bowery", "New York", "NY", 10012, "{ z : [ 1, 2, 3 ] }", new DateTime(2011, 10, 30, 0, 0, 0), new DateTime(2011, 10, 30, 11, 34, 21))
            .baselineValues(4, "first_name_5", "last_name_5", "Chestnut Hill", "Boston", "MA", 12467, "{ [ a, b, c ] }", new DateTime(2015, 6, 1, 0, 0, 0), new DateTime(2015, 9, 22, 15, 46, 10))
            .baselineValues(5, null, null, null, null, null, null, null, null, null)
            .build().run();
  }

  @Test
  public void pushdownJoin() throws Exception {
    String query = "select x.person_id from (select person_id from mysql.`drill_mysql_test`.person) x "
            + "join (select person_id from mysql.`drill_mysql_test`.person) y on x.person_id = y.person_id ";
    testPlanMatchingPatterns(query, new String[]{}, new String[]{"Join"});
  }

  @Test
  public void pushdownJoinAndFilterPushDown() throws Exception {
    final String query = "select * from " +
            "mysql.`drill_mysql_test`.person e " +
            "INNER JOIN " +
            "mysql.`drill_mysql_test`.person s " +
            "ON e.first_name = s.first_name " +
            "WHERE e.last_name > 'hello'";

    testPlanMatchingPatterns(query, new String[] {}, new String[] { "Join", "Filter" });
  }

}
