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

import org.apache.drill.categories.JdbcStorageTest;
import org.apache.drill.PlanTestBase;

import org.joda.time.DateTime;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * JDBC storage plugin tests against MySQL.
 */
@Category(JdbcStorageTest.class)
public class TestJdbcPluginWithMySQLIT extends PlanTestBase {

  @Test
  public void validateResult() throws Exception {

    testBuilder()
            .sqlQuery(
                    "select person_id, " +
                            "first_name, last_name, address, city, state, zip, " +
                            "bigint_field, smallint_field, numeric_field, " +
                            "boolean_field, double_field, float_field, real_field, " +
                            "date_field, datetime_field, year_field, " +
                            "json, text_field, tiny_text_field, medium_text_field, long_text_field, " +
                            "blob_field, bit_field, enum_field " +
                    "from mysql.`drill_mysql_test`.person")
            .ordered()
            .baselineColumns("person_id",
                    "first_name", "last_name", "address", "city", "state", "zip",
                    "bigint_field", "smallint_field", "numeric_field",
                    "boolean_field",
                    "double_field", "float_field", "real_field",
                    "date_field", "datetime_field", "year_field",
                    "json", "text_field", "tiny_text_field", "medium_text_field", "long_text_field",
                    "blob_field", "bit_field", "enum_field")
            .baselineValues(1,
                    "first_name_1", "last_name_1", "1401 John F Kennedy Blvd", "Philadelphia", "PA", 19107,
                    123456789L, 1, 10.01,
                    false,
                    1.0, 1.1, 1.2,
                    new DateTime(2012, 2, 29, 0, 0, 0), new DateTime(2012, 2, 29, 13, 0, 1), new DateTime(2015, 1, 1, 0, 0, 0),
                    "{ a : 5, b : 6 }",
                    "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
                    "xxx",
                    "a medium piece of text",
                    "a longer piece of text this is going on.....",
                    "this is a test".getBytes(),
                    true, "XXX")
            .baselineValues(2,
                    "first_name_2", "last_name_2", "One Ferry Building", "San Francisco", "CA", 94111,
                    45456767L, 3, 30.04,
                    true,
                    3.0, 3.1, 3.2,
                    new DateTime(2011, 10, 30, 0, 0, 0), new DateTime(2011, 10, 30, 11, 34, 21), new DateTime(2015, 1, 1, 0, 0, 0),
                    "{ z : [ 1, 2, 3 ] }",
                    "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
                    "abc",
                    "a medium piece of text 2",
                    "somewhat more text",
                    "this is a test 2".getBytes(),
                    false, "YYY")
            .baselineValues(3,
                    "first_name_3", "last_name_3", "176 Bowery", "New York", "NY", 10012,
                    123090L, -3, 55.12,
                    false,
                    5.0, 5.1, 5.55,
                    new DateTime(2015, 6, 1, 0, 0, 0), new DateTime(2015, 9, 22, 15, 46, 10), new DateTime(1901, 1, 1, 0, 0, 0),
                    "{ [ a, b, c ] }",
                    "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit",
                    "abc",
                    "a medium piece of text 3",
                    "somewhat more text",
                    "this is a test 3".getBytes(),
                    true, "ZZZ")
            .baselineValues(5,
                    null, null, null, null, null, null,
                    null, null, null,
                    null,
                    null, null, null,
                    null, null, null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null, "XXX")
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

  @Test
  public void testPhysicalPlanSubmission() throws Exception {
    testPhysicalPlanExecutionBasedOnQuery("select * from mysql.`drill_mysql_test`.person");
  }

}
