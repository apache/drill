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
 * JDBC storage plugin tests against Oracle.
 */
public class TestJdbcPluginWithOracleIT extends PlanTestBase {

    @Test
    public void baseTest() throws Exception {
        testBuilder()
                .sqlQuery(
                        "select PERSON_ID, " +
                                "FIRST_NAME, LAST_NAME, ADDRESS, CITY, STATE_FIELD, ZIP, " +
                                "JSON_FIELD, SMALLINT_FIELD, NUMERIC_FIELD, BOOLEAN_FIELD, " +
                                "DOUBLE_FIELD, FLOAT_FIELD, REAL_FIELD, TIMESTAMP_FIELD, " +
                                "DATE_FIELD, TEXT_FIELD, NCHAR_FIELD, BLOB_FIELD, " +
                                "CLOB_FIELD, DECIMAL_FIELD, DEC_FIELD " +
                                "from oracle.`SYSTEM`.`person`")
                .ordered()
                .baselineColumns("PERSON_ID",
                        "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE_FIELD", "ZIP",
                        "JSON_FIELD", "SMALLINT_FIELD", "NUMERIC_FIELD", "BOOLEAN_FIELD",
                        "DOUBLE_FIELD", "FLOAT_FIELD", "REAL_FIELD", "TIMESTAMP_FIELD",
                        "DATE_FIELD", "TEXT_FIELD", "NCHAR_FIELD", "BLOB_FIELD",
                        "CLOB_FIELD", "DECIMAL_FIELD", "DEC_FIELD")
                .baselineValues(1.0,
                        "first_name_1", "last_name_1", "1401 John F Kennedy Blvd", "Philadelphia", "PA", 19107.0,
                        "{ a : 5, b : 6 }", 10.0, 123.0, 0.0, 1.34, 1.112, 1.224, new DateTime(2011, 2, 11, 23, 12, 12),
                        new DateTime(2015, 5, 2, 0, 0),
                        "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout",
                        "1", "this is test".getBytes(), "n----", 123.0, 1.0)
                .baselineValues(2.0,
                        "first_name_2", "last_name_2", "One Ferry Building", "San Francisco", "CA", 94111.0,
                        "{ name : Sem, surname : Sem }", 4.0, 45.0, 1.0, 2.54, 4.242, 4.252, new DateTime(2015, 12, 20, 23, 12, 12),
                        new DateTime(2015, 12, 1, 0, 0), "Some text", "1", "this is test2".getBytes(), "n!!!!",
                        14.0, 0.0)
                .baselineValues(3.0,
                        "first_name_3", "last_name_3", "176 Bowery", "New York", "NY", 10012.0,
                        "{ x : 12, y : 16 }", 0.0, 13.0, 0.0, 41.42, 2.421, 6.12, new DateTime(1901, 1, 1, 23, 12, 12),
                        new DateTime(2015, 5, 2, 12, 12, 30), "Some long text 3", "0", "this is test3".getBytes(), "n????",
                        0.0, 1.0)
                .baselineValues(5.0,
                        null, null, null, null, null, null,
                        null, null, null, null, null, null,
                        null, null, null, null, null, null,
                        null, null, null)

                .build().run();
    }

    @Test
    public void pushdownJoin() throws Exception {
        String query = "select x.person_id from (select person_id from oracle.`SYSTEM`.`person`) x "
                + "join (select person_id from oracle.`SYSTEM`.`person`) y on x.person_id = y.person_id ";
        testPlanMatchingPatterns(query, new String[]{ "JOIN" }, new String[]{ "Join" });
    }

    @Test
    public void pushdownJoinAndFilter() throws Exception {
        final String query = "select * from oracle.`SYSTEM`.`person` e " +
                "inner join oracle.`SYSTEM`.`person` s on e.first_name = s.first_name " +
                "where e.last_name <> 'hello'";

        testPlanMatchingPatterns(query, new String[] { "INNER JOIN" }, new String[] { "Join", "Filter" });
    }

}
