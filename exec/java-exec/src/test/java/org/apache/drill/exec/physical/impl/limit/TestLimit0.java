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
package org.apache.drill.exec.physical.impl.limit;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.junit.Test;

public class TestLimit0 extends BaseTestQuery {

  @Test
  public void simpleLimit0() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM (" +
        "SELECT * FROM (VALUES (1, 1.0, DATE '2008-2-23', TIME '12:23:34', TIMESTAMP '2008-2-23 12:23:34.456', " +
        "INTERVAL '1' YEAR, INTERVAL '2' DAY), (1, 1.0, DATE '2008-2-23', TIME '12:23:34', " +
        "TIMESTAMP '2008-2-23 12:23:34.456', INTERVAL '1' YEAR, INTERVAL '2' DAY)) AS " +
        "Example(myInt, myFloat, myDate, myTime, myTimestamp, int1, int2)) T LIMIT 0")
        .expectsEmptyResultSet()
        .baselineColumns("myInt", "myFloat", "myDate", "myTime", "myTimestamp", "int1", "int2")
        .go();
  }

  @Test
  public void simpleLimit0Plan() throws Exception {
    PlanTestBase.testPlanMatchingPatterns("SELECT * FROM (" +
        "SELECT * FROM (VALUES (1, 1.0, DATE '2008-2-23', TIME '12:23:34', TIMESTAMP '2008-2-23 12:23:34.456', " +
        "INTERVAL '1' YEAR, INTERVAL '2' DAY), (1, 1.0, DATE '2008-2-23', TIME '12:23:34', " +
        "TIMESTAMP '2008-2-23 12:23:34.456', INTERVAL '1' YEAR, INTERVAL '2' DAY)) AS " +
        "Example(myInt, myFloat, myDate, myTime, myTimestamp, int1, int2)) T LIMIT 0",
        new String[] {
            ".*Limit.*\n" +
                ".*Values.*"
        },
        new String[] {});
  }

  @Test
  public void countDistinctLimit0() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM " +
        "(SELECT CAST(COUNT(employee_id) AS BIGINT) as c1, " +
        "CAST(SUM(employee_id) AS INT) as s1, " +
        "CAST(COUNT(DISTINCT employee_id) AS BIGINT) as c2 " +
        "FROM cp.`employee.json`) " +
        "T LIMIT 0")
        .baselineColumns("c1", "s1", "c2")
        .expectsEmptyResultSet()
        .go();
  }

  @Test
  public void countDistinctLimit0Plan() throws Exception {
    PlanTestBase.testPlanMatchingPatterns("SELECT * FROM " +
            "(SELECT CAST(COUNT(employee_id) AS BIGINT) as c1, " +
            "CAST(SUM(employee_id) AS INT) as s1, " +
            "CAST(COUNT(DISTINCT employee_id) AS BIGINT) as c2 " +
            "FROM cp.`employee.json`) " +
            "T LIMIT 0",
        new String[] {
            ".*Limit.*\n" +
                ".*Values.*"
        },
        new String[] {});
  }
}
