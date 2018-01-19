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
package com.mapr.drill.maprdb.tests.json;

import com.mapr.tests.annotations.ClusterTest;
import org.apache.drill.PlanTestBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ClusterTest.class)
public class TestDataTypes extends BaseJsonTest {

  @Test
  public void testIntervalAdd() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` b "
        + "where b.start_date < DATE '2012-06-01' + INTERVAL '5' MONTH";
    PlanTestBase.testPlanMatchingPatterns(sql,
        new String[] {"Scan.*condition=.*start_date.*dateDay.*2012-11-01"},
        new String[]{"Filter"}
    );

    runSQLAndVerifyCount(sql, 3);
  }

  @Test
  public void testIntervalAdd2() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` b "
        + "where b.start_date <  INTERVAL '5' MONTH + DATE '2012-06-01'";
    PlanTestBase.testPlanMatchingPatterns(sql,
        new String[] {"Scan.*condition=.*start_date.*dateDay.*2012-11-01"},
        new String[]{"Filter"}
    );

    runSQLAndVerifyCount(sql, 3);
  }

  @Test
  public void testIntervalSub() throws Exception {
    final String sql = "SELECT\n"
        + "  *\n"
        + "FROM\n"
        + "  hbase.`business` b "
        + "where b.start_date < DATE '2012-12-01' - INTERVAL '30' DAY";
    PlanTestBase.testPlanMatchingPatterns(sql,
        new String[] {"Scan.*condition=.*start_date.*dateDay.*2012-11-01"},
        new String[]{"Filter"}
    );

    runSQLAndVerifyCount(sql, 3);
  }

}
