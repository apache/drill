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
package org.apache.drill.exec.planner.logical;

import org.apache.drill.PlanTestBase;
import org.junit.Test;

public class TestConvertCountToDirectScan extends PlanTestBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestConvertCountToDirectScan.class);

  @Test
  public void ensureCaseDoesntConvertToDirectScan() throws Exception {
    testPlanMatchingPatterns(
        "select count(case when n_name = 'ALGERIA' and n_regionkey = 2 then n_nationkey else null end) as cnt from dfs.`${WORKING_PATH}/src/test/resources/directcount.parquet`",
        new String[] { "CASE" },
        new String[]{});
  }

  @Test
  public void ensureConvertSimpleCountToDirectScan() throws Exception {
    final String sql = "select count(*) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(
        sql,
        new String[] { "PojoRecordReader" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();

  }

  @Test
  public void ensureConvertSimpleCountConstToDirectScan() throws Exception {
    final String sql = "select count(100) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(
        sql,
        new String[] { "PojoRecordReader" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();

  }

  @Test
  public void ensureConvertSimpleCountConstExprToDirectScan() throws Exception {
    final String sql = "select count(1 + 2) as cnt from cp.`tpch/nation.parquet`";
    testPlanMatchingPatterns(
        sql,
        new String[] { "PojoRecordReader" },
        new String[]{});

    testBuilder()
        .sqlQuery(sql)
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(25L)
        .go();

  }

}
