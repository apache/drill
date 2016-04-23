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
package org.apache.drill;

import org.junit.Test;

public class TestAggregationQueries extends PlanTestBase {

  @Test // DRILL-4521
  public void ensureVarianceIsAggregateReduced() throws Exception {
    String query01 = "select variance(salary) from cp.`employee.json`";
    testPlanSubstrPatterns(query01,
            new String[] {"EXPR$0=[divide(-($0, /(*($1, $1), $2)), CASE(=($2, 1), null, -($2, 1)))]"},
            new String[] {"EXPR$0=[VARIANCE($0)]"});
    testBuilder().sqlQuery(query01).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(2.8856749581279494E7).go();

    String query02 = "select var_samp(salary) from cp.`employee.json`";
    testBuilder().sqlQuery(query02).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(2.8856749581279494E7).go();

    String query03 = "select var_pop(salary) from cp.`employee.json`";
    testBuilder().sqlQuery(query03).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(2.8831765382507823E7).go();
  }

  @Test // DRILL-4521
  public void ensureStddevIsAggregateReduced() throws Exception {
    String query01 = "select stddev(salary) from cp.`employee.json`";
    testPlanSubstrPatterns(query01,
            new String[] { "EXPR$0=[POWER(divide(-($0, /(*($1, $1), $2)), CASE(=($2, 1), null, -($2, 1))), 0.5)]"},
            new String[] {"EXPR$0=[STDDEV($0)]"});
    testBuilder().sqlQuery(query01).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(5371.84787398894).go();

    String query02 = "select stddev_samp(salary) from cp.`employee.json`";
    testBuilder().sqlQuery(query02).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(5371.84787398894).go();

    String query03 = "select stddev_pop(salary) from cp.`employee.json`";
    testBuilder().sqlQuery(query03).approximateEquality().unOrdered().baselineColumns("EXPR$0").baselineValues(5369.521895151171).go();
  }
}
