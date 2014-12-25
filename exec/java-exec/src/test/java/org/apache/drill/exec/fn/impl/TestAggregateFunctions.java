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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestAggregateFunctions extends BaseTestQuery {

  /*
   * Test checks the count of a nullable column within a map
   * and verifies count is equal only to the number of times the
   * column appears and doesn't include the null count
   */
  @Test
  public void testCountOnNullableColumn() throws Exception {
    testBuilder()
        .sqlQuery("select count(t.x.y)  as cnt1, count(`integer`) as cnt2 from cp.`/jsoninput/input2.json` t")
        .ordered()
        .baselineColumns("cnt1", "cnt2")
        .baselineValues(3l, 4l)
        .build().run();
  }

  @Test
  public void testCountDistinctOnBoolColumn() throws Exception {
    testBuilder()
        .sqlQuery("select count(distinct `bool_val`) as cnt from `sys`.`options`")
        .ordered()
        .baselineColumns("cnt")
        .baselineValues(2l)
        .build().run();
  }
}
