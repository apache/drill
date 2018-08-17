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
package org.apache.drill;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.exec.ops.OperatorMetricRegistry;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(OperatorTest.class)
public class TestOperatorMetrics extends BaseTestQuery {

  @Test
  public void testMetricNames() {
    assertEquals(new String[]{"BYTES_SENT"},
              OperatorMetricRegistry.getMetricNames(UserBitShared.CoreOperatorType.SCREEN_VALUE));

    assertEquals(new String[]{"SPILL_COUNT", "RETIRED1", "PEAK_BATCHES_IN_MEMORY", "MERGE_COUNT", "MIN_BUFFER",
                      "INPUT_BATCHES"},
              OperatorMetricRegistry.getMetricNames(UserBitShared.CoreOperatorType.EXTERNAL_SORT_VALUE));
  }

  @Test
  public void testNonExistentMetricNames() {
    assertNull(OperatorMetricRegistry.getMetricNames(UserBitShared.CoreOperatorType.NESTED_LOOP_JOIN_VALUE));

    assertNull(OperatorMetricRegistry.getMetricNames(202));
  }

}
