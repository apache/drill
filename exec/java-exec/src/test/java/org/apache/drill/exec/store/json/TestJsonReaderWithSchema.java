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
package org.apache.drill.exec.store.json;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJsonReaderWithSchema extends BaseTestJsonReader {

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testSelectFromListWithCase() throws Exception {
    try {
      testBuilder()
              .sqlQuery("select a, typeOf(a) `type` from " +
                "(select case when is_list(field2) then field2[4][1].inner7 end a " +
                "from cp.`jsoninput/union/a.json`) where a is not null")
              .ordered()
              .optionSettingQueriesForTestQuery("alter session set `exec.enable_union_type` = true")
              .optionSettingQueriesForTestQuery("alter session set `store.json.enable_v2_reader` = false")
              .baselineColumns("a", "type")
              .baselineValues(13L, "BIGINT")
              .go();
    } finally {
      client.resetSession(ExecConstants.ENABLE_UNION_TYPE_KEY);
    }
  }
}
