/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.impersonation;

import com.google.common.collect.Maps;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestImpersonationDisabledWithMiniDFS extends BaseTestImpersonation {

  @BeforeClass
  public static void setup() throws Exception {
    startMiniDfsCluster(TestImpersonationDisabledWithMiniDFS.class.getSimpleName(), false);
    startDrillCluster(false);
    addMiniDfsBasedStorage(Maps.<String, WorkspaceConfig>newHashMap());
    createTestData();
  }

  private static void createTestData() throws Exception {
    // Create test table in minidfs.tmp schema for use in test queries
    test(String.format("CREATE TABLE %s.tmp.dfsRegion AS SELECT * FROM cp.`region.json`",
        MINIDFS_STORAGE_PLUGIN_NAME));
  }

  @Test // DRILL-3037
  public void testSimpleQuery() throws Exception {
    final String query =
        String.format("SELECT sales_city, sales_country FROM tmp.dfsRegion ORDER BY region_id DESC LIMIT 2");

    testBuilder()
        .optionSettingQueriesForTestQuery(String.format("USE %s", MINIDFS_STORAGE_PLUGIN_NAME))
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city", "sales_country")
        .baselineValues("Santa Fe", "Mexico")
        .baselineValues("Santa Anita", "Mexico")
        .go();
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    getDrillbitContext().getStorage().deletePlugin(MINIDFS_STORAGE_PLUGIN_NAME);
    stopMiniDfsCluster();
  }
}
