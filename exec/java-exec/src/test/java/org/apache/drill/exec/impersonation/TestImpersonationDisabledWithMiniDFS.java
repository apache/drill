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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class TestImpersonationDisabledWithMiniDFS extends BaseTestImpersonation {
  private static final String MINIDFS_STORAGE_PLUGIN_NAME =
      "minidfs" + TestImpersonationDisabledWithMiniDFS.class.getSimpleName();

  @BeforeClass
  public static void addMiniDfsBasedStorage() throws Exception {
    startMiniDfsCluster(TestImpersonationDisabledWithMiniDFS.class.getSimpleName(), false);

    // Create a HDFS based storage plugin based on local storage plugin and add it to plugin registry (connection string
    // for mini dfs is varies for each run).
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    final FileSystemConfig lfsPluginConfig = (FileSystemConfig) pluginRegistry.getPlugin("dfs_test").getConfig();

    final FileSystemConfig miniDfsPluginConfig = new FileSystemConfig();
    miniDfsPluginConfig.connection = conf.get(FileSystem.FS_DEFAULT_NAME_KEY);

    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap(lfsPluginConfig.workspaces);
    createAndAddWorkspace(dfsCluster.getFileSystem(), "dfstemp", "/tmp", (short)0777, processUser, processUser, workspaces);

    miniDfsPluginConfig.workspaces = workspaces;
    miniDfsPluginConfig.formats = ImmutableMap.copyOf(lfsPluginConfig.formats);
    miniDfsPluginConfig.setEnabled(true);

    pluginRegistry.createOrUpdate(MINIDFS_STORAGE_PLUGIN_NAME, miniDfsPluginConfig, true);

    // Create test table in minidfs.tmp schema for use in test queries
    test(String.format("CREATE TABLE %s.dfstemp.dfsRegion AS SELECT * FROM cp.`region.json`", MINIDFS_STORAGE_PLUGIN_NAME));
  }

  @Test // DRILL-3037
  public void testSimpleQuery() throws Exception {
    final String query =
        String.format("SELECT sales_city, sales_country FROM dfstemp.dfsRegion ORDER BY region_id DESC LIMIT 2");

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
