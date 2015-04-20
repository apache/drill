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
package org.apache.drill.exec.util;

import com.google.common.io.Files;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;

import java.io.File;

/**
 * This class contains utility methods to speed up tests. Some of the production code currently calls this method
 * when the production code is executed as part of the test runs. That's the reason why this code has to be in
 * production module.
 */
public class TestUtilities {
  // Below two variable values are derived from
  // <DRILL_SRC_HOME>/exec/java-exec/src/main/resources/bootstrap-storage-plugins.json.
  private static final String dfsPluginName = "dfs";
  private static final String dfsTmpSchema = "tmp";

  // Below two variable values are derived from
  // <DRILL_SRC_HOME>/exec/java-exec/src/test/resources/bootstrap-storage-plugins.json.
  private static final String dfsTestPluginName = "dfs_test";
  private static final String dfsTestTmpSchema = "tmp";

  /**
   * Create and removes a temporary folder
   *
   * @return absolute path to temporary folder
   */
  public static String createTempDir() {
    final File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    return tmpDir.getAbsolutePath();
  }

  /**
   * Update the location of dfs_test.tmp location. Get the "dfs_test.tmp" workspace and update the location with an
   * exclusive temp directory just for use in the current test jvm.
   *
   * @param pluginRegistry
   * @return JVM exclusive temporary directory location.
   */
  public static void updateDfsTestTmpSchemaLocation(final StoragePluginRegistry pluginRegistry,
                                                      final String tmpDirPath)
      throws ExecutionSetupException {
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin(dfsTestPluginName);
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    final WorkspaceConfig tmpWSConfig = pluginConfig.workspaces.get(dfsTestTmpSchema);

    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(tmpDirPath, true, tmpWSConfig.getDefaultInputFormat());

    pluginConfig.workspaces.remove(dfsTestTmpSchema);
    pluginConfig.workspaces.put(dfsTestTmpSchema, newTmpWSConfig);

    pluginRegistry.createOrUpdate(dfsTestPluginName, pluginConfig, true);
  }

  /**
   * Make the dfs.tmp schema immutable, so that tests writers don't use the dfs.tmp to create views.
   * Schema "dfs.tmp" added as part of the default bootstrap plugins file that comes with drill-java-exec jar
   */
  public static void makeDfsTmpSchemaImmutable(final StoragePluginRegistry pluginRegistry) throws ExecutionSetupException {
    final FileSystemPlugin dfsPlugin = (FileSystemPlugin) pluginRegistry.getPlugin(dfsPluginName);
    final FileSystemConfig dfsPluginConfig = (FileSystemConfig) dfsPlugin.getConfig();
    final WorkspaceConfig tmpWSConfig = dfsPluginConfig.workspaces.get(dfsTmpSchema);

    final WorkspaceConfig newTmpWSConfig = new WorkspaceConfig(tmpWSConfig.getLocation(), false,
        tmpWSConfig.getDefaultInputFormat());

    dfsPluginConfig.workspaces.remove(dfsTmpSchema);
    dfsPluginConfig.workspaces.put(dfsTmpSchema, newTmpWSConfig);

    pluginRegistry.createOrUpdate(dfsPluginName, dfsPluginConfig, true);
  }
}
