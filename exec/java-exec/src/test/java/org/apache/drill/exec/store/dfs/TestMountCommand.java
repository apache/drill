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
package org.apache.drill.exec.store.dfs;

import org.apache.commons.lang3.SystemUtils;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;

import static org.apache.drill.exec.ExecConstants.FILE_PLUGIN_MOUNT_COMMANDS;
import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(UnlikelyTest.class)
public class TestMountCommand extends ClusterTest {

  private static File testFile;
  private static String touchCmd, rmCmd;

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(FILE_PLUGIN_MOUNT_COMMANDS, true);

    startCluster(builder);

    // A file that will be created by the filesystem plugin's mount command
    // and deleted by its unmount command.
    testFile = new File(String.format(
      "%s/drill-mount-test",
      cluster.getDrillTempDir().getAbsolutePath()
    ));

   if (SystemUtils.IS_OS_WINDOWS) {
     touchCmd = "type nul > %s";
     rmCmd = "del %s";
   } else {
     touchCmd = "touch %s";
     rmCmd = "rm %s";
   }
  }

  @Test
  public void testMountUnmount() throws Exception {
    StoragePluginRegistry pluginRegistry = cluster.storageRegistry();
    FileSystemConfig dfsConfig = (FileSystemConfig) pluginRegistry
      .getDefinedConfig(DFS_PLUGIN_NAME);

    FileSystemConfig dfsConfigNew = new FileSystemConfig(
      dfsConfig.getConnection(),
      Arrays.asList(String.format(touchCmd, testFile.getAbsolutePath()).split(" ")),
      Arrays.asList(String.format(rmCmd, testFile.getAbsolutePath()).split(" ")),
      dfsConfig.getConfig(),
      dfsConfig.getWorkspaces(),
      dfsConfig.getFormats(),
      null, null,
      dfsConfig.getCredentialsProvider()
    );
    dfsConfigNew.setEnabled(true);
    pluginRegistry.put(DFS_PLUGIN_NAME, dfsConfigNew);

    // Run a query to trigger the mount command because plugins are lazily initialised.
    run("show files in %s", DFS_PLUGIN_NAME);
    assertTrue(testFile.exists());

    cluster.storageRegistry().setEnabled(DFS_PLUGIN_NAME, false);
    assertFalse(testFile.exists());
  }
}
