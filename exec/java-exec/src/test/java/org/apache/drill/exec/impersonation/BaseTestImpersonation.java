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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class BaseTestImpersonation extends PlanTestBase {
  protected static final String MINIDFS_STORAGE_PLUGIN_NAME = "miniDfsPlugin";
  protected static final String processUser = System.getProperty("user.name");

  protected static MiniDFSCluster dfsCluster;
  protected static Configuration dfsConf;
  protected static FileSystem fs;
  protected static String miniDfsStoragePath;

  // Test users and groups
  protected static final String[] org1Users = { "user0_1", "user1_1", "user2_1", "user3_1", "user4_1", "user5_1" };
  protected static final String[] org1Groups = { "group0_1", "group1_1", "group2_1", "group3_1", "group4_1", "group5_1" };
  protected static final String[] org2Users = { "user0_2", "user1_2", "user2_2", "user3_2", "user4_2", "user5_2" };
  protected static final String[] org2Groups = { "group0_2", "group1_2", "group2_2", "group3_2", "group4_2", "group5_2" };

  static {
    // "user0_1" belongs to "groups0_1". From "user1_1" onwards each user belongs to corresponding group and the group
    // before it, i.e "user1_1" belongs to "group1_1" and "group0_1" and so on.
    UserGroupInformation.createUserForTesting(org1Users[0], new String[]{org1Groups[0]});
    for(int i=1; i<org1Users.length; i++) {
      UserGroupInformation.createUserForTesting(org1Users[i], new String[] { org1Groups[i], org1Groups[i-1] });
    }

    UserGroupInformation.createUserForTesting(org2Users[0], new String[] { org2Groups[0] });
    for(int i=1; i<org2Users.length; i++) {
      UserGroupInformation.createUserForTesting(org2Users[i], new String[] { org2Groups[i], org2Groups[i-1] });
    }
  }

  /**
   * Start a MiniDFS cluster backed Drillbit cluster with impersonation enabled.
   * @param testClass
   * @throws Exception
   */
  protected static void startMiniDfsCluster(String testClass) throws Exception {
    startMiniDfsCluster(testClass, true);
  }

  /**
   * Start a MiniDFS cluster backed Drillbit cluster
   * @param testClass
   * @param isImpersonationEnabled Enable impersonation in the cluster?
   * @throws Exception
   */
  protected static void startMiniDfsCluster(
      final String testClass, final boolean isImpersonationEnabled) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(testClass), "Expected a non-null and non-empty test class name");
    dfsConf = new Configuration();

    // Set the MiniDfs base dir to be the temp directory of the test, so that all files created within the MiniDfs
    // are properly cleanup when test exits.
    miniDfsStoragePath = System.getProperty("java.io.tmpdir") + Path.SEPARATOR + testClass;
    dfsConf.set("hdfs.minidfs.basedir", miniDfsStoragePath);

    if (isImpersonationEnabled) {
      // Set the proxyuser settings so that the user who is running the Drillbits/MiniDfs can impersonate other users.
      dfsConf.set(String.format("hadoop.proxyuser.%s.hosts", processUser), "*");
      dfsConf.set(String.format("hadoop.proxyuser.%s.groups", processUser), "*");
    }

    // Start the MiniDfs cluster
    dfsCluster = new MiniDFSCluster.Builder(dfsConf)
        .numDataNodes(3)
        .format(true)
        .build();

    fs = dfsCluster.getFileSystem();
  }

  protected static void startDrillCluster(final boolean isImpersonationEnabled) throws Exception {
    final Properties props = cloneDefaultTestConfigProperties();
    props.setProperty(ExecConstants.IMPERSONATION_ENABLED, Boolean.toString(isImpersonationEnabled));

    updateTestCluster(1, DrillConfig.create(props));
  }

  protected static void addMiniDfsBasedStorage(final Map<String, WorkspaceConfig> workspaces)
      throws Exception {
    // Create a HDFS based storage plugin based on local storage plugin and add it to plugin registry (connection string
    // for mini dfs is varies for each run).
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    final FileSystemConfig lfsPluginConfig = (FileSystemConfig) pluginRegistry.getPlugin("dfs_test").getConfig();

    final FileSystemConfig miniDfsPluginConfig = new FileSystemConfig();
    miniDfsPluginConfig.connection = dfsConf.get(FileSystem.FS_DEFAULT_NAME_KEY);

    createAndAddWorkspace("tmp", "/tmp", (short)0777, processUser, processUser, workspaces);

    miniDfsPluginConfig.workspaces = workspaces;
    miniDfsPluginConfig.formats = ImmutableMap.copyOf(lfsPluginConfig.formats);
    miniDfsPluginConfig.setEnabled(true);

    pluginRegistry.createOrUpdate(MINIDFS_STORAGE_PLUGIN_NAME, miniDfsPluginConfig, true);
  }

  protected static void createAndAddWorkspace(String name, String path, short permissions, String owner,
      String group, final Map<String, WorkspaceConfig> workspaces) throws Exception {
    final Path dirPath = new Path(path);
    FileSystem.mkdirs(fs, dirPath, new FsPermission(permissions));
    fs.setOwner(dirPath, owner, group);
    final WorkspaceConfig ws = new WorkspaceConfig(path, true, "parquet");
    workspaces.put(name, ws);
  }

  protected static void stopMiniDfsCluster() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }

    if (miniDfsStoragePath != null) {
      FileUtils.deleteQuietly(new File(miniDfsStoragePath));
    }
  }
}
