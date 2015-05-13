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
import org.apache.commons.io.FileUtils;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class BaseTestImpersonation extends PlanTestBase {
  protected static final String processUser = System.getProperty("user.name");

  protected static MiniDFSCluster dfsCluster;
  protected static Configuration conf;
  protected static String miniDfsStoragePath;

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
    conf = new Configuration();

    // Set the MiniDfs base dir to be the temp directory of the test, so that all files created within the MiniDfs
    // are properly cleanup when test exits.
    miniDfsStoragePath = System.getProperty("java.io.tmpdir") + Path.SEPARATOR + testClass;
    conf.set("hdfs.minidfs.basedir", miniDfsStoragePath);

    if (isImpersonationEnabled) {
      // Set the proxyuser settings so that the user who is running the Drillbits/MiniDfs can impersonate other users.
      conf.set(String.format("hadoop.proxyuser.%s.hosts", processUser), "*");
      conf.set(String.format("hadoop.proxyuser.%s.groups", processUser), "*");
    }

    // Start the MiniDfs cluster
    dfsCluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .format(true)
        .build();

    final Properties props = cloneDefaultTestConfigProperties();
    props.setProperty(ExecConstants.IMPERSONATION_ENABLED, Boolean.toString(isImpersonationEnabled));

    updateTestCluster(1, DrillConfig.create(props));
  }

  protected static void createAndAddWorkspace(FileSystem fs, String name, String path, short permissions, String owner,
      String group,
      Map<String, WorkspaceConfig> workspaces) throws Exception {
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
