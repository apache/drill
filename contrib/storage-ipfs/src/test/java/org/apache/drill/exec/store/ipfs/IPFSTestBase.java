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


package org.apache.drill.exec.store.ipfs;


import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPFSTestBase extends ClusterTest {
  private static final Logger logger = LoggerFactory.getLogger(IPFSTestBase.class);
  private static StoragePluginRegistry pluginRegistry;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    builder.configProperty(ExecConstants.INITIAL_BIT_PORT, IPFSGroupScan.DEFAULT_CONTROL_PORT)
        .configProperty(ExecConstants.INITIAL_DATA_PORT, IPFSGroupScan.DEFAULT_DATA_PORT)
        .configProperty(ExecConstants.INITIAL_USER_PORT, IPFSGroupScan.DEFAULT_USER_PORT)
        .configProperty(ExecConstants.DRILL_PORT_HUNT, false)
        .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
        .clusterSize(1)
        .withLocalZk();
    startCluster(builder);
    pluginRegistry = cluster.drillbit().getContext().getStorage();

    IPFSTestSuit.initIPFS();
    initIPFSStoragePlugin();
  }

  private static void initIPFSStoragePlugin() throws Exception {
    pluginRegistry
        .put(
            IPFSStoragePluginConfig.NAME,
            IPFSTestSuit.getIpfsStoragePluginConfig());
  }

  @AfterClass
  public static void tearDownIPFSTestBase() throws StoragePluginRegistry.PluginException {
    if (pluginRegistry != null) {
      pluginRegistry.remove(IPFSStoragePluginConfig.NAME);
    } else {
      logger.warn("Plugin Registry was null");
    }
  }
}