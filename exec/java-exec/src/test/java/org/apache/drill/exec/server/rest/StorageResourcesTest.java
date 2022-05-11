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
package org.apache.drill.exec.server.rest;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.mock.MockStorageEngineConfig;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class StorageResourcesTest extends ClusterTest {
  @BeforeClass
  public static void setUp() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true);

    startCluster(builder);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    cluster.close();
  }

  @Test
  public void testCreateStorageConfig() throws Exception {
    try {
      PluginConfigWrapper pcw = new PluginConfigWrapper(MockStorageEngineConfig.NAME, MockStorageEngineConfig.INSTANCE);
      cluster.restClientFixture().postStorageConfig(pcw);
      StoragePluginConfig config_server = cluster.storageRegistry().getDefinedConfig(MockStorageEngineConfig.NAME);
      Assert.assertEquals(MockStorageEngineConfig.INSTANCE, config_server);
      Assert.assertTrue(config_server.isEnabled());
    } finally {
      cluster.storageRegistry().remove(MockStorageEngineConfig.NAME);
    }
  }

  @Test
  public void testEnabledToggle() throws Exception {
    try {
      PluginConfigWrapper pcw = new PluginConfigWrapper(MockStorageEngineConfig.NAME, MockStorageEngineConfig.INSTANCE);
      cluster.restClientFixture().postStorageConfig(pcw);
      cluster.restClientFixture().toggleEnabled(MockStorageEngineConfig.NAME, false);
      StoragePluginConfig config_server = cluster.storageRegistry().getStoredConfig(MockStorageEngineConfig.NAME);
      Assert.assertEquals(pcw.getConfig(), config_server);
      Assert.assertFalse(config_server.isEnabled());
    } finally {
      cluster.storageRegistry().remove(MockStorageEngineConfig.NAME);
    }
  }

  @Test
  public void testDeleteStorageConfig() throws Exception {
    try {
      PluginConfigWrapper pcw = new PluginConfigWrapper(MockStorageEngineConfig.NAME, MockStorageEngineConfig.INSTANCE);
      cluster.restClientFixture().postStorageConfig(pcw);
      cluster.restClientFixture().deleteStorageConfig(MockStorageEngineConfig.NAME);
      StoragePluginConfig config_server = cluster.storageRegistry().getStoredConfig(MockStorageEngineConfig.NAME);
      Assert.assertNull(config_server);
    } finally {
      cluster.storageRegistry().remove(MockStorageEngineConfig.NAME);
    }
  }
}
