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
package org.apache.drill.exec.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.util.StoragePluginTestUtils;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.junit.After;
import org.junit.Test;

/**
 * Tests the storage plugin registry. Plugins are (at present)
 * tightly coupled to the Drillbit context so we need to start
 * a Drillbit per tests to ensure each test works from a clean,
 * known registry.
 * <p>
 * This is several big tests because of the setup cost of
 * starting the Drillbits in the needed config.
 */
public class TestPluginRegistry extends BasePluginRegistryTest {

  @After
  public void cleanup() throws Exception {
    FileUtils.cleanDirectory(dirTestWatcher.getStoreDir());
  }

  @Test
  public void testBasicLifecycle() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    try (ClusterFixture cluster = builder.build();) {
      StoragePluginRegistry registry = cluster.storageRegistry();

      // Bootstrap file loaded.
      assertNotNull(registry.getPlugin(StoragePluginTestUtils.CP_PLUGIN_NAME)); // Normal
      assertNotNull(registry.getPlugin("sys")); // System
      assertNull(registry.getConfig("sys")); // Not editable

      assertNull(registry.getPlugin("bogus"));

      // Enabled plugins
      Map<String, StoragePluginConfig> configMap = registry.enabledConfigs();
      assertTrue(configMap.containsKey(StoragePluginTestUtils.CP_PLUGIN_NAME));
      assertFalse(configMap.containsKey("s3")); // Disabled, but still appears
      assertFalse(configMap.containsKey("sys"));

      // All stored plugins, including disabled
      configMap = registry.storedConfigs();
      assertTrue(configMap.containsKey(StoragePluginTestUtils.CP_PLUGIN_NAME));
      assertTrue(configMap.containsKey("s3")); // Disabled, but still appears
      assertNotNull(configMap.get("s3"));
      assertSame(registry.getConfig("s3"), configMap.get("s3"));
      assertFalse(configMap.containsKey("sys"));
      int bootstrapCount = configMap.size();

      // Create a new plugin
      FileSystemConfig pConfig1 = new FileSystemConfig("myConn",
          new HashMap<>(), new HashMap<>(), new HashMap<>());
      pConfig1.setEnabled(true);
      registry.put("myPlugin", pConfig1);
      StoragePlugin plugin1 = registry.getPlugin("myPlugin");
      assertNotNull(plugin1);
      assertSame(plugin1, registry.getPlugin(pConfig1));
      configMap = registry.storedConfigs();

      // Names converted to lowercase in persistent storage
      assertTrue(configMap.containsKey("myplugin"));
      assertEquals(bootstrapCount + 1, configMap.size());

      // Names are case-insensitive
      assertSame(plugin1, registry.getPlugin("myplugin"));
      assertSame(plugin1, registry.getPlugin("MYPLUGIN"));

      // Update the plugin
      Map<String, String> props = new HashMap<>();
      props.put("foo", "bar");
      FileSystemConfig pConfig2 = new FileSystemConfig("myConn",
          props, new HashMap<>(), new HashMap<>());
      pConfig2.setEnabled(true);
      registry.put("myPlugin", pConfig2);
      StoragePlugin plugin2 = registry.getPlugin("myPlugin");
      assertNotSame(plugin1, plugin2);
      assertTrue(plugin2 instanceof FileSystemPlugin);
      FileSystemPlugin fsStorage = (FileSystemPlugin) plugin2;
      assertSame(pConfig2, fsStorage.getConfig());
      assertSame(plugin2, registry.getPlugin(pConfig2));

      // Suppose a query was planned with plugin1 and now starts
      // to execute. Plugin1 has been replaced with plugin2. However
      // the registry moved the old plugin to ephemeral storage where
      // it can still be found by configuration.
      StoragePlugin ePlugin1 = registry.getPlugin(pConfig1);
      assertSame(plugin1, ePlugin1);
      assertNotSame(plugin2, ePlugin1);

      // Now, another thread does the same. It gets the same
      // ephemeral plugin.
      assertSame(plugin1, registry.getPlugin(pConfig1));

      // Change the stored plugin back to the first config.
      registry.put("myPlugin", pConfig1);

      // Now, lets suppose thread 3 starts to execute. It sees the original plugin
      assertSame(plugin1, registry.getPlugin("myPlugin"));

      // But, the ephemeral plugin lives on. Go back to the second
      // config.
      registry.put("myPlugin", pConfig2);
      assertSame(plugin2, registry.getPlugin("myPlugin"));

      // Thread 4, using the first config from planning in thread 3,
      // still sees the first plugin.
      assertSame(plugin1, registry.getPlugin(pConfig1));

      // Disable
      pConfig2.setEnabled(false);
      assertNull(registry.getPlugin("myPlugin"));

      // Though disabled, a running query will create an ephemeral
      // plugin for the config.
      assertSame(plugin2, registry.getPlugin(pConfig2));

      // Disabling an ephemeral plugin neither makes sense
      // nor will have any effect.
      ePlugin1.getConfig().setEnabled(false);
      assertSame(ePlugin1, registry.getPlugin(pConfig1));
      assertTrue(registry.storedConfigs().containsKey("myplugin"));
      assertFalse(registry.enabledConfigs().containsKey("myplugin"));

      // Enable. The config is retrieved from the persistent store.
      // We notice the config is in the ephemeral store and
      // so we restore it.
      pConfig2.setEnabled(true);
      assertSame(plugin2, registry.getPlugin("myPlugin"));
      assertSame(plugin2, registry.getPlugin(pConfig2));
      assertTrue(registry.storedConfigs().containsKey("myplugin"));
      assertTrue(registry.enabledConfigs().containsKey("myplugin"));

      // Delete the plugin
      registry.remove("myPlugin");
      assertNull(registry.getPlugin("myPlugin"));

      // Again a running query will retrieve the plugin from ephemeral storage
      assertSame(plugin1, registry.getPlugin(pConfig1));
      assertSame(plugin2, registry.getPlugin(pConfig2));

      // Delete again, no-op
      registry.remove("myPlugin");

      // The retrieve-from-ephemeral does not kick in if we create
      // a new plugin with the same config but a different name.
      pConfig1.setEnabled(true);
      registry.put("alias", pConfig1);
      StoragePlugin plugin4 = registry.getPlugin("alias");
      assertNotNull(plugin4);
      assertNotSame(plugin1, plugin4);

      // Delete the second name. The config is the same as one already
      // in ephemeral store, so the second is closed. The first will
      // be returned on subsequent queries.
      registry.remove("alias");
      assertNull(registry.getPlugin("alias"));
      assertSame(plugin1, registry.getPlugin(pConfig1));

      // Try to change a system plugin
      StoragePlugin sysPlugin = registry.getPlugin("sys");
      assertNotNull(sysPlugin);
      FileSystemConfig pConfig3 = new FileSystemConfig("myConn",
          props, new HashMap<>(), new HashMap<>());
      pConfig3.setEnabled(true);
      try {
        registry.put("sys", pConfig3);
        fail();
      } catch (UserException e) {
        // Expected
      }
      pConfig3.setEnabled(false);
      try {
        registry.put("sys", pConfig3);
        fail();
      } catch (UserException e) {
        // Expected
      }
      assertSame(sysPlugin, registry.getPlugin("sys"));

      // Try to delete a system plugin
      try {
        registry.remove("sys");
        fail();
      } catch (UserException e) {
        // Expected
      }

      // There is no protection for disabling a system plugin because
      // there is no code that will allow that at present.
    }
  }

  @Test
  public void testStoreSync() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher)
        .withBits("bit1", "bit2");

    // We want a non-buffered, local file system store, in a known location
    // so that the two Drillbits will coordinate roughly he same way they
    // will when using the ZK store in distributed mode.
    builder.configBuilder()
      .put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, true)
      .put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_PATH,
          dirTestWatcher.getStoreDir().getAbsolutePath());
    try (ClusterFixture cluster = builder.build();) {
      StoragePluginRegistry registry1 = cluster.storageRegistry("bit1");
      StoragePluginRegistry registry2 = cluster.storageRegistry("bit2");

      // Define a plugin in Drillbit 1
      FileSystemConfig pConfig1 = new FileSystemConfig("myConn",
          new HashMap<>(), new HashMap<>(), new HashMap<>());
      pConfig1.setEnabled(true);
      registry1.put("myPlugin", pConfig1);
      StoragePlugin plugin1 = registry1.getPlugin("myPlugin");
      assertNotNull(plugin1);

      // Should appear in Drillbit 2
      StoragePlugin plugin2 = registry2.getPlugin("myPlugin");
      assertNotNull(plugin2);
      assertEquals(pConfig1, plugin1.getConfig());

      // Change in Drillbit 1
      Map<String, String> props = new HashMap<>();
      props.put("foo", "bar");
      FileSystemConfig pConfig3 = new FileSystemConfig("myConn",
          props, new HashMap<>(), new HashMap<>());
      pConfig3.setEnabled(true);
      registry1.put("myPlugin", pConfig3);
      plugin1 = registry1.getPlugin("myPlugin");
      assertSame(pConfig3, plugin1.getConfig());

      // Change should appear in Drillbit 2
      plugin2 = registry2.getPlugin("myPlugin");
      assertNotNull(plugin2);
      assertEquals(pConfig3, plugin1.getConfig());

      // Delete in Drillbit 2
      registry2.remove("myPlugin");

      // Should not be available in Drillbit 1
      assertNull(registry1.getPlugin("myPlugin"));
    }
  }
}
