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

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.apache.drill.exec.util.StoragePluginTestUtils.CP_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUserPluginRegistry extends BasePluginRegistry {

    @Test
    public void testSeparatePluginsForUsers() throws Exception {
        ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher)
                .clusterSize(1)
                .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
                .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
                .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
                .configProperty(ExecConstants.SEPARATE_WORKSPACE, true);

        try (ClusterFixture cluster = builder.build();
             ClientFixture client1 = cluster.clientBuilder()
                     .property(DrillProperties.USER, TEST_USER_1)
                     .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
                     .build();
             ClientFixture client2 = cluster.clientBuilder()
                     .property(DrillProperties.USER, TEST_USER_2)
                     .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
                     .build()) {

            Map<String, StoragePluginRegistry> userStorage = new HashMap<>();
            for (Map.Entry<UserServer.BitToUserConnection, UserServer.BitToUserConnectionConfig> userConnection : cluster.drillbit().getContext().getUserConnections()) {
                UserSession session = userConnection.getKey().getSession();
                userStorage.put(session.getCredentials().getUserName(), session.getStorage());
            }

            // Check the separate UserStoragePluginRegistry is used for each user
            assertEquals(2, userStorage.size());
            assertEquals("It should be UserStoragePluginRegistry for separate user", UserStoragePluginRegistry.class,
                    userStorage.get(TEST_USER_1).getClass());
            assertEquals("It should be UserStoragePluginRegistry for separate user", UserStoragePluginRegistry.class,
                    userStorage.get(TEST_USER_2).getClass());
            assertNotSame(userStorage.get(TEST_USER_1), userStorage.get(TEST_USER_2));

            // Create a new plugin for TEST_USER_1
            FileSystemConfig pConfig1 = myConfig1();
            assertFalse(userStorage.get(TEST_USER_1).availablePlugins().contains(MY_PLUGIN_KEY));
            userStorage.get(TEST_USER_1).put(MY_PLUGIN_NAME, pConfig1);

            // Check plugin is created for TEST_USER_1
            assertTrue(userStorage.get(TEST_USER_1).availablePlugins().contains(MY_PLUGIN_KEY));
            StoragePlugin plugin = userStorage.get(TEST_USER_1).getPlugin(MY_PLUGIN_NAME);
            assertNotNull(plugin);
            assertSame(plugin, userStorage.get(TEST_USER_1).getPluginByConfig(pConfig1));

            // Check TEST_USER_2 doesn't have TEST_USER_1's plugin
            assertFalse(userStorage.get(TEST_USER_2).availablePlugins().contains(MY_PLUGIN_KEY));
            StoragePlugin nullPlugin = userStorage.get(TEST_USER_2).getPlugin(MY_PLUGIN_NAME);
            assertNull(nullPlugin);
        }
    }

    @Test
    public void testLifecycleSeparateWorkspace() throws Exception {
        ClusterFixtureBuilder builder = ClusterFixture.bareBuilder(dirTestWatcher)
                .clusterSize(2)
                .configProperty(ExecConstants.ALLOW_LOOPBACK_ADDRESS_BINDING, true)
                .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
                .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
                .configProperty(ExecConstants.SEPARATE_WORKSPACE, true);
        builder.configClientProperty(DrillProperties.USER, TEST_USER_1);
        builder.configClientProperty(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD);

        try (ClusterFixture cluster = builder.build();
             ClientFixture client = cluster.clientBuilder().build()) {
            Map<String, StoragePluginRegistry> userStorage = new HashMap<>();
            for (Map.Entry<UserServer.BitToUserConnection, UserServer.BitToUserConnectionConfig> userConnection : cluster.drillbit().getContext().getUserConnections()) {
                UserSession session = userConnection.getKey().getSession();
                userStorage.put(session.getCredentials().getUserName(), session.getStorage());
            }
            StoragePluginRegistry registry = userStorage.get(TEST_USER_1);
            // Bootstrap file loaded.
            assertNotNull(registry.getPlugin(CP_PLUGIN_NAME)); // Normal
            assertNotNull(registry.getPlugin(SYS_PLUGIN_NAME)); // System
            assertNull(registry.getStoredConfig(SYS_PLUGIN_NAME)); // Not editable

            assertNull(registry.getPlugin("bogus"));

            // Enabled plugins
            Map<String, StoragePluginConfig> configMap = registry.enabledConfigs();
            assertTrue(configMap.containsKey(CP_PLUGIN_NAME));
            assertFalse(configMap.containsKey(S3_PLUGIN_NAME)); // Disabled, but still appears
            assertFalse(configMap.containsKey(SYS_PLUGIN_NAME));

            assertNotNull(registry.getDefinedConfig(CP_PLUGIN_NAME));
            assertNull(registry.getDefinedConfig(S3_PLUGIN_NAME));
            assertNotNull(registry.getDefinedConfig(SYS_PLUGIN_NAME));

            // All stored plugins, including disabled
            configMap = registry.storedConfigs();
            assertTrue(configMap.containsKey(CP_PLUGIN_NAME));
            assertTrue(configMap.containsKey(S3_PLUGIN_NAME)); // Disabled, but still appears
            assertNotNull(configMap.get(S3_PLUGIN_NAME));
            assertSame(registry.getStoredConfig(S3_PLUGIN_NAME), configMap.get(S3_PLUGIN_NAME));
            assertFalse(configMap.containsKey(SYS_PLUGIN_NAME));
            int bootstrapCount = configMap.size();

            // Enabled only
            configMap = registry.storedConfigs(StoragePluginRegistry.PluginFilter.ENABLED);
            assertTrue(configMap.containsKey(CP_PLUGIN_NAME));
            assertFalse(configMap.containsKey(S3_PLUGIN_NAME));

            // Disabled only
            configMap = registry.storedConfigs(StoragePluginRegistry.PluginFilter.DISABLED);
            assertFalse(configMap.containsKey(CP_PLUGIN_NAME));
            assertTrue(configMap.containsKey(S3_PLUGIN_NAME));

            // Create a new plugin
            FileSystemConfig pConfig11 = myConfig1();
            registry.put(MY_PLUGIN_NAME, pConfig11);
            StoragePlugin plugin1 = registry.getPlugin(MY_PLUGIN_NAME);
            assertNotNull(plugin1);
            assertSame(plugin1, registry.getPluginByConfig(pConfig11));
            configMap = registry.storedConfigs();

            // Names converted to lowercase in persistent storage
            assertTrue(configMap.containsKey(MY_PLUGIN_KEY));
            assertEquals(bootstrapCount + 1, configMap.size());

            // Names are case-insensitive
            assertSame(plugin1, registry.getPlugin(MY_PLUGIN_KEY));
            assertSame(plugin1, registry.getPlugin(MY_PLUGIN_NAME.toUpperCase()));

            // Update the plugin
            FileSystemConfig pConfig2 = myConfig2();
            registry.put(MY_PLUGIN_NAME, pConfig2);
            StoragePlugin plugin2 = registry.getPlugin(MY_PLUGIN_NAME);
            assertNotSame(plugin1, plugin2);
            assertTrue(plugin2 instanceof FileSystemPlugin);
            FileSystemPlugin fsStorage = (FileSystemPlugin) plugin2;
            assertSame(pConfig2, fsStorage.getConfig());
            assertSame(plugin2, registry.getPluginByConfig(pConfig2));

            // Cannot create/update a plugin with null or blank name

            FileSystemConfig pConfig3 = myConfig1();
            try {
                registry.put(null, pConfig3);
                fail();
            } catch (StoragePluginRegistry.PluginException e) {
                // Expected
            }
            try {
                registry.put("  ", pConfig3);
                fail();
            } catch (StoragePluginRegistry.PluginException e) {
                // Expected
            }
        }
    }
}
