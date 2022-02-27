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

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


/**
 * Tests the UserSession getting down to the storage plugins.  Makes sure
 * the correct UserSession is going to the correct plugin.
 */
public class TestUserSessionInStorage extends ClusterTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @After
  public void cleanup() throws Exception {
    FileUtils.cleanDirectory(dirTestWatcher.getStoreDir());
  }

  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testAnonymousUserSession() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    ClusterFixture cluster = builder.build();

    StoragePluginRegistry registry = cluster.storageRegistry();

    // Run a query
    String sql = "SHOW FILES IN dfs";
    client.queryBuilder().sql(sql).run();

    AbstractStoragePlugin plugin = (AbstractStoragePlugin) registry.getPlugin("dfs");

    // Get the active user.  If there is no authentication, the UserSession may be null.  The
    // getActiveUser() method is always safe to use because it will return anonymous if the
    // UserSession is null.
    assertEquals("anonymous", plugin.getActiveUser());
  }

  @Test
  public void testWithUser() throws Exception {
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    StoragePluginRegistry registry = cluster.storageRegistry();
    // Run a query
    String sql = "SHOW FILES IN dfs";
    client.queryBuilder().sql(sql).run();

    AbstractStoragePlugin plugin = (AbstractStoragePlugin) registry.getPlugin("dfs");

    // Get the user session.  This should not be null.
    assertNotNull(plugin.getSession());
    assertEquals(TEST_USER_1, plugin.getActiveUser());
  }

  @Test
  public void testUserUpdate() throws Exception {
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    StoragePluginRegistry registry = cluster.storageRegistry();
    // Run a query
    String sql = "SHOW FILES IN dfs";
    client.queryBuilder().sql(sql).run();

    AbstractStoragePlugin plugin = (AbstractStoragePlugin) registry.getPlugin("dfs");

    // Get the user session.  This should not be null.
    assertNotNull(plugin.getSession());
    assertEquals(TEST_USER_1, plugin.getActiveUser());

    // Run another query with a different user.
    client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_2)
      .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
      .build();

    client.queryBuilder().sql(sql).run();
    assertEquals(TEST_USER_2, plugin.getActiveUser());
  }
}
