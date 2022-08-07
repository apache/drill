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

package org.apache.drill.exec.store.splunk;

import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

@Category({SlowTest.class})
public class TestSplunkUserTranslation extends SplunkBaseTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);
    startCluster(builder);
  }
  
  @Test
  public void testEmptyUserCredentials() throws Exception {
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();

    // First verify that the user has no credentials
    StoragePluginRegistry registry = cluster.storageRegistry();
    StoragePlugin plugin = registry.getPlugin("ut_splunk");
    PlainCredentialsProvider credentialsProvider = (PlainCredentialsProvider) plugin.getConfig().getCredentialsProvider();
    Map<String, String> credentials = credentialsProvider.getUserCredentials(ADMIN_USER);
    assertNotNull(credentials);
    assertNull(credentials.get("username"));
    assertNull(credentials.get("password"));
  }

  @Test
  public void testQueryWithMissingCredentials() throws Exception {
    // This test validates that the correct credentials are sent down to Splunk.
    // The query should fail, but Drill should not crash
    ClientFixture client = cluster
      .clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();

    String sql = "SELECT `component` FROM splunk.`_introspection` ORDER BY `component` LIMIT 2";
    try {
      client.queryBuilder().sql(sql).run();
      fail();
    } catch (UserException e) {
      assertTrue(e.getMessage().contains("You do not have valid credentials for this API."));
    }
  }


}
