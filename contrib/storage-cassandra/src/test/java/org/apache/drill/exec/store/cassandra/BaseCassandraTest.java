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
package org.apache.drill.exec.store.cassandra;

import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.CassandraContainer;

import java.util.HashMap;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;

public class BaseCassandraTest extends ClusterTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestCassandraSuite.initCassandra();
    initCassandraPlugin(TestCassandraSuite.cassandra);
  }

  private static void initCassandraPlugin(CassandraContainer<?> cassandra) throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
        .configProperty(ExecConstants.HTTP_ENABLE, true)
        .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
        .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);
    startCluster(builder);

    CassandraStorageConfig config = new CassandraStorageConfig(
        cassandra.getHost(),
        cassandra.getMappedPort(CassandraContainer.CQL_PORT),
        cassandra.getUsername(),
        cassandra.getPassword(),
        AuthMode.SHARED_USER.name(),
        null);
    config.setEnabled(true);
    cluster.defineStoragePlugin("cassandra", config);

    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(new HashMap<>());
    // Add authorized user
    credentialsProvider.setUserCredentials(cassandra.getUsername(), cassandra.getPassword(), TEST_USER_1);
    // Add unauthorized user
    credentialsProvider.setUserCredentials("nope", "no way dude", TEST_USER_2);

    CassandraStorageConfig ut_config = new CassandraStorageConfig(
        cassandra.getHost(),
        cassandra.getMappedPort(CassandraContainer.CQL_PORT),
        null, null,
        AuthMode.USER_TRANSLATION.name(),
        credentialsProvider);
    ut_config.setEnabled(true);
    cluster.defineStoragePlugin("ut_cassandra", ut_config);
  }

  @AfterClass
  public static void tearDownCassandra() {
    if (TestCassandraSuite.isRunningSuite()) {
      TestCassandraSuite.tearDownCluster();
    }
  }
}
