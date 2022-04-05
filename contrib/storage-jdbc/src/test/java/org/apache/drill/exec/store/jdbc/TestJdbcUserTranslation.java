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

package org.apache.drill.exec.store.jdbc;

import org.apache.drill.categories.JdbcStorageTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.junit.Assert.assertTrue;

@Category(JdbcStorageTest.class)
public class TestJdbcUserTranslation extends ClusterTest {

  @BeforeClass
  public static void initMysql() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.HTTP_ENABLE, true)
      .configProperty(ExecConstants.HTTP_PORT_HUNT, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true);

    startCluster(builder);

    String jdbcUrl = "";
    PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider(new HashMap<>());

    JdbcStorageConfig jdbcStorageConfig = new JdbcStorageConfig("com.mysql.cj.jdbc.Driver", jdbcUrl,
      null, null, false, false, null, credentialsProvider, "user_translation", 10000);
    jdbcStorageConfig.setEnabled(true);

    cluster.defineStoragePlugin("mysql", jdbcStorageConfig);
  }

  @Test
  public void testShowDatabasesWithUserWithNoCreds() throws Exception {
    ClientFixture client = cluster.clientBuilder()
      .property(DrillProperties.USER, TEST_USER_1)
      .property(DrillProperties.PASSWORD, TEST_USER_1_PASSWORD)
      .build();

    String sql = "SHOW DATABASES";
    QuerySummary results = client.queryBuilder().sql(sql).run();
    assertTrue(results.succeeded());
  }
}
