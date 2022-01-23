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

import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_GROUP;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER_PASSWORD;
// import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;

public class TestPerUserAuthentication extends ClusterTest {

  private static final String DRIVER = "org.h2.Driver";
  private static String url;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = ClusterFixture.bareBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE)
      .configProperty(DrillProperties.USER, PROCESS_USER)
      .configProperty(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD)
      .build();

    ClientFixture admin = cluster.clientBuilder()
      .property(DrillProperties.USER, PROCESS_USER)
      .property(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD)
      .build();
    admin.alterSystem(ExecConstants.ADMIN_USERS_KEY, ADMIN_USER + "," + PROCESS_USER);
    admin.alterSystem(ExecConstants.ADMIN_USER_GROUPS_KEY, ADMIN_GROUP);

    client = cluster.clientBuilder()
      .property(DrillProperties.USER, ADMIN_USER)
      .property(DrillProperties.PASSWORD, ADMIN_USER_PASSWORD)
      .build();

    url = "jdbc:h2:" + dirTestWatcher.getTmpDir().getCanonicalPath();
  }


  @Test
  public void testWithPerUserCredentials() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);

    try(ClusterFixture cluster = builder.build()) {
      ClientFixture client = cluster
        .clientBuilder()
        .property(DrillProperties.USER, TEST_USER_2)
        .property(DrillProperties.PASSWORD, TEST_USER_2_PASSWORD)
        .build();

      Map<String, Object> sourceParameters = Collections.emptyMap();

      Map<String, String> creds = new HashMap<>();
      creds.put("username", "user1");
      creds.put("password", "pass1");
      PlainCredentialsProvider credentialsProvider = new PlainCredentialsProvider("testUser2", creds);

      JdbcStorageConfig config = new JdbcStorageConfig(
        DRIVER, url, null, null, false, false, sourceParameters, credentialsProvider, true, 1000);

      String sql = "SHOW DATABASES";
      RowSet results = client.queryBuilder().sql(sql).rowSet();
      results.print();
    }



    //JdbcStoragePlugin.initDataSource(config);
  }

}
