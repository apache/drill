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
package org.apache.drill.exec.impersonation;

import org.apache.drill.categories.SecurityTest;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.dotdrill.DotDrillType;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.UserExceptionMatcher;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TYPE;

@Category({SlowTest.class, SecurityTest.class})
public class TestInboundImpersonation extends BaseTestImpersonation {

  public static final String OWNER = org1Users[0];
  public static final String OWNER_PASSWORD = "owner";

  public static final String TARGET_NAME = org1Users[1];
  public static final String TARGET_PASSWORD = "target";

  public static final String DATA_GROUP = org1Groups[0];

  public static final String PROXY_NAME = org1Users[2];
  public static final String PROXY_PASSWORD = "proxy";

  // Because cluster fixture save client properties even client closed,
  // Use a dedicated cluster fixture for each test case so that the client fixture has clean properties.
  @Test
  public void selectChainedView() throws Exception {
    startMiniDfsCluster(TestInboundImpersonation.class.getSimpleName());
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configClientProperty(DrillProperties.USER, PROXY_NAME)
      .configClientProperty(DrillProperties.PASSWORD, PROXY_PASSWORD);

    startCluster(builder);
    addMiniDfsBasedStorage(createTestWorkspaces());
    createTestData();
    // Authorize PROXY_NAME to impersonate TARGET_NAME
    try (ClientFixture adminClient = cluster.client(PROCESS_USER, PROCESS_USER_PASSWORD)) {
      adminClient.alterSystem(
          ExecConstants.IMPERSONATION_POLICIES_KEY,
          "[ { proxy_principals : { users: [\"" + PROXY_NAME + "\" ] },"
              + "target_principals : { users : [\"" + TARGET_NAME + "\"] } } ]"
      );

      // Connect as PROXY_NAME and query for IMPERSONATION_TARGET
      // data belongs to OWNER, however a view is shared with IMPERSONATION_TARGET
      ClientFixture.ClientBuilder clientBuilder = cluster.clientBuilder()
          .property(DrillProperties.USER, PROXY_NAME)
          .property(DrillProperties.PASSWORD, PROXY_PASSWORD)
          .property(DrillProperties.IMPERSONATION_TARGET, TARGET_NAME);
      try (ClientFixture client = clientBuilder.build()) {
        client.testBuilder()
            .sqlQuery("SELECT * FROM %s.u0_lineitem ORDER BY l_orderkey LIMIT 1", getWSSchema(OWNER))
            .ordered()
            .baselineColumns("l_orderkey", "l_partkey")
            .baselineValues(1, 1552)
            .go();
      }
      adminClient.resetSystem(ExecConstants.IMPERSONATION_POLICIES_KEY);
    }
  }

  @Test(expected = IllegalStateException.class)
  // PERMISSION ERROR: Proxy user 'user2_1' is not authorized to impersonate target user 'user0_2'.
  public void unauthorizedTarget() throws Exception {
    final String unauthorizedTarget = org2Users[0];
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configClientProperty(DrillProperties.USER, PROXY_NAME)
      .configClientProperty(DrillProperties.PASSWORD, PROXY_PASSWORD)
      .configClientProperty(DrillProperties.IMPERSONATION_TARGET, unauthorizedTarget);
    startCluster(builder);
  }

  @Test
  public void invalidPolicy() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configClientProperty(DrillProperties.USER, PROCESS_USER)
      .configClientProperty(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD);
    startCluster(builder);

    UserException userException = Assert.assertThrows(
      UserException.class,
      () -> run("ALTER SYSTEM SET `%s`='%s'", ExecConstants.IMPERSONATION_POLICIES_KEY,
        "[ invalid json ]"));
    MatcherAssert.assertThat(userException,
      new UserExceptionMatcher(UserBitShared.DrillPBError.ErrorType.VALIDATION,
        "Invalid impersonation policies."));
  }

  @Test
  public void invalidProxy() throws Exception {
    ClusterFixtureBuilder builder = new ClusterFixtureBuilder(dirTestWatcher)
      .configProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, true)
      .configProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, TYPE)
      .configProperty(ExecConstants.IMPERSONATION_ENABLED, true)
      .configClientProperty(DrillProperties.USER, PROCESS_USER)
      .configClientProperty(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD);
    startCluster(builder);

    UserException userException = Assert.assertThrows(UserException.class,
      () -> run("ALTER SYSTEM SET `%s`='%s'", ExecConstants.IMPERSONATION_POLICIES_KEY,
        "[ { proxy_principals : { users: [\"*\" ] }," + "target_principals : { users : [\"" + TARGET_NAME + "\"] } } ]"));
    MatcherAssert.assertThat(userException,
      new UserExceptionMatcher(UserBitShared.DrillPBError.ErrorType.VALIDATION,
        "Proxy principals cannot have a wildcard entry."));
  }

  private static Map<String, WorkspaceConfig> createTestWorkspaces() throws Exception {
    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap();
    createAndAddWorkspace(OWNER, getUserHome(OWNER), (short) 0755, OWNER, DATA_GROUP, workspaces);
    createAndAddWorkspace(PROXY_NAME, getUserHome(PROXY_NAME), (short) 0755, PROXY_NAME, DATA_GROUP,
        workspaces);
    return workspaces;
  }

  private static void createTestData() throws Exception {
    // Create table accessible only by OWNER
    final String tableName = "lineitem";
    try (ClientFixture client = cluster.client(OWNER, OWNER_PASSWORD)) {
      client.run("USE " + getWSSchema(OWNER));
      client.run("CREATE TABLE %s as SELECT * FROM cp.`tpch/%s.parquet`", tableName, tableName);

      // Change the ownership and permissions manually.
      // Currently there is no option to specify the default permissions and ownership for new tables.
      final Path tablePath = new Path(getUserHome(OWNER), tableName);
      fs.setOwner(tablePath, OWNER, DATA_GROUP);
      fs.setPermission(tablePath, new FsPermission((short) 0700));

      // Create a view on top of lineitem table; allow IMPERSONATION_TARGET to read the view
      // /user/user0_1    u0_lineitem    750    user0_1:group0_1
      final String viewName = "u0_lineitem";
      client.alterSession(ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY, "0750");
      client.run("CREATE VIEW %s.%s AS SELECT l_orderkey, l_partkey FROM %s.%s",
          getWSSchema(OWNER), viewName, getWSSchema(OWNER), "lineitem");
      // Verify the view file created has the expected permissions and ownership
      final Path viewFilePath = new Path(getUserHome(OWNER), viewName + DotDrillType.VIEW.getEnding());
      final FileStatus status = fs.getFileStatus(viewFilePath);
      assertEquals(org1Groups[0], status.getGroup());
      assertEquals(OWNER, status.getOwner());
      assertEquals(0750, status.getPermission().toShort());
    }
  }
}
