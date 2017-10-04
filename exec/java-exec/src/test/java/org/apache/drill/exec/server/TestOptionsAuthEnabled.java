/**
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
package org.apache.drill.exec.server;

import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.FixtureBuilder;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_GROUP;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.ADMIN_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.PROCESS_USER_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;




import java.util.Properties;

/**
 * Test setting system scoped options with user authentication enabled. (DRILL-3622)
 */
public class TestOptionsAuthEnabled extends BaseTestQuery {
  private static final String setSysOptionQuery =
      String.format("ALTER SYSTEM SET `%s` = %d;", ExecConstants.SLICE_TARGET, 200);

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Create a new DrillConfig which has user authentication enabled and test authenticator set
    final DrillConfig config = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
        .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
            ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
        false);

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.USER, PROCESS_USER);
    connectionProps.setProperty(DrillProperties.PASSWORD, PROCESS_USER_PASSWORD);

    updateTestCluster(1, config, connectionProps);

    // Add user "admin" to admin username list
    test(String.format("ALTER SYSTEM SET `%s`='%s,%s'", ExecConstants.ADMIN_USERS_KEY, ADMIN_USER, PROCESS_USER));

    // Set "admingrp" to admin username list
    test(String.format("ALTER SYSTEM SET `%s`='%s'", ExecConstants.ADMIN_USER_GROUPS_KEY, ADMIN_GROUP));
  }

  @Test
  public void updateSysOptAsAdminUser() throws Exception {
    updateClient(ADMIN_USER, ADMIN_USER_PASSWORD);
    setOptHelper();
  }

  @Test
  public void updateSysOptAsNonAdminUser() throws Exception {
    updateClient(TEST_USER_2, TEST_USER_2_PASSWORD);
    errorMsgTestHelper(setSysOptionQuery, "Not authorized to change SYSTEM options.");
  }

  @Test
  public void updateSysOptAsUserInAdminGroup() throws Exception {
    updateClient(TEST_USER_1, TEST_USER_1_PASSWORD);
    setOptHelper();
  }

  @Test
  public void trySettingAdminOptsAtSessionScopeAsAdmin() throws Exception {
    updateClient(ADMIN_USER, ADMIN_USER_PASSWORD);
    final String setOptionQuery =
        String.format("ALTER SESSION SET `%s`='%s,%s'", ExecConstants.ADMIN_USERS_KEY, ADMIN_USER, PROCESS_USER);
    errorMsgTestHelper(setOptionQuery, "PERMISSION ERROR: Cannot change option security.admin.users in scope SESSION");
  }

  @Test
  public void trySettingAdminOptsAtSessionScopeAsNonAdmin() throws Exception {
    updateClient(TEST_USER_2, TEST_USER_2_PASSWORD);
    final String setOptionQuery =
        String.format("ALTER SESSION SET `%s`='%s,%s'", ExecConstants.ADMIN_USERS_KEY, ADMIN_USER, PROCESS_USER);
    errorMsgTestHelper(setOptionQuery, "PERMISSION ERROR: Cannot change option security.admin.users in scope SESSION");
  }

  private void setOptHelper() throws Exception {
    try {
      test(setSysOptionQuery);
      testBuilder()
          .sqlQuery(String.format("SELECT num_val FROM sys.options WHERE name = '%s' AND optionScope = 'SYSTEM'",
              ExecConstants.SLICE_TARGET))
          .unOrdered()
          .baselineColumns("num_val")
          .baselineValues(200L)
          .go();
    } finally {
      test(String.format("ALTER SYSTEM SET `%s` = %d;", ExecConstants.SLICE_TARGET, ExecConstants.SLICE_TARGET_DEFAULT));
    }
  }

  @Test
  public void testAdminUserOptions() throws Exception {
    FixtureBuilder builder = ClusterFixture.builder();

    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      OptionManager optionManager = cluster.drillbit().getContext().getOptionManager();

      // Admin Users Tests
      // config file should have the 'fake' default admin user and it should be returned
      // by the option manager if the option has not been set by the user
      String configAdminUser =  optionManager.getOption(ExecConstants.ADMIN_USERS_VALIDATOR);;
      assertEquals(configAdminUser, ExecConstants.ADMIN_USERS_VALIDATOR.DEFAULT_ADMIN_USERS);

      // Option accessor should never return the 'fake' default from the config
      String adminUser1 = ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(optionManager);
      assertNotEquals(adminUser1, ExecConstants.ADMIN_USERS_VALIDATOR.DEFAULT_ADMIN_USERS);

      // Change TEST_ADMIN_USER if necessary
      String TEST_ADMIN_USER = "ronswanson";
      if (adminUser1.equals(TEST_ADMIN_USER)) {
        TEST_ADMIN_USER += "thefirst";
      }
      // Check if the admin option accessor honors a user-supplied values
      String sql = String.format("ALTER SYSTEM SET `%s`='%s'", ExecConstants.ADMIN_USERS_KEY, TEST_ADMIN_USER);
      client.queryBuilder().sql(sql).run();
      String adminUser2 = ExecConstants.ADMIN_USERS_VALIDATOR.getAdminUsers(optionManager);
      assertEquals(adminUser2, TEST_ADMIN_USER);

      // Admin User Groups Tests

      // config file should have the 'fake' default admin user and it should be returned
      // by the option manager if the option has not been set by the user
      String configAdminUserGroups =  optionManager.getOption(ExecConstants.ADMIN_USER_GROUPS_VALIDATOR);
      assertEquals(configAdminUserGroups, ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.DEFAULT_ADMIN_USER_GROUPS);

      // Option accessor should never return the 'fake' default from the config
      String adminUserGroups1 = ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(optionManager);
      assertNotEquals(adminUserGroups1, ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.DEFAULT_ADMIN_USER_GROUPS);

      // Change TEST_ADMIN_USER_GROUPS if necessary
      String TEST_ADMIN_USER_GROUPS = "yakshavers";
      if (adminUserGroups1.equals(TEST_ADMIN_USER_GROUPS)) {
        TEST_ADMIN_USER_GROUPS += ",wormracers";
      }
      // Check if the admin option accessor honors a user-supplied values
      sql = String.format("ALTER SYSTEM SET `%s`='%s'", ExecConstants.ADMIN_USER_GROUPS_KEY, TEST_ADMIN_USER_GROUPS);
      client.queryBuilder().sql(sql).run();
      String adminUserGroups2 = ExecConstants.ADMIN_USER_GROUPS_VALIDATOR.getAdminUserGroups(optionManager);
      assertEquals(adminUserGroups2, TEST_ADMIN_USER_GROUPS);
    }
  }


}
