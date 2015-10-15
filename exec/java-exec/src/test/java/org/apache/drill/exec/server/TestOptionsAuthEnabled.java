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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.junit.BeforeClass;
import org.junit.Test;

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
    final Properties props = cloneDefaultTestConfigProperties();
    props.setProperty(ExecConstants.USER_AUTHENTICATION_ENABLED, "true");
    props.setProperty(ExecConstants.USER_AUTHENTICATOR_IMPL, UserAuthenticatorTestImpl.TYPE);

    updateTestCluster(1, DrillConfig.create(props));

    updateClient(PROCESS_USER, PROCESS_USER_PASSWORD);

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
    errorMsgTestHelper(setOptionQuery, "Admin related settings can only be set at SYSTEM level scope");
  }

  @Test
  public void trySettingAdminOptsAtSessionScopeAsNonAdmin() throws Exception {
    updateClient(TEST_USER_2, TEST_USER_2_PASSWORD);
    final String setOptionQuery =
        String.format("ALTER SESSION SET `%s`='%s,%s'", ExecConstants.ADMIN_USERS_KEY, ADMIN_USER, PROCESS_USER);
    errorMsgTestHelper(setOptionQuery, "Admin related settings can only be set at SYSTEM level scope");
  }

  private void setOptHelper() throws Exception {
    try {
      test(setSysOptionQuery);
      testBuilder()
          .sqlQuery(String.format("SELECT num_val FROM sys.options WHERE name = '%s' AND type = 'SYSTEM'",
              ExecConstants.SLICE_TARGET))
          .unOrdered()
          .baselineColumns("num_val")
          .baselineValues(200L)
          .go();
    } finally {
      test(String.format("ALTER SYSTEM SET `%s` = %d;", ExecConstants.SLICE_TARGET, ExecConstants.SLICE_TARGET_DEFAULT));
    }
  }
}
