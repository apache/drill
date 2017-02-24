/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.rpc.user.security;

import com.typesafe.config.ConfigValueFactory;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_1_PASSWORD;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2;
import static org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl.TEST_USER_2_PASSWORD;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestCustomUserAuthenticator extends BaseTestQuery {

  @BeforeClass
  public static void setupCluster() {
    // Create a new DrillConfig which has user authentication enabled and authenticator set to
    // UserAuthenticatorTestImpl.
    final Properties props = cloneDefaultTestConfigProperties();
    final DrillConfig newConfig = new DrillConfig(DrillConfig.create(props)
            .withValue(ExecConstants.USER_AUTHENTICATION_ENABLED,
                ConfigValueFactory.fromAnyRef("true"))
            .withValue(ExecConstants.USER_AUTHENTICATOR_IMPL,
                ConfigValueFactory.fromAnyRef(UserAuthenticatorTestImpl.TYPE)),
        false);

    final Properties connectionProps = new Properties();
    connectionProps.setProperty(DrillProperties.USER, "anonymous");
    connectionProps.setProperty(DrillProperties.PASSWORD, "anything works!");
    updateTestCluster(3, newConfig, connectionProps);
  }

  @Test
  public void positiveUserAuth() throws Exception {
    runTest(TEST_USER_1, TEST_USER_1_PASSWORD);
    runTest(TEST_USER_2, TEST_USER_2_PASSWORD);
  }

  @Test
  public void negativeUserAuth() throws Exception {
    negativeAuthHelper(TEST_USER_1, "blah.. blah..");
    negativeAuthHelper(TEST_USER_2, "blah.. blah..");
    negativeAuthHelper("invalidUserName", "blah.. blah..");
  }

  @Test
  public void emptyPassword() throws Exception {
    try {
      runTest(TEST_USER_2, "");
      fail("Expected an exception.");
    } catch (RpcException e) {
      final String exMsg = e.getMessage();
      assertThat(exMsg, containsString("Insufficient credentials"));
    } catch (Exception e) {
      fail("Expected an RpcException.");
    }
  }

  @Test
  public void positiveUserAuthAfterNegativeUserAuth() throws Exception {
    negativeAuthHelper("blah.. blah..", "blah.. blah..");
    runTest(TEST_USER_2, TEST_USER_2_PASSWORD);
  }

  private static void negativeAuthHelper(final String user, final String password) throws Exception {
    RpcException negativeAuthEx = null;
    try {
      runTest(user, password);
    } catch (RpcException e) {
      negativeAuthEx = e;
    }

    assertNotNull("Expected RpcException.", negativeAuthEx);
    final String exMsg = negativeAuthEx.getMessage();
    assertThat(exMsg, containsString("Authentication failed"));
    assertThat(exMsg, containsString("Incorrect credentials"));
  }

  private static void runTest(final String user, final String password) throws Exception {
    final Properties connectionProps = new Properties();

    connectionProps.setProperty(DrillProperties.USER, user);
    connectionProps.setProperty(DrillProperties.PASSWORD, password);

    updateClient(connectionProps);

    // Run few queries using the new client
    test("SHOW SCHEMAS");
    test("USE INFORMATION_SCHEMA");
    test("SHOW TABLES");
    test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE 'COLUMNS'");
    test("SELECT * FROM cp.`region.json` LIMIT 5");
  }
}
