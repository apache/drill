/**
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
package org.apache.drill.exec.impersonation;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.user.InboundImpersonationManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

public class TestInboundImpersonationPrivileges extends BaseTestImpersonation {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestInboundImpersonationPrivileges.class);

  // policies on which the tests are based
  private static final String IMPERSONATION_POLICIES;

  static {
    try {
      IMPERSONATION_POLICIES = Files.toString(FileUtils.getResourceAsFile("/inbound_impersonation_policies.json"),
          Charsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException("Cannot load impersonation policies.", e);
    }
  }

  private static boolean checkPrivileges(final String proxyName, final String targetName) {
    ExecConstants.IMPERSONATION_POLICY_VALIDATOR.validate(
        OptionValue.createString(OptionValue.OptionType.SYSTEM,
            ExecConstants.IMPERSONATION_POLICIES_KEY,
            IMPERSONATION_POLICIES), null);
    try {
      return InboundImpersonationManager.hasImpersonationPrivileges(proxyName, targetName, IMPERSONATION_POLICIES);
    } catch (final Exception e) {
      logger.error("Failed to check impersonation privileges.", e);
      return false;
    }
  }

  private static void run(final String proxyName, final String targetName, final boolean expected) {
    assertEquals("proxyName: " + proxyName + " targetName: " + targetName,
        expected, checkPrivileges(proxyName, targetName));
  }

  @Test
  public void allTargetUsers() {
    for (final String user : org1Users) {
      run("user0_1", user, true);
    }
    for (final String user : org2Users) {
      run("user0_1", user, true);
    }
  }

  @Test
  public void noTargetUsers() {
    for (final String user : org1Users) {
      run("user1_1", user, false);
    }
    for (final String user : org2Users) {
      run("user1_1", user, false);
    }
  }

  @Test
  public void someTargetUsersAndGroups() {
    run("user2_1", "user3_1", true);
    run("user2_1", "user3_1", true);
    run("user2_1", "user1_1", false);
    run("user2_1", "user4_1", false);
    for (final String user : org1Users) {
      if (!user.equals("user3_1") && !user.equals("user2_1")) {
        run("user2_1", user, false);
      }
    }
    for (final String user : org2Users) {
      run("user2_1", user, false);
    }
  }

  @Test
  public void someTargetUsers() {
    run("user4_1", "user1_1", true);
    run("user4_1", "user3_1", true);
    for (final String user : org1Users) {
      if (!user.equals("user1_1") && !user.equals("user3_1")) {
        run("user4_1", user, false);
      }
    }
    for (final String user : org2Users) {
      run("user4_1", user, false);
    }
  }

  @Test
  public void oneTargetGroup() {
    run("user5_1", "user4_2", true);
    run("user5_1", "user5_2", true);
    run("user5_1", "user4_1", false);
    run("user5_1", "user3_2", false);
  }

  @Test
  public void twoTargetUsers() {
    run("user5_2", "user0_2", true);
    run("user5_2", "user1_2", true);
    run("user5_2", "user2_2", false);
    run("user5_2", "user0_1", false);
    run("user5_2", "user1_1", false);
  }

  @Test
  public void twoTargetGroups() {
    run("user3_2", "user4_2", true);
    run("user3_2", "user1_2", true);
    run("user3_2", "user2_2", true);
    run("user3_2", "user0_2", false);
    run("user3_2", "user5_2", false);
    for (final String user : org1Users) {
      run("user3_2", user, false);
    }
  }
}
