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
package org.apache.drill.exec.rpc.user.security;

import com.google.common.base.Strings;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;

import java.io.IOException;

/*
 * Implement {@link org.apache.drill.exec.rpc.user.security.UserAuthenticator} for testing UserAuthenticator and
 * authentication of users from Java client to Drillbit.
 */
@UserAuthenticatorTemplate(type = UserAuthenticatorTestImpl.TYPE)
public class UserAuthenticatorTestImpl implements UserAuthenticator {
  public static final String TYPE = "drillTestAuthenticator";

  public static final String TEST_USER_1 = "testUser1";
  public static final String TEST_USER_2 = "testUser2";
  public static final String TEST_USER_1_PASSWORD = "testUser1Password";
  public static final String TEST_USER_2_PASSWORD = "testUser2Password";

  @Override
  public void setup(DrillConfig drillConfig) throws DrillbitStartupException {
    // Nothing to setup.
  }

  @Override
  public void authenticate(String user, String password) throws UserAuthenticationException {

    if ("anonymous".equals(user)) {
      // Allow user "anonymous" for test framework to work.
      return;
    }

    if (!(TEST_USER_1.equals(user) && TEST_USER_1_PASSWORD.equals(password)) &&
        !(TEST_USER_2.equals(user) && TEST_USER_2_PASSWORD.equals(password))) {
      throw new UserAuthenticationException();
    }
  }

  @Override
  public void close() throws IOException {
    // Nothing to cleanup.
  }
}
