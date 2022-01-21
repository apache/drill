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

package org.apache.drill.exec.store.security;

import org.apache.drill.common.logical.security.CredentialsProvider;

import java.util.Map;

public class PerUserUsernamePasswordCredentials extends UsernamePasswordCredentials {
  public static final String DEFAULT_USERNAME = "default_username";
  public static final String DEFAULT_PASSWORD = "default_password";

  private final String defaultUsername;
  private final String defaultPassword;
  private final String activeUser;

  public PerUserUsernamePasswordCredentials(CredentialsProvider credentialsProvider, String activeUser) {
    super(credentialsProvider, activeUser);
    this.activeUser = activeUser;
    Map<String, String> credentials = credentialsProvider.getCredentials();

    this.defaultUsername = credentials.get(DEFAULT_USERNAME);
    this.defaultPassword = credentials.get(DEFAULT_PASSWORD);
  }

  public String getDefaultUsername() {
    return defaultUsername;
  }

  public String getDefaultPassword() {
    return defaultPassword;
  }

}
