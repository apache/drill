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

import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UsernamePasswordCredentials {
  private static final Logger logger = LoggerFactory.getLogger(UsernamePasswordCredentials.class);
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  private final String username;
  private final String password;

  public UsernamePasswordCredentials(CredentialsProvider credentialsProvider) {
    if (credentialsProvider == null) {
      this.username = null;
      this.password = null;
    } else {
      Map<String, String> credentials = credentialsProvider.getCredentials() == null ? new HashMap<>() : credentialsProvider.getCredentials();
      this.username = credentials.get(USERNAME);
      this.password = credentials.get(PASSWORD);
    }
  }

  /**
   * This constructor is used for per-user credentials when the active user is known.
   * @param credentialsProvider The credentials provider.  Will only work for Plain and Vault credentials
   *                            provider.
   * @param activeUser The logged in userID
   */
  public UsernamePasswordCredentials(CredentialsProvider credentialsProvider, String activeUser) {
    logger.debug("Getting credentials for {}", activeUser);
    if (credentialsProvider == null) {
      this.username = null;
      this.password = null;
    } else {
      Map<String, String> credentials = credentialsProvider.getCredentials(activeUser);
      this.username = credentials.get(USERNAME);
      this.password = credentials.get(PASSWORD);
    }
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  @Override
  public int hashCode() {
    return Objects.hash(username, password);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UsernamePasswordCredentials that = (UsernamePasswordCredentials) o;
    return Objects.equals(username, that.password) &&
      Objects.equals(password, that.password);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("username", username)
      .maskedField("password", password)
      .toString();
  }
}
