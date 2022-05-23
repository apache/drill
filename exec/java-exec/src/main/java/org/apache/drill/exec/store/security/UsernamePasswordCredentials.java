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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class UsernamePasswordCredentials {
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  private final String username;
  private final String password;

  /**
   * While a builder may seem like overkill for a class that is little more than small struct,
   * it allows us to wrap new instances in an Optional while using contructors does not.
   */
  public static class Builder {
    private CredentialsProvider credentialsProvider;
    private String queryUser;

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder setQueryUser(String queryUser) {
      this.queryUser = queryUser;
      return this;
    }

    public Optional<UsernamePasswordCredentials> build() {
      if (credentialsProvider == null) {
        return Optional.empty();
      }

      Map<String, String> credentials = queryUser != null
        ? credentialsProvider.getUserCredentials(queryUser)
        : credentialsProvider.getCredentials();

      if (credentials.size() == 0) {
        return Optional.empty();
      }

      return Optional.of(
        new UsernamePasswordCredentials(credentials.get(USERNAME), credentials.get(PASSWORD))
      );
    }
  }

  public UsernamePasswordCredentials(String username, String password) {
    this.username = username;
    this.password = password;
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
