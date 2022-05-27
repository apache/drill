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
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;

import java.util.Map;
import java.util.Optional;

public class UsernamePasswordWithProxyCredentials extends UsernamePasswordCredentials {
  private final String proxyUsername;
  private final String proxyPassword;

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

    public Optional<UsernamePasswordWithProxyCredentials> build() {
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
        new UsernamePasswordWithProxyCredentials(
          credentials.get(USERNAME),
          credentials.get(PASSWORD),
          credentials.get(OAuthTokenCredentials.PROXY_USERNAME),
          credentials.get(OAuthTokenCredentials.PROXY_PASSWORD)
        )
      );
    }
  }

  public UsernamePasswordWithProxyCredentials(
    String username,
    String password,
    String proxyUsername,
    String proxyPassword
  ) {
    super(username, password);
    this.proxyUsername = proxyUsername;
    this.proxyPassword = proxyPassword;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }
}
