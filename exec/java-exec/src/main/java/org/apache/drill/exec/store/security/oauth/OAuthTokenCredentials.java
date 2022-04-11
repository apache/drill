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

package org.apache.drill.exec.store.security.oauth;

import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;

import java.util.Map;
import java.util.Optional;

public class OAuthTokenCredentials extends UsernamePasswordCredentials {

  public static final String CLIENT_ID = "clientID";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String ACCESS_TOKEN = "accessToken";
  public static final String REFRESH_TOKEN = "refreshToken";
  public static final String TOKEN_URI = "tokenURI";
  public static final String PROXY_USERNAME = "proxyUsername";
  public static final String PROXY_PASSWORD = "proxyPassword";

  private final String clientID;
  private final String clientSecret;
  private final String tokenURI;
  private Optional<PersistentTokenTable> tokenTable;

  /**
   * While a builder may seem like overkill for a class that is little more than small struct,
   * it allows us to wrap new instances in an Optional while using contructors does not.
   */
  public static class Builder {
    private CredentialsProvider credentialsProvider;
    private String queryUser;
    private PersistentTokenTable tokenTable;

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public Builder setQueryUser(String queryUser) {
      this.queryUser = queryUser;
      return this;
    }

    public Builder setTokenTable(PersistentTokenTable tokenTable) {
      this.tokenTable = tokenTable;
      return this;
    }

    public Optional<OAuthTokenCredentials> build() {
      if (credentialsProvider == null) {
        return Optional.empty();
      }

      Map<String, String> credentials = queryUser != null
        ? credentialsProvider.getCredentials(queryUser)
        : credentialsProvider.getCredentials();

      if (credentials.size() == 0) {
        return Optional.empty();
      }

      return Optional.of(
        new OAuthTokenCredentials(
          credentials.get(USERNAME),
          credentials.get(PASSWORD),
          credentials.get(CLIENT_ID),
          credentials.get(CLIENT_SECRET),
          credentials.get(TOKEN_URI),
          tokenTable
        )
      );
    }
  }

  public OAuthTokenCredentials(
    String username,
    String password,
    String clientID,
    String clientSecret,
    String tokenURI,
    PersistentTokenTable tokenTable
  ) {
    super(username, password);
    this.clientID = clientID;
    this.clientSecret = clientSecret;
    this.tokenURI = tokenURI;
    this.tokenTable = Optional.ofNullable(tokenTable);
  }

  public String getClientID() {
    return clientID;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getAccessToken() {
    return tokenTable.map(PersistentTokenTable::getAccessToken).orElse(null);
  }

  public String getRefreshToken() {
    return tokenTable.map(PersistentTokenTable::getRefreshToken).orElse(null);
  }

  public String getTokenUri() {
    return tokenURI;
  }
}
