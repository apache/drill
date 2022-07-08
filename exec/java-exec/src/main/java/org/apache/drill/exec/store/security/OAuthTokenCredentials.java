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

public class OAuthTokenCredentials {

  public static final String CLIENT_ID = "clientID";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String ACCESS_TOKEN = "accessToken";
  public static final String REFRESH_TOKEN = "refreshToken";
  public static final String EXPIRES_IN = "expiresIn";

  private final String clientID;
  private final String clientSecret;
  private String accessToken;
  private String refreshToken;
  private String expiresIn;

  public OAuthTokenCredentials(CredentialsProvider credentialsProvider) {
    Map<String, String> credentials = credentialsProvider.getCredentials();
    this.clientID = credentials.get(CLIENT_ID);
    this.clientSecret = credentials.get(CLIENT_SECRET);

    try {
      this.accessToken = credentials.get(ACCESS_TOKEN);
    } catch (NullPointerException e) {
      this.accessToken = null;
    }

    try {
      this.refreshToken = credentials.get(REFRESH_TOKEN);
    } catch (NullPointerException e) {
      this.refreshToken = null;
    }

    try {
      this.expiresIn = credentials.get(EXPIRES_IN);
    } catch (NullPointerException e) {
      this.expiresIn = null;
    }
  }

  public String getClientID() {
    return clientID;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getRefreshToken() {
    return refreshToken;
  }

  public String getExpiresIn() {
    return expiresIn;
  }
}
