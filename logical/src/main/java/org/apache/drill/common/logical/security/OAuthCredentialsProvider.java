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

package org.apache.drill.common.logical.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OAuthCredentialsProvider implements CredentialsProvider {

  private final Map<String, String> tokens;

  @JsonCreator
  public OAuthCredentialsProvider(@JsonProperty("tokens") Map<String, String> tokens) {
    if (tokens == null) {
      this.tokens = new HashMap<>();
    } else {
      this.tokens = tokens;
    }
  }

  @Override
  public Map<String, String> getCredentials() {
    return tokens;
  }

  public void setAccessToken(String accessToken) {
    tokens.put("access_token", accessToken);
  }

  public void setRefreshToken(String refreshToken) {
    tokens.put("refresh_token", refreshToken);
  }

  public void setAuthorizationCode(String authCode) {
    tokens.put("authorizationCode", authCode);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OAuthCredentialsProvider that = (OAuthCredentialsProvider) o;
    return Objects.equals(tokens, that.tokens);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tokens);
  }
}
