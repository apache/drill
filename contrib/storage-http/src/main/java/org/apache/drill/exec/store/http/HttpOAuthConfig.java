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

package org.apache.drill.exec.store.http;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.store.security.OAuthTokenCredentials;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Builder
@Getter
@Setter
@Accessors(fluent = true)
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpOAuthConfig.HttpOAuthConfigBuilder.class)
public class HttpOAuthConfig {

  @JsonProperty("baseURL")
  private final String baseURL;

  @JsonProperty("clientID")
  private final String clientID;

  @JsonProperty("clientSecret")
  private final String clientSecret;

  @JsonProperty("callbackURL")
  private final String callbackURL;

  @JsonProperty("authorizationURL")
  private final String authorizationURL;

  @JsonProperty("authorizationPath")
  private final String authorizationPath;

  @JsonProperty("authorizationParams")
  private final Map<String, String> authorizationParams;

  @JsonProperty("accessTokenPath")
  private final String accessTokenPath;

  @JsonProperty("generateCSRFToken")
  private final boolean generateCSRFToken;

  @JsonProperty("scope")
  private final String scope;

  @JsonProperty("tokens")
  private final Map<String, String> tokens;

  /**
   * Clone constructor used for updating tokens
   * @param that The original oAuth configs
   * @param tokens The updated tokens
   */
  public HttpOAuthConfig(HttpOAuthConfig that, Map<String, String> tokens) {
    this.baseURL = that.baseURL;
    this.clientID = that.clientID;
    this.clientSecret = that.clientSecret;
    this.callbackURL = that.callbackURL;
    this.authorizationURL = that.authorizationURL;
    this.authorizationPath = that.authorizationPath;
    this.authorizationParams = that.authorizationParams;
    this.accessTokenPath = that.accessTokenPath;
    this.generateCSRFToken = that.generateCSRFToken;
    this.scope = that.scope;
    this.tokens = tokens == null ? new HashMap<>() : tokens;
  }

  private HttpOAuthConfig(HttpOAuthConfig.HttpOAuthConfigBuilder builder) {
    this.baseURL = builder.baseURL;
    this.clientID = builder.clientID;
    this.clientSecret = builder.clientSecret;
    this.callbackURL = builder.callbackURL;
    this.authorizationURL = builder.authorizationURL;
    this.authorizationPath = builder.authorizationPath;
    this.authorizationParams = builder.authorizationParams;
    this.accessTokenPath = builder.accessTokenPath;
    this.generateCSRFToken = builder.generateCSRFToken;
    this.scope = builder.scope;
    this.tokens = builder.tokens;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("baseURL", baseURL)
      .field("clientID", clientID)
      .maskedField("clientSecret", clientSecret)
      .field("callbackURL", callbackURL)
      .field("authorizationURL", authorizationURL)
      .field("authorizationParams", authorizationParams)
      .field("authorizationPath", authorizationPath)
      .field("accessTokenPath", accessTokenPath)
      .field("generateCSRFToken", generateCSRFToken)
      .field("tokens", tokens.keySet())
      .toString();
  }

  @JsonIgnore
  public String getAccessToken() {
    if (tokens != null && tokens.containsKey(OAuthTokenCredentials.ACCESS_TOKEN)) {
      return tokens.get(OAuthTokenCredentials.ACCESS_TOKEN);
    } else {
      return null;
    }
  }

  @JsonIgnore
  public String getRefreshToken() {
    if (tokens != null && tokens.containsKey(OAuthTokenCredentials.REFRESH_TOKEN)) {
      return tokens.get(OAuthTokenCredentials.REFRESH_TOKEN);
    } else {
      return null;
    }
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpOAuthConfigBuilder {
    @Getter
    @Setter
    private String baseURL;

    @Getter
    @Setter
    private String clientID;

    @Getter
    @Setter
    private String clientSecret;

    @Getter
    @Setter
    private String callbackURL;

    @Getter
    @Setter
    private String authorizationURL;

    @Getter
    @Setter
    private Map<String, String> authorizationParams;

    @Getter
    @Setter
    private String authorizationPath;

    @Getter
    @Setter
    private String accessTokenPath;

    @Getter
    @Setter
    private String scope;

    @Getter
    @Setter
    private boolean generateCSRFToken;

    public HttpOAuthConfig build() {
      return new HttpOAuthConfig(this);
    }
  }
}
