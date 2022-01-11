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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Builder
@Getter
@Setter
@ToString
@Accessors(fluent = true)
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpOAuthConfig.HttpOAuthConfigBuilder.class)
public class HttpOAuthConfig {

  @JsonProperty("callbackURL")
  private final String callbackURL;

  @JsonProperty("authorizationURL")
  private final String authorizationURL;

  @JsonProperty("authorizationParams")
  private final Map<String, String> authorizationParams;

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
    this.callbackURL = that.callbackURL;
    this.authorizationURL = that.authorizationURL;
    this.authorizationParams = that.authorizationParams;
    this.generateCSRFToken = that.generateCSRFToken;
    this.scope = that.scope;
    this.tokens = tokens == null ? new HashMap<>() : tokens;
  }

  private HttpOAuthConfig(HttpOAuthConfig.HttpOAuthConfigBuilder builder) {
    this.callbackURL = builder.callbackURL;
    this.authorizationURL = builder.authorizationURL;
    this.authorizationParams = builder.authorizationParams;
    this.generateCSRFToken = builder.generateCSRFToken;
    this.scope = builder.scope;
    this.tokens = builder.tokens;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpOAuthConfigBuilder {
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
    private String scope;

    @Getter
    @Setter
    private boolean generateCSRFToken;

    public HttpOAuthConfig build() {
      return new HttpOAuthConfig(this);
    }
  }
}
