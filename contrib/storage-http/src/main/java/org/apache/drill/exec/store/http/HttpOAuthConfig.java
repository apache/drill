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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
@ToString
@Getter
@Setter
@Builder
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpOAuthConfig.HttpOAuthConfigBuilder.class)
public class HttpOAuthConfig {

  private final String callbackURL;
  private final String authorizationURL;
  private final Map<String, String> authorizationParams;
  private final boolean generateCSRFToken;
  private final String scope;
  private final Map<String, String> tokens;

  @JsonCreator
  public HttpOAuthConfig(@JsonProperty("callbackURL") String callbackURL,
                         @JsonProperty("authorizationURL") String authorizationURL,
                         @JsonProperty("authorizationParams") Map<String, String> authorizationParams,
                         @JsonProperty("generateCSRFToken") boolean generateCSRFToken,
                         @JsonProperty("scope") String scope,
                         @JsonProperty("tokens") Map<String, String> tokens) {
    this.callbackURL = callbackURL;
    this.authorizationURL = authorizationURL;
    this.authorizationParams = authorizationParams;
    this.generateCSRFToken = generateCSRFToken;
    this.scope = scope;
    this.tokens = tokens;
  }

  public HttpOAuthConfig(HttpOAuthConfig.HttpOAuthConfigBuilder builder) {
    this.callbackURL = builder.callbackURL;
    this.authorizationURL = builder.authorizationURL;
    this.authorizationParams = builder.authorizationParams;
    this.generateCSRFToken = builder.generateCSRFToken;
    this.scope = builder.scope;
    this.tokens = builder.tokens;
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpOAuthConfigBuilder {
    public HttpOAuthConfig build() {
      return new HttpOAuthConfig(this);
    }
  }
}
