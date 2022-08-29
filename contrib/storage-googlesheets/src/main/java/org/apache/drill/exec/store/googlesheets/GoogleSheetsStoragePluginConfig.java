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

package org.apache.drill.exec.store.googlesheets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.store.security.CredentialProviderUtils;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonTypeName(GoogleSheetsStoragePluginConfig.NAME)
public class GoogleSheetsStoragePluginConfig extends StoragePluginConfig {
  private static final String AUTH_URI = "https://accounts.google.com/o/oauth2/auth";
  private static final String TOKEN_URI = "https://oauth2.googleapis.com/token";
  private static final String GOOGLE_SHEET_SCOPE = "https://www.googleapis.com/auth/spreadsheets";
  private static final String GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly";
  private static final String DEFAULT_SCOPE = GOOGLE_SHEET_SCOPE + " " + GOOGLE_DRIVE_SCOPE;
  private static final String DEFAULT_RESPONSE_TYPE = "code";
  public static final String NAME = "googlesheets";
  private final List<String> redirectUris;
  private final String authUri;
  private final String tokenUri;
  private final Boolean extractHeaders;
  private final Boolean allTextMode;
  private final OAuthConfig oAuthConfig;

  @JsonCreator
  public GoogleSheetsStoragePluginConfig(@JsonProperty("clientID") String clientID,
                                         @JsonProperty("clientSecret") String clientSecret,
                                         @JsonProperty("redirectUris") List<String> redirectUris,
                                         @JsonProperty("authUri") String authUri,
                                         @JsonProperty("tokenUri") String tokenUri,
                                         @JsonProperty("allTextMode") Boolean allTextMode,
                                         @JsonProperty("extractHeaders") Boolean extractHeaders,
                                         @JsonProperty("oAuthConfig") OAuthConfig oAuthConfig,
                                         @JsonProperty("credentialsProvider") CredentialsProvider credentialsProvider) {
    super(CredentialProviderUtils.getCredentialsProvider(clientID, clientSecret, null, null,
        null, null, null, credentialsProvider),
      false);
    this.redirectUris = redirectUris == null ? new ArrayList<>() : redirectUris;
    this.authUri = StringUtils.isEmpty(authUri) ? AUTH_URI: authUri;
    this.tokenUri = StringUtils.isEmpty(tokenUri) ? TOKEN_URI: tokenUri;

    this.extractHeaders = extractHeaders != null && extractHeaders;
    this.allTextMode = allTextMode != null && allTextMode;

    Map<String, String> authParams = new HashMap<>();

    // Add OAuth Information
    if (oAuthConfig == null) {
      StringBuilder callbackBuilder = new StringBuilder();
      if (redirectUris != null) {
        boolean firstRun = true;
        for (String url: redirectUris) {
          if (!firstRun) {
            callbackBuilder.append(",");
          }
          callbackBuilder.append(url);
          firstRun = false;
        }
        // Add additional parameter for Google Authentication
        authParams.put("response_type", DEFAULT_RESPONSE_TYPE);
        authParams.put("scope",DEFAULT_SCOPE);
      }

      this.oAuthConfig = OAuthConfig.builder()
        .authorizationURL(AUTH_URI)
        .callbackURL(callbackBuilder.toString())
        .authorizationParams(authParams)
        .build();
    } else {
      this.oAuthConfig = oAuthConfig;
    }
  }

  public GoogleSheetsStoragePluginConfig(GoogleSheetsStoragePluginConfig that, CredentialsProvider credentialsProvider) {
    super(credentialsProvider, false, that.authMode);
    this.redirectUris = that.redirectUris;
    this.authUri = that.authUri;
    this.tokenUri = that.tokenUri;
    this.extractHeaders = that.extractHeaders;
    this.allTextMode = that.allTextMode;
    this.oAuthConfig = that.oAuthConfig;
  }

  @JsonIgnore
  public static GoogleSheetsStoragePluginConfigBuilder builder() {
    return new GoogleSheetsStoragePluginConfigBuilder();
  }

  @JsonIgnore
  public Optional<OAuthTokenCredentials> getOAuthCredentials() {
    return new OAuthTokenCredentials.Builder()
      .setCredentialsProvider(credentialsProvider)
      .build();
  }

  @JsonIgnore
  public String getClientID() {
    if(getOAuthCredentials().isPresent()) {
      return getOAuthCredentials().get().getClientID();
    } else {
      return null;
    }
  }

  @JsonIgnore
  public String getClientSecret() {
    if (getOAuthCredentials().isPresent()) {
      return getOAuthCredentials().get().getClientSecret();
    } else {
      return null;
    }
  }

  @JsonProperty("allTextMode")
  public Boolean allTextMode() {
    return allTextMode;
  }

  @JsonProperty("extractHeaders")
  public Boolean getExtractHeaders() {
    return extractHeaders;
  }

  @JsonProperty("oAuthConfig")
  public OAuthConfig getoAuthConfig() {
    return oAuthConfig;
  }

  @JsonIgnore
  public GoogleClientSecrets getSecrets() {
    Details details = new Details()
      .setClientId(getClientID())
      .setClientSecret(getClientSecret())
      .setRedirectUris(redirectUris)
      .setAuthUri(oAuthConfig.getAuthorizationURL())
      .setTokenUri(tokenUri);

    return new GoogleClientSecrets().setInstalled(details);
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    GoogleSheetsStoragePluginConfig thatConfig  = (GoogleSheetsStoragePluginConfig) that;
    return Objects.equals(credentialsProvider, thatConfig.credentialsProvider) &&
      Objects.equals(redirectUris, thatConfig.redirectUris) &&
      Objects.equals(tokenUri, thatConfig.tokenUri) &&
      Objects.equals(allTextMode, thatConfig.allTextMode) &&
      Objects.equals(oAuthConfig, thatConfig.oAuthConfig) &&
      Objects.equals(extractHeaders, thatConfig.extractHeaders);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credentialsProvider, redirectUris, tokenUri, allTextMode, extractHeaders, oAuthConfig);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("credentialProvider", credentialsProvider)
      .field("redirectUris", redirectUris.toArray())
      .field("extractHeaders", extractHeaders)
      .field("allTextMode", allTextMode)
      .field("tokenUri", tokenUri)
      .field("oauthConfig", oAuthConfig)
      .toString();
  }

  @Override
  public StoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    return new GoogleSheetsStoragePluginConfig(this, credentialsProvider);
  }

  public static class GoogleSheetsStoragePluginConfigBuilder {
    private String clientID;

    private String clientSecret;

    private List<String> redirectUris;

    private String authUri;

    private String tokenUri;

    private OAuthConfig oAuthConfig;

    private Boolean allTextMode;

    private Boolean extractHeaders;

    private CredentialsProvider credentialsProvider;

    GoogleSheetsStoragePluginConfigBuilder() {
    }

    public GoogleSheetsStoragePluginConfigBuilder clientID(String clientID) {
      this.clientID = clientID;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder clientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder redirectUris(List<String> redirectUris) {
      this.redirectUris = redirectUris;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder authUri(String authUri) {
      this.authUri = authUri;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder tokenUri(String tokenUri) {
      this.tokenUri = tokenUri;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder OAuthConfig(OAuthConfig oAuthConfig) {
      this.oAuthConfig = oAuthConfig;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder allTextMode(Boolean allTextMode) {
      this.allTextMode = allTextMode;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder extractHeaders(Boolean extractHeaders) {
      this.extractHeaders = extractHeaders;
      return this;
    }

    public GoogleSheetsStoragePluginConfigBuilder credentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public GoogleSheetsStoragePluginConfig build() {
      return new GoogleSheetsStoragePluginConfig(clientID, clientSecret, redirectUris, authUri, tokenUri, allTextMode, extractHeaders, oAuthConfig, credentialsProvider);
    }

    @Override
    public String toString() {
      return new PlanStringBuilder(this)
        .field("clientID", clientID)
        .maskedField("clientSecret", clientSecret)
        .field("allTextMode", allTextMode)
        .field("extractHeaders", extractHeaders)
        .field("redirectUris", redirectUris)
        .field("authUri", authUri)
        .field("tokenUri", tokenUri)
        .field("oAuthConfig", oAuthConfig)
        .toString();
    }
  }
}
