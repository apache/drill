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
package org.apache.drill.common.logical;


import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
public abstract class StoragePluginConfig {

  Logger logger = LoggerFactory.getLogger(StoragePluginConfig.class);

  // DO NOT include enabled status in equality and hash
  // comparisons; doing so will break the plugin registry.
  protected Boolean enabled;
  protected final boolean directCredentials;
  protected final CredentialsProvider credentialsProvider;
  protected final AuthMode authMode;
  protected OAuthConfig oAuthConfig;

  public StoragePluginConfig() {
    this(PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER,  true);
  }

  public StoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean directCredentials
  ) {
    // The overridable default auth mode is shared user.
    this(credentialsProvider, directCredentials, AuthMode.SHARED_USER);
  }

  public StoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean directCredentials,
    AuthMode authMode
  ) {
    this(credentialsProvider, directCredentials, authMode, null);
  }

  public StoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean directCredentials,
    AuthMode authMode,
    OAuthConfig oAuthConfig
  ) {
    this.credentialsProvider = credentialsProvider;
    this.directCredentials = directCredentials;
    this.authMode = authMode;
    this.oAuthConfig = oAuthConfig;
  }

  /**
   * Check for enabled status of the plugin
   *
   * @return true, when enabled. False, when disabled or status is absent
   */
  public boolean isEnabled() {
    return enabled != null && enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Allows to check whether the enabled status is present in config
   *
   * @return true if enabled status is present, false otherwise
   */
  @JsonIgnore
  public boolean isEnabledStatusPresent() {
    return enabled != null;
  }

  public String getValue(String key) {
    return null;
  }

  public CredentialsProvider getCredentialsProvider() {
    if (directCredentials) {
      return null;
    }
    return credentialsProvider;
  }

  public StoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider) {
    throw UserException.unsupportedError()
      .message("%s does not support credential provider updates.", getClass())
      .build(logger);
  }

  public AuthMode getAuthMode() {
    return authMode;
  }

  @JsonProperty("oAuthConfig")
  @JsonInclude(Include.NON_NULL)
  public OAuthConfig oAuthConfig() {
    return oAuthConfig;
  }

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

  /**
   * The standardised authentication modes that storage plugins may offer.
   */
  public enum AuthMode {
    /**
     * Connects using a single set of shared credentials stored in some
     * credential provider. If no credentials are present, the plugin may
     * connect with no credentials or make implicit use of the Drillbit's
     * identity (e.g. OS process user). Unaffected by the Drill query user's
     * identity.
     */
    SHARED_USER,
    /**
     * Depending on the plugin, connects using one of the two modes above then
     * instructs the external storage to set the identity on the connection
     * to that of the Drill query user.  User identity in the external system
     * will match the Drill query user's identity.
     */
    USER_IMPERSONATION,
    /**
     * Connects with stored credentials looked up for (translated from)
     * the Drill query user.  User identity in the external system will be
     * a function of the Drill query user's identity (1-1 or *-1) .
     */
    USER_TRANSLATION;

    public static AuthMode parseOrDefault(String authMode, AuthMode defavlt) {
      return !Strings.isNullOrEmpty(authMode) ? AuthMode.valueOf(authMode.toUpperCase()) : defavlt;
    }
  }
}
