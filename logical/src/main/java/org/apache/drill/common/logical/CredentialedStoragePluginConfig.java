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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CredentialedStoragePluginConfig extends StoragePluginConfig {

  private static final Logger logger = LoggerFactory.getLogger(CredentialedStoragePluginConfig.class);
  protected boolean directCredentials;
  protected final CredentialsProvider credentialsProvider;
  protected OAuthConfig oAuthConfig;

  public CredentialedStoragePluginConfig() {
    this(PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER,  true);
  }

  public CredentialedStoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean directCredentials
  ) {
    // Default auth mode for credentialed storage plugins is shared user.
    this(credentialsProvider, directCredentials, AuthMode.SHARED_USER);
  }

  public CredentialedStoragePluginConfig(
    CredentialsProvider credentialsProvider,
    boolean directCredentials,
    AuthMode authMode
  ) {
    this.credentialsProvider = credentialsProvider;
    this.directCredentials = directCredentials;
    this.authMode = authMode;
    this.oAuthConfig = null;
  }

  public CredentialedStoragePluginConfig(
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

  public abstract CredentialedStoragePluginConfig updateCredentialProvider(CredentialsProvider credentialsProvider);

  @JsonProperty("oAuthConfig")
  @JsonInclude(Include.NON_NULL)
  public OAuthConfig oAuthConfig() {
    return oAuthConfig;
  }

  public CredentialsProvider getCredentialsProvider() {
    if (directCredentials) {
      return null;
    }
    return credentialsProvider;
  }
}
