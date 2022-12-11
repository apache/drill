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

package org.apache.drill.exec.store.dfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.logical.OAuthConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class enables Drill to access file systems which use OAuth 2.0 for
 * authorization. The class contains methods to interact with Drill's token management
 * which makes use of a persistent store for the access and refresh tokens.
 */
public abstract class OAuthEnabledFileSystem extends FileSystem {
  private static final Logger logger = LoggerFactory.getLogger(OAuthEnabledFileSystem.class);

  private StoragePluginConfig pluginConfig;
  private PersistentTokenTable tokenTable;
  private CredentialsProvider credentialsProvider;
  private OAuthConfig oAuthConfig;

  public StoragePluginConfig getPluginConfig() {
    return pluginConfig;
  }

  public void setPluginConfig(StoragePluginConfig pluginConfig) {
    this.pluginConfig = pluginConfig;
    this.credentialsProvider = pluginConfig.getCredentialsProvider();
  }

  public AuthMode getAuthMode() {
    if (pluginConfig != null) {
      return this.pluginConfig.getAuthMode();
    } else {
      return null;
    }
  }

  public void setTokenTable(PersistentTokenTable tokenTable) {
    this.tokenTable = tokenTable;
  }

  public PersistentTokenTable getTokenTable() {
    return this.tokenTable;
  }

  public void setoAuthConfig(OAuthConfig oAuthConfig) {
    this.oAuthConfig = oAuthConfig;
  }

  public OAuthConfig getoAuthConfig() {
    return this.oAuthConfig;
  }

  public CredentialsProvider getCredentialsProvider() {
    return this.credentialsProvider;
  }

  /**
   * This function must be called by the inheritor class after every operation to make sure
   * that the tokens stay current.  This method compares the access token with the one from the
   * persistent store.  If the incoming tokens are different, it will update the persistent store.
   * @param accessToken The new access token
   * @param refreshToken The new refresh token
   */
  public void updateTokens(String accessToken, String refreshToken) {
    if (StringUtils.isNotEmpty(accessToken) && ! accessToken.contentEquals(tokenTable.getAccessToken())) {
      logger.debug("Updating access token for OAuth File System");
      tokenTable.setAccessToken(accessToken);
    }

    if (StringUtils.isNotEmpty(refreshToken) && ! refreshToken.contentEquals(tokenTable.getRefreshToken())) {
      logger.debug("Updating refresh token for OAuth File System");
      tokenTable.setRefreshToken(refreshToken);
    }
  }

  /**
   * This function must be called by the inheritor class after every operation to make sure
   * that the tokens stay current.  This method compares the access token with the one from the
   * persistent store.  If the incoming tokens are different, it will update the persistent store.
   * @param accessToken The new access token
   * @param refreshToken The new refresh token
   * @param expiresAt  The new expires at value.
   */
  public void updateTokens(String accessToken, String refreshToken, String expiresAt) {
    updateTokens(accessToken, refreshToken);
    if (StringUtils.isNotEmpty(expiresAt)) {
      logger.debug("Updating expires at for OAuth File System.");
      tokenTable.setExpiresIn(expiresAt);
    }
  }
}
