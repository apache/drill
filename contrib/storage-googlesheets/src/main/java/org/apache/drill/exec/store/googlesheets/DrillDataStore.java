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

import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.util.store.AbstractMemoryDataStore;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.oauth.TokenRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class DrillDataStore<V extends Serializable> extends AbstractMemoryDataStore<V> {

  private static final Logger logger = LoggerFactory.getLogger(DrillDataStore.class);
  private final PersistentTokenTable tokenTable;

  private final DrillDataStoreFactory drillDataStoreFactory;

  DrillDataStore(OAuthTokenProvider tokenProvider, String pluginName, String userID, DrillDataStoreFactory dataStoreFactory) {
    super(dataStoreFactory, userID);
    this.drillDataStoreFactory = dataStoreFactory;
    TokenRegistry tokenRegistry = tokenProvider.getOauthTokenRegistry(userID);
    this.tokenTable = tokenRegistry.getTokenTable(pluginName);
    if (hasValidTokens(tokenTable)) {
      keyValueMap.put(tokenTable.ACCESS_TOKEN_KEY, tokenTable.getAccessToken().getBytes(StandardCharsets.UTF_8));
      keyValueMap.put(tokenTable.REFRESH_TOKEN_KEY, tokenTable.getRefreshToken().getBytes(StandardCharsets.UTF_8));
      if (tokenTable.getExpiresIn() != null) {
        keyValueMap.put(tokenTable.EXPIRES_IN_KEY, tokenTable.getExpiresIn().getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * Updates credentials in Drill's persistent store.
   */
  @Override
  public void save() {
    logger.debug("Saving credentials to token table");
    String accessToken = new String(keyValueMap.get(tokenTable.ACCESS_TOKEN_KEY), StandardCharsets.UTF_8);
    String refreshToken = new String(keyValueMap.get(tokenTable.REFRESH_TOKEN_KEY), StandardCharsets.UTF_8);
    String expiresIn = new String(keyValueMap.get(tokenTable.EXPIRES_IN_KEY), StandardCharsets.UTF_8);
    tokenTable.setAccessToken(accessToken);
    tokenTable.setRefreshToken(refreshToken);
    tokenTable.setExpiresIn(expiresIn);
  }

  /**
   * Returns a {@link StoredCredential} containing the given user's access and refresh tokens.  This method
   * must only be called AFTER the tokenTable has been initialized.
   * @return A {@link StoredCredential with the user's access and refresh tokens.}
   */
  public StoredCredential getStoredCredential() {
    if (tokenTable == null) {
      logger.debug("Token table is null. Please be sure to initialize token table before calling getStoredCredentials.");
      return null;
    }

    StoredCredential storedCredential = new StoredCredential();
    storedCredential.setAccessToken(tokenTable.getAccessToken());
    storedCredential.setRefreshToken(tokenTable.getRefreshToken());

    if (StringUtils.isNotEmpty(tokenTable.getExpiresIn())) {
      storedCredential.setExpirationTimeMilliseconds(Long.valueOf(tokenTable.getExpiresIn()));
    }
    return storedCredential;
  }

  @Override
  public DrillDataStoreFactory getDataStoreFactory() {
    return drillDataStoreFactory;
  }

  private boolean hasValidTokens(PersistentTokenTable tokenTable) {
    return StringUtils.isNotEmpty(tokenTable.getAccessToken()) && StringUtils.isNotEmpty(tokenTable.getRefreshToken());
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("tokenTable", tokenTable)
      .field("data store factory", drillDataStoreFactory)
      .toString();
  }
}
