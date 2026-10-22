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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.auth.oauth2.StoredCredential;
import com.google.api.client.util.store.DataStore;
import com.google.api.services.drive.Drive;
import com.google.api.services.sheets.v4.Sheets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.oauth.TokenRegistry;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.base.filter.FilterPushDownUtils;
import org.apache.drill.exec.store.googlesheets.schema.GoogleSheetsSchemaFactory;
import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class GoogleSheetsStoragePlugin extends AbstractStoragePlugin {
  private final static Logger logger = LoggerFactory.getLogger(GoogleSheetsStoragePlugin.class);
  private final static String SHARED_USERNAME = "anonymous";
  private final GoogleSheetsStoragePluginConfig config;
  private final GoogleSheetsSchemaFactory schemaFactory;
  private final OAuthTokenProvider tokenProvider;
  /**
   * Google credential stores and services, cached per identity.
   *
   * <p>These were previously single fields on the plugin. A plugin instance is
   * shared by every query, so the first caller's authenticated service was handed
   * to every later caller regardless of which user asked for it, which under
   * {@code USER_TRANSLATION} meant one user's queries reaching Google as another
   * user. They are keyed by {@link #cacheKey(String)} so each identity gets, and
   * keeps, only its own.
   */
  private final Map<String, DataStore<StoredCredential>> dataStores = new ConcurrentHashMap<>();
  private final Map<String, Sheets> sheetsServices = new ConcurrentHashMap<>();
  private final Map<String, Drive> driveServices = new ConcurrentHashMap<>();


  public GoogleSheetsStoragePlugin(GoogleSheetsStoragePluginConfig configuration, DrillbitContext context, String name) {
    super(context, name);
    this.config = configuration;
    this.tokenProvider = context.getOauthTokenProvider();
    this.schemaFactory = new GoogleSheetsSchemaFactory(this);
  }

  public void initializeOauthTokenTable(SchemaConfig schemaConfig) {
    // A word about how GoogleSheets (GS) handles authorization and authentication.
    // GS uses OAuth 2.0 for authorization.
    // The GS Sheets object is the client which interacts with the actual data, however
    // it does not provide a straightforward way of passing credentials into this object.
    // GS has three objects:  the credential, storedCredential, and the credential dataStore.
    //
    // The Credential Object
    // The credential really should be called the applicationCredential or something like that, as
    // it stores the OAuth credentials for the application such as the clientID, clientSecret
    //
    // The Stored Credential Object
    // This object has no relation to the Credential object, and it stores the user's credentials,
    // specifically the access and refresh tokens.
    //
    // The DataStore Object is a synchronized store of storedCredential objects.
    // The approach we take here is to use Drill's existing OAuth infrastructure
    // to store the tokens in PersistentTokenStores, just like the HTTP plugin. When
    // the plugin is loaded, we read the tokens from the persistent store into a GS dataStore.
    // This happens when the plugin is registered.

    getTokenRegistry(schemaConfig == null ? null : schemaConfig.getUserName());
  }

  /**
   * The token registry holding {@code queryUser}'s tokens, with this plugin's
   * token table created in it if absent. Resolved per call; see
   * {@link #dataStores} for why nothing here is cached on the plugin.
   */
  public TokenRegistry getTokenRegistry(String queryUser) {
    TokenRegistry tokenRegistry = tokenProvider.getOauthTokenRegistry(registryIdentity(queryUser));
    tokenRegistry.createTokenTable(getName());
    return tokenRegistry;
  }

  /**
   * The identity whose Drill token registry holds this plugin's tokens: the query
   * user under user translation, or null for the single shared registry. Note that
   * {@link #SHARED_USERNAME} is only a Google-side placeholder and must not be used
   * as a registry identity, or a shared plugin's tokens would be looked up in a
   * per-user registry that the REST credential endpoints never write to.
   */
  private String registryIdentity(String queryUser) {
    return config.getAuthMode() == AuthMode.USER_TRANSLATION ? queryUser : null;
  }

  /**
   * The Google-side data store id, which must be a non-null string and is not a
   * Drill identity. Under user translation it is the query user; otherwise a
   * fixed placeholder.
   */
  private String googleStoreId(String queryUser) {
    return config.getAuthMode() == AuthMode.USER_TRANSLATION ? queryUser : SHARED_USERNAME;
  }

  /**
   * Cache key for the per-identity maps. Prefixed rather than using the user name
   * directly, so that a user literally named {@link #SHARED_USERNAME} — which is
   * what every web user is called when authentication is disabled — cannot share
   * a cache entry with a shared-credential plugin.
   */
  private String cacheKey(String queryUser) {
    return config.getAuthMode() == AuthMode.USER_TRANSLATION ? "user:" + queryUser : "shared";
  }

  private DataStore<StoredCredential> buildDataStore(String queryUser) {
    return new DrillDataStoreFactory(tokenProvider, getName(), registryIdentity(queryUser))
      .createDataStore(googleStoreId(queryUser));
  }

  public DataStore<StoredCredential> getDataStore(String queryUser) {
    return dataStores.computeIfAbsent(cacheKey(queryUser), k -> buildDataStore(queryUser));
  }


  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    initializeOauthTokenTable(schemaConfig);
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  /**
   * This plugin's token table for the shared identity. Prefer
   * {@link #getTokenTable(String)} wherever the query user is known.
   */
  public PersistentTokenTable getTokenTable() {
    return getTokenTable(null);
  }

  /** This plugin's token table holding {@code queryUser}'s tokens. */
  public PersistentTokenTable getTokenTable(String queryUser) {
    return getTokenRegistry(queryUser).getTokenTable(getName());
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
                                           SessionOptionManager options) throws IOException {
    return getPhysicalScan(userName, selection, AbstractGroupScan.ALL_COLUMNS,
      options, null);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
                                           SessionOptionManager options, MetadataProviderManager metadataProviderManager) throws IOException {
    return getPhysicalScan(userName, selection, AbstractGroupScan.ALL_COLUMNS,
      options, metadataProviderManager);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
                                           List<SchemaPath> columns) throws IOException {
    return getPhysicalScan(userName, selection, columns, null, null);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    return getPhysicalScan(userName, selection, AbstractGroupScan.ALL_COLUMNS, null);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns, SessionOptionManager options,
                                           MetadataProviderManager metadataProviderManager) throws IOException {
    GoogleSheetsScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<GoogleSheetsScanSpec>() {});
    return new GoogleSheetsGroupScan(userName, scanSpec, this, metadataProviderManager);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {

    // Push-down planning is done at the logical phase so it can
    // influence parallelization in the physical phase. Note that many
    // existing plugins perform filter push-down at the physical
    // phase, which also works fine if push-down is independent of
    // parallelization.
    if (FilterPushDownUtils.isFilterPushDownPhase(phase)) {
      return GoogleSheetsPushDownListener.rulesFor(optimizerContext);
    } else {
      return ImmutableSet.of();
    }
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  /**
   * This function is only used for testing and creates the necessary token tables.  Note that
   * the token tables still need to be populated.
   */
  @VisibleForTesting
  public void initializeTokenTableForTesting() {
    getTokenRegistry(null);
  }

  @Override
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public boolean supportsInsert() {
    return true;
  }

  /**
   * This method gets (and caches) the Google Service needed for API calls.
   * @return An authenticated {@link Sheets} Google Sheets service.
   */
  public Sheets getSheetsService(String queryUser) {
    return sheetsServices.computeIfAbsent(cacheKey(queryUser), k -> buildSheetsService(queryUser));
  }

  private Sheets buildSheetsService(String queryUser) {
    try {
      return GoogleSheetsUtils.getSheetsService(
        config, getDataStore(queryUser), googleStoreId(queryUser));
    } catch (IOException | GeneralSecurityException e) {
      throw UserException.connectionError(e)
        .message("Error connecting to Googlesheets Service: " + e.getMessage())
        .build(logger);
    }
  }

  /**
   * This method gets (and caches) the Google Drive Service needed for mapping Google Sheet names
   * to file tokens.
   * @param queryUser A {@link String} of the current query user.
   * @return A validated and authenticated {@link Drive} instance.
   */
  public Drive getDriveService(String queryUser) {
    return driveServices.computeIfAbsent(cacheKey(queryUser), k -> buildDriveService(queryUser));
  }

  private Drive buildDriveService(String queryUser) {
    try {
      return GoogleSheetsUtils.getDriveService(
        config, getDataStore(queryUser), googleStoreId(queryUser));
    } catch (IOException | GeneralSecurityException e) {
      throw UserException.connectionError(e)
        .message("Error connecting to Google Drive Service: " + e.getMessage())
        .build(logger);
    }
  }
}
