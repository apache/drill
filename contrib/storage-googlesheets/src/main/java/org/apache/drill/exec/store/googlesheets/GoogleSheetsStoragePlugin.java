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
import java.util.Set;

public class GoogleSheetsStoragePlugin extends AbstractStoragePlugin {
  private final static Logger logger = LoggerFactory.getLogger(GoogleSheetsStoragePlugin.class);
  private final static String SHARED_USERNAME = "anonymous";
  private final GoogleSheetsStoragePluginConfig config;
  private final GoogleSheetsSchemaFactory schemaFactory;
  private final OAuthTokenProvider tokenProvider;
  private DataStore<StoredCredential> dataStore;
  private Sheets service;
  private Drive driveService;
  private TokenRegistry tokenRegistry;
  private String username;


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

    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      this.username = schemaConfig.getUserName();
      tokenRegistry = tokenProvider.getOauthTokenRegistry(this.username);
    } else {
      this.username = SHARED_USERNAME;
      tokenRegistry = tokenProvider.getOauthTokenRegistry(null);
    }
    tokenRegistry.createTokenTable(getName());
    this.dataStore = new DrillDataStoreFactory(tokenProvider, getName()).createDataStore(this.username);
  }

  public DataStore<StoredCredential> getDataStore(String username) {
    if (this.dataStore == null) {
      this.dataStore = new DrillDataStoreFactory(tokenProvider, getName()).createDataStore(username);
    }
    return dataStore;
  }


  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    initializeOauthTokenTable(schemaConfig);
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  public PersistentTokenTable getTokenTable() {
    return tokenRegistry.getTokenTable(getName());
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
    return new GoogleSheetsGroupScan(this.username, scanSpec, this, metadataProviderManager);
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
    OAuthTokenProvider tokenProvider = context.getOauthTokenProvider();
    tokenRegistry = tokenProvider.getOauthTokenRegistry(null);
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
    if (service != null && dataStore != null) {
      return service;
    } else {
      // Check if datastore is null and initialize if so.
      if (dataStore == null) {
        this.dataStore = getDataStore(queryUser);
      }

      try {
        if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
          service = GoogleSheetsUtils.getSheetsService(config, dataStore, queryUser);
        } else {
          service = GoogleSheetsUtils.getSheetsService(config, dataStore, SHARED_USERNAME);
        }
        return service;
      } catch (IOException | GeneralSecurityException e) {
        throw UserException.connectionError(e)
          .message("Error connecting to Googlesheets Service: " + e.getMessage())
          .build(logger);
      }
    }
  }

  /**
   * This method gets (and caches) the Google Drive Service needed for mapping Google Sheet names
   * to file tokens.
   * @param queryUser A {@link String} of the current query user.
   * @return A validated and authenticated {@link Drive} instance.
   */
  public Drive getDriveService(String queryUser) {
    if (driveService != null && dataStore != null) {
      return driveService;
    } else {
      // Check if datastore is null and initialize if so.
      if (dataStore == null) {
        this.dataStore = getDataStore(queryUser);
      }

      try {
        if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
          driveService = GoogleSheetsUtils.getDriveService(config, dataStore, queryUser);
        } else {
          driveService = GoogleSheetsUtils.getDriveService(config, dataStore, SHARED_USERNAME);
        }
        return driveService;
      } catch (IOException | GeneralSecurityException e) {
        throw UserException.connectionError(e)
          .message("Error connecting to Google Drive Service: " + e.getMessage())
          .build(logger);
      }
    }
  }
}
