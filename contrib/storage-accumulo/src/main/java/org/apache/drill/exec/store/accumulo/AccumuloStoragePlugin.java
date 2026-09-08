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
package org.apache.drill.exec.store.accumulo;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.store.accumulo.schema.AccumuloSchemaProvider;
import org.apache.drill.exec.store.accumulo.schema.MetadataTableSchemaProvider;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableSet;

/**
 * Accumulo storage plugin for Apache Drill.
 *
 * <p>This plugin provides read access to Apache Accumulo tables,
 * with support for filter, projection, limit, and sort pushdowns.</p>
 *
 * <p>Authentication modes:</p>
 * <ul>
 *   <li>PASSWORD: Username/password authentication</li>
 *   <li>KERBEROS + SHARED_USER: Service principal for all queries</li>
 *   <li>KERBEROS + USER_IMPERSONATION: Service authenticates, then impersonates
 *       Drill user via delegation token</li>
 * </ul>
 */
public class AccumuloStoragePlugin extends AbstractStoragePlugin {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloStoragePlugin.class);

  private final AccumuloStoragePluginConfig config;
  private final AccumuloSchemaFactory schemaFactory;
  private final AccumuloSchemaProvider schemaProvider;
  private final AccumuloConnectionManager connectionManager;

  public AccumuloStoragePlugin(
      AccumuloStoragePluginConfig config,
      DrillbitContext context,
      String name) {
    super(context, name);
    this.config = config;
    this.schemaProvider = new MetadataTableSchemaProvider(config.getSchemaMetadataTable());
    this.schemaFactory = new AccumuloSchemaFactory(this);
    this.connectionManager = new AccumuloConnectionManager(config);

    logger.info("Initialized Accumulo storage plugin '{}' with ZooKeeper quorum: {}, authType: {}, authMode: {}",
        name, config.getZookeeperQuorum(), config.getAuthenticationType(), config.getAuthMode());
  }

  /**
   * Returns the schema provider for this plugin.
   */
  public AccumuloSchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  /**
   * Returns the connection manager for this plugin.
   */
  public AccumuloConnectionManager getConnectionManager() {
    return connectionManager;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public AccumuloStoragePluginConfig getConfig() {
    return config;
  }

  /**
   * Returns a shared AccumuloClient for service-level operations.
   *
   * <p>This is the service client that authenticates using the configured
   * credentials (password or Kerberos). For user impersonation, use
   * {@link #getClientForUser(String)} instead.</p>
   *
   * @return the service Accumulo client
   * @throws UserException if connection fails
   */
  public AccumuloClient getClient() {
    return connectionManager.getServiceClient();
  }

  /**
   * Returns an AccumuloClient for the specified user.
   *
   * <p>Behavior depends on the auth mode:</p>
   * <ul>
   *   <li>SHARED_USER: Returns the service client (same for all users)</li>
   *   <li>USER_IMPERSONATION: Returns a client using the user's delegation token</li>
   * </ul>
   *
   * @param userName the Drill query user name
   * @return an AccumuloClient for the user
   */
  public AccumuloClient getClientForUser(String userName) {
    return connectionManager.getClientForUser(userName);
  }

  /**
   * Generates a delegation token for the specified user.
   *
   * <p>This is used in distributed execution to pass the user's credentials
   * to executor fragments.</p>
   *
   * @param userName the user to generate a token for
   * @return the delegation token info, or null if impersonation is not enabled
   */
  public DelegationTokenInfo generateDelegationToken(String userName) {
    if (!config.isUserImpersonationEnabled() || !config.isUseDelegationTokens()) {
      return null;
    }

    try {
      return connectionManager.getDelegationToken(userName);
    } catch (Exception e) {
      throw UserException.connectionError(e)
          .message("Failed to generate delegation token for user '%s'", userName)
          .addContext("Plugin", getName())
          .build(logger);
    }
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    AccumuloScanSpec scanSpec = selection.getListWith(new TypeReference<AccumuloScanSpec>() {});

    // Generate delegation token if user impersonation is enabled
    DelegationTokenInfo delegationToken = null;
    if (config.isUserImpersonationEnabled()) {
      delegationToken = generateDelegationToken(userName);
      logger.debug("Generated delegation token for user '{}' in physical scan", userName);
    }

    return new AccumuloGroupScan(userName, this, scanSpec, null, -1, delegationToken);
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules(
      OptimizerRulesContext optimizerRulesContext,
      PlannerPhase phase) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(
            AccumuloPushSortIntoScan.SORT_ON_SCAN
        );
      case PHYSICAL:
        return ImmutableSet.of(
            AccumuloPushFilterIntoScan.FILTER_ON_SCAN,
            AccumuloPushFilterIntoScan.FILTER_ON_PROJECT
        );
      default:
        return ImmutableSet.of();
    }
  }

  @Override
  public void close() throws Exception {
    logger.debug("Closing Accumulo storage plugin: {}", getName());
    connectionManager.close();
  }
}
