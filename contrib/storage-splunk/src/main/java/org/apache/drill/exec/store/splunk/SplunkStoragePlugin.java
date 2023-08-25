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

package org.apache.drill.exec.store.splunk;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.base.filter.FilterPushDownUtils;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SplunkStoragePlugin extends AbstractStoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(SplunkStoragePlugin.class);
  private final SplunkPluginConfig config;
  private final SplunkSchemaFactory schemaFactory;

  public SplunkStoragePlugin(SplunkPluginConfig configuration, DrillbitContext context, String name) {
    super(context, name);
    this.config = configuration;
    this.schemaFactory = new SplunkSchemaFactory(this);
  }

  @Override
  public SplunkPluginConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return config.isWritable();
  }

  @Override
  public boolean supportsInsert() {
    return config.isWritable();
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    // Check to see if user translation is enabled.  If so, and creds are
    // not present, then do not register any schemata.  This prevents
    // info schema errors.
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      Optional<UsernamePasswordCredentials> userCreds = config.getUsernamePasswordCredentials(schemaConfig.getUserName());
      if (! userCreds.isPresent()) {
        logger.debug(
          "No schemas will be registered in {} for query user {}.",
          getName(), schemaConfig.getUserName()
        );
        return;
      }
    }
    schemaFactory.registerSchemas(schemaConfig, parent);
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
    SplunkScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<SplunkScanSpec>() {});
    return new SplunkGroupScan(scanSpec, metadataProviderManager);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {

    // Push-down planning is done at the logical phase so it can
    // influence parallelization in the physical phase. Note that many
    // existing plugins perform filter push-down at the physical
    // phase, which also works fine if push-down is independent of
    // parallelization.
    if (FilterPushDownUtils.isFilterPushDownPhase(phase)) {
      return SplunkPushDownListener.rulesFor(optimizerContext);
    } else {
      return ImmutableSet.of();
    }
  }
}
