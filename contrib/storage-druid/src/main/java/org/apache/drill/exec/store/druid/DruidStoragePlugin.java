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
package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SessionOptionManager;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.druid.druid.ScanQueryBuilder;
import org.apache.drill.exec.store.druid.rest.DruidAdminClient;
import org.apache.drill.exec.store.druid.rest.DruidQueryClient;
import org.apache.drill.exec.store.druid.rest.RestClient;
import org.apache.drill.exec.store.druid.rest.RestClientWrapper;
import org.apache.drill.exec.store.druid.schema.DruidSchemaFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DruidStoragePlugin extends AbstractStoragePlugin {
  private final DrillbitContext context;
  private final DruidStoragePluginConfig pluginConfig;
  private final DruidAdminClient druidAdminClient;
  private final DruidQueryClient druidQueryClient;
  private final DruidSchemaFactory schemaFactory;
  private final ScanQueryBuilder scanQueryBuilder;

  public DruidStoragePlugin(DruidStoragePluginConfig pluginConfig, DrillbitContext context, String name) {
    super(context, name);
    this.pluginConfig = pluginConfig;
    this.context = context;
    RestClient restClient = new RestClientWrapper();
    this.druidAdminClient = new DruidAdminClient(pluginConfig.getCoordinatorAddress(), restClient);
    this.druidQueryClient = new DruidQueryClient(pluginConfig.getBrokerAddress(), restClient);
    this.schemaFactory = new DruidSchemaFactory(this, name);
    this.scanQueryBuilder = new ScanQueryBuilder();
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
    DruidScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<DruidScanSpec>() {});
    return new DruidGroupScan(userName, this, scanSpec, null, -1, metadataProviderManager);
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules(
    OptimizerRulesContext optimizerRulesContext,
    PlannerPhase phase
  ) {
    switch (phase) {
      case PHYSICAL:
        return ImmutableSet.of(DruidPushDownFilterForScan.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  @Override
  public DruidStoragePluginConfig getConfig() {
    return pluginConfig;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  public DruidAdminClient getAdminClient() {
    return this.druidAdminClient;
  }

  public DruidQueryClient getDruidQueryClient() { return this.druidQueryClient; }

  public ScanQueryBuilder getScanQueryBuilder() { return scanQueryBuilder; }
}
