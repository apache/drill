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
package org.apache.drill.exec.store.drill.plugin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.PluginRulesProviderImpl;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRulesSupplier;
import org.apache.drill.exec.store.drill.plugin.plan.DrillPluginImplementor;
import org.apache.drill.exec.store.drill.plugin.schema.DrillSchemaFactory;
import org.apache.drill.exec.store.plan.rel.PluginRel;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DrillStoragePlugin extends AbstractStoragePlugin {

  private final DrillStoragePluginConfig drillConfig;
  private final DrillSchemaFactory schemaFactory;
  private final StoragePluginRulesSupplier storagePluginRulesSupplier;
  private final Map<String, DrillClient> userClients;

  public DrillStoragePlugin(
      DrillStoragePluginConfig drillConfig,
      DrillbitContext context,
      String name) {
    super(context, name);
    this.drillConfig = drillConfig;
    this.schemaFactory = new DrillSchemaFactory(this, name);
    this.storagePluginRulesSupplier = storagePluginRulesSupplier(name);

    assert drillConfig.getConnection().startsWith(DrillStoragePluginConfig.CONNECTION_STRING_PREFIX);

    this.userClients = new ConcurrentHashMap<>();
  }

  private static StoragePluginRulesSupplier storagePluginRulesSupplier(String name) {
    Convention convention = new Convention.Impl("DRILL." + name, PluginRel.class);
    return StoragePluginRulesSupplier.builder()
      .rulesProvider(new PluginRulesProviderImpl(convention, DrillPluginImplementor::new))
      .supportsProjectPushdown(true)
      .supportsSortPushdown(true)
      .supportsAggregatePushdown(true)
      .supportsFilterPushdown(true)
      .supportsLimitPushdown(true)
      .supportsUnionPushdown(true)
      .supportsJoinPushdown(true)
      .convention(convention)
      .build();
  }

  @Override
  public DrillStoragePluginConfig getConfig() {
    return drillConfig;
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
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    DrillScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<DrillScanSpec>() {
    });
    return new DrillGroupScan(userName, drillConfig, scanSpec);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
      case LOGICAL:
        return storagePluginRulesSupplier.getOptimizerRules();
      case LOGICAL_PRUNE_AND_JOIN:
      case LOGICAL_PRUNE:
      case PARTITION_PRUNING:
      case JOIN_PLANNING:
      default:
        return Collections.emptySet();
    }
  }

  public Convention convention() {
    return storagePluginRulesSupplier.convention();
  }

  public DrillClient getClient(String userName) {
    userClients.computeIfAbsent(userName, this::createClient);
    // recompute drill client for the case of closed connection
    return userClients.computeIfPresent(userName, (name, value) -> {
      if (!value.connectionIsActive()) {
        AutoCloseables.closeSilently(value);
        return createClient(name);
      }
      return value;
    });
  }

  private DrillClient createClient(String userName) {
    return drillConfig.getDrillClient(userName, null);
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(userClients.values().toArray(new AutoCloseable[0]));
  }
}
