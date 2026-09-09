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

package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginRulesSupplier;
import org.apache.drill.exec.store.plan.rel.PluginRel;
import org.apache.drill.exec.store.PluginRulesProviderImpl;
import org.apache.drill.exec.store.sentinel.plan.SentinelPluginImplementor;
import org.apache.drill.exec.store.sentinel.auth.SentinelTokenManager;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.Collections;

public class SentinelStoragePlugin extends AbstractStoragePlugin {
  private static final Logger logger = LoggerFactory.getLogger(SentinelStoragePlugin.class);

  private final SentinelStoragePluginConfig config;
  private final SentinelTokenManager tokenManager;
  private final SentinelSchemaFactory schemaFactory;
  private final StoragePluginRulesSupplier rulesSupplier;
  private final Convention convention;

  public SentinelStoragePlugin(SentinelStoragePluginConfig config,
                              DrillbitContext context,
                              String name) {
    super(context, name);
    this.config = config;
    this.tokenManager = new SentinelTokenManager(
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret(),
        config.getAuthMode(),
        config.getCredentialsProvider(),
        config.getTokenEndpoint());
    this.schemaFactory = new SentinelSchemaFactory(this);

    this.convention = new Convention.Impl("SENTINEL." + name, PluginRel.class);
    this.rulesSupplier = StoragePluginRulesSupplier.builder()
        .rulesProvider(new PluginRulesProviderImpl(convention, SentinelPluginImplementor::new))
        .supportsFilterPushdown(true)
        .supportsProjectPushdown(true)
        .supportsLimitPushdown(true)
        .supportsSortPushdown(true)
        .supportsAggregatePushdown(true)
        .convention(convention)
        .build();
  }

  @Override
  public SentinelStoragePluginConfig getConfig() {
    return config;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(
      OptimizerRulesContext optimizerContext,
      PlannerPhase phase) {
    if (phase == PlannerPhase.PHYSICAL || phase == PlannerPhase.LOGICAL) {
      return rulesSupplier.getOptimizerRules();
    }
    return Collections.emptySet();
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    SentinelScanSpec scanSpec = selection.getListWith(new TypeReference<SentinelScanSpec>() {});
    return new SentinelGroupScan(config, scanSpec, (MetadataProviderManager) null);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  public SentinelTokenManager getTokenManager() {
    return tokenManager;
  }

  public Convention getConvention() {
    return convention;
  }

  public Set<String> getTableNames() {
    if (config.getTables() != null && !config.getTables().isEmpty()) {
      return Set.copyOf(config.getTables());
    }

    return Set.of(
        "SecurityAlert",
        "SecurityEvent",
        "CommonSecurityLog",
        "AuditLogs",
        "SigninLogs",
        "AADNonInteractiveUserSignInLogs",
        "AADServicePrincipalSignInLogs",
        "Heartbeat",
        "Syslog",
        "Event",
        "W3CIISLog",
        "WindowsFirewall",
        "NetworkMonitoring",
        "DNSEvents",
        "OfficeActivity"
    );
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
