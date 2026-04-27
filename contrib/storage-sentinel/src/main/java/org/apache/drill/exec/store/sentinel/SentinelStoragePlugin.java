package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.StoragePluginRulesSupplier;
import org.apache.drill.exec.store.plan.rel.PluginRel;
import org.apache.drill.exec.store.PluginRulesProviderImpl;
import org.apache.drill.exec.store.sentinel.plan.SentinelPluginImplementor;
import org.apache.drill.exec.store.sentinel.auth.SentinelTokenManager;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        config.getCredentialsProvider());
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
