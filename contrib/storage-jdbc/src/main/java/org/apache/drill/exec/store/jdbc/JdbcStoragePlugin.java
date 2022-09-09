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
package org.apache.drill.exec.store.jdbc;

import java.util.Properties;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

  static final Logger logger = LoggerFactory.getLogger(JdbcStoragePlugin.class);

  private final JdbcStorageConfig jdbcStorageConfig;
  private final JdbcDialectFactory dialectFactory;
  private final JdbcConventionFactory conventionFactory;
  // DataSources for this storage config keyed on JDBC username
  private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

  public JdbcStoragePlugin(JdbcStorageConfig jdbcStorageConfig, DrillbitContext context, String name) {
    super(context, name);
    this.jdbcStorageConfig = jdbcStorageConfig;
    this.dialectFactory = new JdbcDialectFactory();
    this.conventionFactory = new JdbcConventionFactory();
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    UserCredentials userCreds = config.getQueryUserCredentials();
    Optional<DataSource> dataSource = getDataSource(userCreds);
    if (!dataSource.isPresent()) {
      logger.debug(
        "No schemas will be registered in {} for query user {}.",
        getName(),
        config.getUserName()
      );
      return;
    }

    SqlDialect dialect = getDialect(dataSource.get());
    getJdbcDialect(dialect).registerSchemas(config, parent);
  }

  public Optional<DataSource> getDataSource(UserCredentials userCredentials) {
    Optional<UsernamePasswordCredentials> jdbcCreds = jdbcStorageConfig.getUsernamePasswordCredentials(userCredentials);

    if (!jdbcCreds.isPresent() && jdbcStorageConfig.getAuthMode() == AuthMode.USER_TRANSLATION) {
      logger.info(
        "There are no {} mode credentials in {} for query user {}, will not attempt to connect.",
        AuthMode.USER_TRANSLATION,
        getName(),
        userCredentials.getUserName()
      );
      return Optional.empty();
    }

    // Missing creds is valid under SHARED_USER (e.g. unsecured DBs, BigQuery's OAuth)
    // and we fall back to using a key of Drillbit process username in this instance.
    String dsKey = jdbcCreds.isPresent()
      ? jdbcCreds.get().getUsername()
      : ImpersonationUtil.getProcessUserName();

    return Optional.of(dataSources.computeIfAbsent(
      dsKey,
      ds -> initDataSource(this.jdbcStorageConfig, jdbcCreds.orElse(null))
    ));
  }

  public SqlDialect getDialect(DataSource dataSource) {
    return JdbcSchema.createDialect(
      SqlDialectFactoryImpl.INSTANCE,
      dataSource
    );
  }

  public JdbcDialect getJdbcDialect(SqlDialect dialect) {
    return dialectFactory.getJdbcDialect(this, dialect);
  }

  public DrillJdbcConvention getConvention(SqlDialect dialect, String username) {
    return conventionFactory.getJdbcConvention(this, dialect, username);
  }

  @Override
  public JdbcStorageConfig getConfig() {
    return jdbcStorageConfig;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return jdbcStorageConfig.isWritable();
  }

  @Override
  public boolean supportsInsert() {
    return jdbcStorageConfig.isWritable();
  }

  @Override
  public Set<? extends RelOptRule> getOptimizerRules(
    OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
      case LOGICAL:
      case PHYSICAL: {
        UserCredentials userCreds = optimizerContext.getContextInformation().getQueryUserCredentials();

        String userName = userCreds.getUserName();
        return getDataSource(userCreds)
          .map(dataSource -> getConvention(getDialect(dataSource), userName).getRules())
          .orElse(ImmutableSet.of());
      }
      case LOGICAL_PRUNE_AND_JOIN:
      case LOGICAL_PRUNE:
      case PARTITION_PRUNING:
      case JOIN_PLANNING:
      default:
        return ImmutableSet.of();
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(dataSources.values());
  }

  /**
   * Initializes {@link HikariDataSource} instance and configures it based on given
   * storage plugin configuration.
   * Basic parameters such as driver, url, user name and password are set using setters.
   * Other source parameters are set dynamically through the properties. See the list
   * of available Hikari properties: <a href="https://github.com/brettwooldridge/HikariCP">.
   *
   * @param config storage plugin config
   * @return Hikari data source instance
   * @throws UserException if unable to configure Hikari data source
   */
  @VisibleForTesting
  static HikariDataSource initDataSource(
    JdbcStorageConfig config,
    UsernamePasswordCredentials jdbcCredentials
  ) {
    try {
      Properties properties = new Properties();

      /*
        Set default HikariCP values which prefer to connect lazily to avoid overwhelming source
      systems with connections which mostly remain idle.  A data source that is present in N
      storage configs replicated over P drillbits with a HikariCP minimumIdle value of Q will
      have N×P×Q connections made to it eagerly.
        The trade off of lazier connections is increased latency after periods of inactivity in
      which the pool has emptied.  When comparing the defaults that follow with e.g. the
      HikariCP defaults, bear in mind that the context here is OLAP, not OLTP.  It is normal
      for queries to run for a long time and to be separated by long intermissions. Users who
      prefer eager to lazy connections remain free to overwrite the following defaults in their
      storage config.
      */

      // maximum amount of time that a connection is allowed to sit idle in the pool, 0 ⇒ forever
      properties.setProperty("idleTimeout", String.valueOf(TimeUnit.HOURS.toMillis(2)));
      // how frequently HikariCP will attempt to keep a connection alive, 0 ⇒ disabled
      properties.setProperty("keepaliveTime", String.valueOf(TimeUnit.MINUTES.toMillis(5)));
      // maximum lifetime of a connection in the pool, 0 ⇒ forever
      properties.setProperty("maxLifetime", String.valueOf(TimeUnit.HOURS.toMillis(12)));
      // minimum number of idle connections that HikariCP tries to maintain in the pool, 0 ⇒ none
      properties.setProperty("minimumIdle", "0");
      // maximum size that the pool is allowed to reach, including both idle and in-use connections
      properties.setProperty("maximumPoolSize", "10");

      // apply any HikariCP parameters the user may have set, overwriting defaults
      properties.putAll(config.getSourceParameters());

      HikariConfig hikariConfig = new HikariConfig(properties);

      hikariConfig.setDriverClassName(config.getDriver());
      hikariConfig.setJdbcUrl(config.getUrl());

      if (jdbcCredentials != null) {
        hikariConfig.setUsername(jdbcCredentials.getUsername());
        hikariConfig.setPassword(jdbcCredentials.getPassword());
      }

      /*
      The following serves as a hint to the driver, which *might* enable database
      optimizations.  Unfortunately some JDBC drivers without read-only support,
      notably Snowflake's, fail to connect outright when this option is set even
      though it is only a hint, so enabling it is generally problematic.

      The solution is to leave that option as null.
      */
      if (config.isWritable() != null) {
        hikariConfig.setReadOnly(!config.isWritable());
      }

      return new HikariDataSource(hikariConfig);
    } catch (RuntimeException e) {
      throw UserException.connectionError(e)
        .message("Unable to configure data source: %s", e.getMessage())
        .build(logger);
    }
  }
}
