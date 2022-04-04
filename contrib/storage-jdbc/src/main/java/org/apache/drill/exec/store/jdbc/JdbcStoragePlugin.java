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
import org.apache.drill.common.logical.AbstractSecuredStoragePluginConfig.AuthMode;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(JdbcStoragePlugin.class);

  private final JdbcStorageConfig config;
  // DataSources keyed on userName
  private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

  public JdbcStoragePlugin(JdbcStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    DataSource dataSource = getDataSource(config.getQueryUserCredentials());
    SqlDialect dialect = getDialect(dataSource);
    getJdbcDialect(dialect).registerSchemas(config, parent);
  }

  public DataSource getDataSource(UserCredentials userCredentials) {
    String connUserName = config.authMode == AuthMode.SHARED_USER
      ? ImpersonationUtil.getProcessUserName()
      : userCredentials.getUserName();

    return dataSources.computeIfAbsent(
      connUserName,
      ds -> initDataSource(this.config, userCredentials)
    );
  }

  public SqlDialect getDialect(DataSource dataSource) {
    return JdbcSchema.createDialect(
      SqlDialectFactoryImpl.INSTANCE,
      dataSource
    );
  }

  public JdbcDialect getJdbcDialect(SqlDialect dialect) {
    return JdbcDialectFactory.getJdbcDialect(this, dialect);
  }

  public DrillJdbcConvention getConvention(SqlDialect dialect) {
    return JdbcConventionFactory.getJdbcConvention(this, dialect);
  }

  @Override
  public JdbcStorageConfig getConfig() {
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
  public Set<RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext context) {
    UserCredentials userCreds = context.getContextInformation().getQueryUserCredentials();
    DataSource dataSource = getDataSource(userCreds);
    return getConvention(getDialect(dataSource)).getRules();
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
  static HikariDataSource initDataSource(JdbcStorageConfig config, UserCredentials userCredentials) {
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

      logger.debug("Initializing JDBC connection without username");
      UsernamePasswordCredentials credentials = config.getJdbcCredentials(userCredentials);
      hikariConfig.setUsername(credentials.getUsername());
      hikariConfig.setPassword(credentials.getPassword());


      /*
      The following serves as a hint to the driver, which *might* enable database
      optimizations.  Unfortunately some JDBC drivers without read-only support,
      notably Snowflake's, fail to connect outright when this option is set even
      though it is only a hint, so enabling it is generally problematic.
      */
      // hikariConfig.setReadOnly(!config.isWritable());

      return new HikariDataSource(hikariConfig);
    } catch (RuntimeException e) {
      throw UserException.connectionError(e)
        .message("Unable to configure data source: %s", e.getMessage())
        .build(logger);
    }
  }
}
