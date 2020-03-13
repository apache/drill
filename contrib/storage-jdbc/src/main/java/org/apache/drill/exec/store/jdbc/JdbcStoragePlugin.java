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

import javax.sql.DataSource;
import java.util.Properties;
import java.util.Set;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

  private static final Logger logger = LoggerFactory.getLogger(JdbcStoragePlugin.class);

  private final JdbcStorageConfig config;
  private final HikariDataSource dataSource;
  private final SqlDialect dialect;
  private final DrillJdbcConvention convention;

  public JdbcStoragePlugin(JdbcStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    this.dataSource = initDataSource(config);
    this.dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
    this.convention = new DrillJdbcConvention(dialect, name, this);
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    JdbcCatalogSchema schema = new JdbcCatalogSchema(getName(), dataSource, dialect, convention,
        !this.config.areTableNamesCaseInsensitive());
    SchemaPlus holder = parent.add(getName(), schema);
    schema.setHolder(holder);
  }

  @Override
  public JdbcStorageConfig getConfig() {
    return config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public SqlDialect getDialect() {
    return dialect;
  }

  @Override
  public Set<RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext context) {
    return convention.getRules();
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(dataSource);
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
  static HikariDataSource initDataSource(JdbcStorageConfig config) {
    try {
      Properties properties = new Properties();
      properties.putAll(config.getSourceParameters());

      HikariConfig hikariConfig = new HikariConfig(properties);

      hikariConfig.setDriverClassName(config.getDriver());
      hikariConfig.setJdbcUrl(config.getUrl());
      hikariConfig.setUsername(config.getUsername());
      hikariConfig.setPassword(config.getPassword());

      return new HikariDataSource(hikariConfig);
    } catch (RuntimeException e) {
      throw UserException.connectionError(e)
        .message("Unable to configure data source: %s", e.getMessage())
        .build(logger);
    }
  }
}
