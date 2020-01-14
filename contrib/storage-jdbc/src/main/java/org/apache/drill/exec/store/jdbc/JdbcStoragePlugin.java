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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;
import javax.sql.DataSource;
import java.util.Set;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
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
  private final BasicDataSource dataSource;
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
   * Initializes {@link BasicDataSource} instance and configures it based on given
   * storage plugin configuration.
   * Basic parameters such as driver, url, user name and password are set using setters.
   * Other source parameters are set dynamically by invoking setter based on given parameter name.
   * If given parameter is absent, it will be ignored. If value is incorrect
   * (for example, String is passed instead of int), data source initialization will fail.
   * Parameter names should correspond to names available in documentation:
   * <a href="https://commons.apache.org/proper/commons-dbcp/configuration.html">.
   *
   * @param config storage plugin config
   * @return basic data source instance
   * @throws UserException if unable to set source parameter
   */
  @VisibleForTesting
  static BasicDataSource initDataSource(JdbcStorageConfig config) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName(config.getDriver());
    dataSource.setUrl(config.getUrl());
    dataSource.setUsername(config.getUsername());
    dataSource.setPassword(config.getPassword());

    MethodHandles.Lookup publicLookup = MethodHandles.publicLookup();
    for (Map.Entry<String, Object> entry : config.getSourceParameters().entrySet()) {
      try {
        Class<?> parameterType = dataSource.getClass().getDeclaredField(entry.getKey()).getType();
        MethodType methodType = MethodType.methodType(void.class, parameterType);
        MethodHandle methodHandle = publicLookup.findVirtual(dataSource.getClass(),
          "set" + StringUtils.capitalize(entry.getKey()), methodType);
        methodHandle.invokeWithArguments(dataSource, entry.getValue());
      } catch (ReflectiveOperationException e) {
        logger.warn("Unable to find / access setter for parameter {}: {}", entry.getKey(), e.getMessage());
      } catch (Throwable e) {
        throw UserException.connectionError()
          .message("Unable to set value %s for parameter %s", entry.getKey(), entry.getValue())
          .addContext("Error message:", e.getMessage())
          .build(logger);
      }
    }
    return dataSource;
  }
}
