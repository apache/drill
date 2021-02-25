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
package org.apache.drill.exec.store.phoenix;

import java.io.IOException;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.phoenix.rules.PhoenixConvention;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import com.fasterxml.jackson.core.type.TypeReference;

public class PhoenixStoragePlugin extends AbstractStoragePlugin {

  private final PhoenixStoragePluginConfig config;
  private final DataSource dataSource;
  private final SqlDialect dialect;
  private final PhoenixConvention convention;
  private final PhoenixSchemaFactory schemaFactory;

  public PhoenixStoragePlugin(PhoenixStoragePluginConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    this.dataSource = initNoPoolingDataSource(config);
    this.dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource);
    this.convention = new PhoenixConvention(dialect, name, this);
    this.schemaFactory = new PhoenixSchemaFactory(this);
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public SqlDialect getDialect() {
    return dialect;
  }

  public PhoenixConvention getConvention() {
    return convention;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    return convention.getRules();
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    PhoenixScanSpec scanSpec = selection.getListWith(context.getLpPersistence().getMapper(), new TypeReference<PhoenixScanSpec>() {});
    return new PhoenixGroupScan(scanSpec, this);
  }

  private static DataSource initNoPoolingDataSource(PhoenixStoragePluginConfig config) {
    // Don't use the pool with the connection
    PhoenixDataSource dataSource = null;
    if (StringUtils.isNotBlank(config.getJdbcURL())) {
      dataSource = new PhoenixDataSource(config.getJdbcURL(), config.getProps()); // the props is initiated.
    } else {
      dataSource = new PhoenixDataSource(config.getHost(), config.getPort(), config.getProps());
    }
    if (config.getUsername() != null && config.getPassword() != null) {
      if (dataSource.getConnectionProperties() == null) {
        dataSource.setConnectionProperties(Maps.newHashMap());
      }
      dataSource.getConnectionProperties().put("user", config.getUsername());
      dataSource.getConnectionProperties().put("password", config.getPassword());
    }
    return dataSource;
  }
}
