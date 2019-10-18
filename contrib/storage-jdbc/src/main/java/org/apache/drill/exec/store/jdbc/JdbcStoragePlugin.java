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
import java.util.Set;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

public class JdbcStoragePlugin extends AbstractStoragePlugin {

  private final JdbcStorageConfig config;
  private final DataSource source;
  private final SqlDialect dialect;
  private final DrillJdbcConvention convention;

  public JdbcStoragePlugin(JdbcStorageConfig config, DrillbitContext context, String name) {
    super(context, name);
    this.config = config;
    BasicDataSource source = new BasicDataSource();
    source.setDriverClassName(config.getDriver());
    source.setUrl(config.getUrl());

    if (config.getUsername() != null) {
      source.setUsername(config.getUsername());
    }

    if (config.getPassword() != null) {
      source.setPassword(config.getPassword());
    }

    this.source = source;
    this.dialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, source);
    this.convention = new DrillJdbcConvention(dialect, name, this);
  }


  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    JdbcCatalogSchema schema = new JdbcCatalogSchema(getName(), source, dialect, convention,
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

  public DataSource getSource() {
    return source;
  }

  public SqlDialect getDialect() {
    return dialect;
  }

  @Override
  public Set<RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext context) {
    return convention.getRules();
  }
}
