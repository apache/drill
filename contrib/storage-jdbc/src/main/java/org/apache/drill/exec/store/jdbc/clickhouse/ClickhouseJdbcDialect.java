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
package org.apache.drill.exec.store.jdbc.clickhouse;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SubsetRemover;
import org.apache.drill.exec.store.jdbc.DrillJdbcConvention;
import org.apache.drill.exec.store.jdbc.JdbcDialect;
import org.apache.drill.exec.store.jdbc.JdbcStoragePlugin;

import java.util.Optional;

import javax.sql.DataSource;

public class ClickhouseJdbcDialect implements JdbcDialect {

  private final JdbcStoragePlugin plugin;
  private final SqlDialect dialect;

  public ClickhouseJdbcDialect(JdbcStoragePlugin plugin, SqlDialect dialect) {
    this.plugin = plugin;
    this.dialect = dialect;
  }

  @Override
  public void registerSchemas(SchemaConfig config, SchemaPlus parent) {
    UserCredentials userCreds = config.getQueryUserCredentials();
    Optional<DataSource> dataSource = plugin.getDataSource(userCreds);
    if (!dataSource.isPresent()) {
      return;
    }
    DrillJdbcConvention convention = plugin.getConvention(dialect, config.getQueryUserCredentials());

    ClickhouseCatalogSchema schema = new ClickhouseCatalogSchema(
      plugin.getName(),
      dataSource.get(),
      dialect,
      convention
    );
    SchemaPlus holder = parent.add(plugin.getName(), schema);
    schema.setHolder(holder);
  }

  @Override
  public String generateSql(RelOptCluster cluster, RelNode input) {
    final JdbcImplementor jdbcImplementor = new ClickhouseJdbcImplementor(dialect,
      (JavaTypeFactory) cluster.getTypeFactory());
    final JdbcImplementor.Result result = jdbcImplementor.visitRoot(
      input.accept(SubsetRemover.INSTANCE));
    return result.asStatement().toSqlString(dialect).getSql();
  }
}
