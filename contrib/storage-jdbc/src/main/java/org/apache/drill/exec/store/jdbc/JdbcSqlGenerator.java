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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.exec.store.SubsetRemover;
import org.apache.drill.exec.store.jdbc.clickhouse.ClickhouseJdbcImplementor;

import static ru.yandex.clickhouse.ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX;

public class JdbcSqlGenerator {
  /**
   * Generate sql for different jdbc databases
   */
  public static String generateSql(JdbcStoragePlugin plugin,
                                   RelOptCluster cluster, RelNode input) {
    final SqlDialect dialect = plugin.getDialect();
    final JdbcImplementor jdbcImplementor;
    if (plugin.getConfig().getUrl().startsWith(JDBC_CLICKHOUSE_PREFIX)) {
      jdbcImplementor = new ClickhouseJdbcImplementor(dialect,
        (JavaTypeFactory) cluster.getTypeFactory());
    } else {
      jdbcImplementor = new JdbcImplementor(dialect,
        (JavaTypeFactory) cluster.getTypeFactory());
    }
    final JdbcImplementor.Result result = jdbcImplementor.visitChild(0,
      input.accept(SubsetRemover.INSTANCE));
    return result.asStatement().toSqlString(dialect).getSql();
  }
}
