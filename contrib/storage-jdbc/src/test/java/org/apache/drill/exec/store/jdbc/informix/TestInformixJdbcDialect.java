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
package org.apache.drill.exec.store.jdbc.informix;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.util.Optional;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.drill.exec.store.jdbc.JdbcDialect;
import org.apache.drill.exec.store.jdbc.JdbcDialectFactory;
import org.apache.drill.exec.store.jdbc.JdbcStorageConfig;
import org.apache.drill.exec.store.jdbc.JdbcStoragePlugin;
import org.junit.Test;

public class TestInformixJdbcDialect {

  @Test
  public void dropsCatalogFromTargetTable() throws Exception {
    JdbcTable jdbcTable = newJdbcTable("sample_database", null, "sample_table");
    TableScan scan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(relOptTable);
    when(relOptTable.maybeUnwrap(JdbcTable.class)).thenReturn(Optional.of(jdbcTable));

    String sql = InformixJdbcImplementor.getSqlTargetTable(scan)
      .toSqlString(InformixSqlDialect.DEFAULT)
      .getSql();

    assertEquals("sample_table", sql);
  }

  @Test
  public void keepsSchemaWhenPresent() throws Exception {
    JdbcTable jdbcTable = newJdbcTable("sample_database", "sample_owner", "sample_table");
    TableScan scan = mock(TableScan.class);
    RelOptTable relOptTable = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(relOptTable);
    when(relOptTable.maybeUnwrap(JdbcTable.class)).thenReturn(Optional.of(jdbcTable));

    String sql = InformixJdbcImplementor.getSqlTargetTable(scan)
      .toSqlString(InformixSqlDialect.DEFAULT)
      .getSql();

    assertEquals("sample_owner.sample_table", sql);
  }

  @Test
  public void factorySelectsInformixDialect() {
    JdbcStoragePlugin plugin = mock(JdbcStoragePlugin.class);
    when(plugin.getConfig()).thenReturn(newJdbcStorageConfig("jdbc:other://localhost"));

    JdbcDialect dialect = new JdbcDialectFactory().getJdbcDialect(plugin, InformixSqlDialect.DEFAULT);
    assertTrue(dialect instanceof InformixJdbcDialect);
  }

  @Test
  public void factorySelectsInformixDialectFromUrl() {
    JdbcStoragePlugin plugin = mock(JdbcStoragePlugin.class);
    when(plugin.getConfig()).thenReturn(
      newJdbcStorageConfig("jdbc:informix-sqli://localhost:1526/sample_database"));

    JdbcDialect dialect = new JdbcDialectFactory().getJdbcDialect(
      plugin, SqlDialect.DatabaseProduct.UNKNOWN.getDialect());
    assertTrue(dialect instanceof InformixJdbcDialect);
  }

  private static JdbcStorageConfig newJdbcStorageConfig(String url) {
    JdbcStorageConfig config = new JdbcStorageConfig(
      "com.informix.jdbc.IfxDriver",
      url,
      null,
      null,
      true,
      false,
      null,
      null,
      null,
      0
    );
    return config;
  }

  private static JdbcTable newJdbcTable(String catalog, String schema, String table) throws Exception {
    DataSource dataSource = mock(DataSource.class);
    JdbcSchema jdbcSchema = new JdbcSchema(dataSource, InformixSqlDialect.DEFAULT, null, catalog, schema);
    Constructor<JdbcTable> ctor = JdbcTable.class.getDeclaredConstructor(
      JdbcSchema.class,
      String.class,
      String.class,
      String.class,
      Schema.TableType.class
    );
    ctor.setAccessible(true);
    return ctor.newInstance(jdbcSchema, catalog, schema, table, Schema.TableType.TABLE);
  }
}
