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
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.StorageStrategy;
import org.apache.drill.exec.store.jdbc.utils.JdbcQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CapitalizingJdbcSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(CapitalizingJdbcSchema.class);

  private final Map<String, CapitalizingJdbcSchema> schemaMap;
  private final JdbcSchema inner;
  private final boolean caseSensitive;
  private final JdbcStoragePlugin plugin;

  public CapitalizingJdbcSchema(List<String> parentSchemaPath, String name,
                          DataSource dataSource,
                         SqlDialect dialect, JdbcConvention convention, String catalog, String schema, boolean caseSensitive, JdbcStoragePlugin plugin) {
    super(parentSchemaPath, name);
    this.schemaMap = new HashMap<>();
    this.inner = new JdbcSchema(dataSource, dialect, convention, catalog, schema);
    this.caseSensitive = caseSensitive;
    this.plugin = plugin;
  }

  @Override
  public String getTypeName() {
    return JdbcStorageConfig.NAME;
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return inner.getFunctions(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return inner.getFunctionNames();
  }

  @Override
  public CapitalizingJdbcSchema getSubSchema(String name) {
    return schemaMap.get(name);
  }

  void setHolder(SchemaPlus plusOfThis) {
    for (String s : getSubSchemaNames()) {
      CapitalizingJdbcSchema inner = getSubSchema(s);
      SchemaPlus holder = plusOfThis.add(s, inner);
      inner.setHolder(holder);
    }
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return schemaMap.keySet();
  }

  @Override
  public Set<String> getTableNames() {
    if (isCatalogSchema()) {
      return Collections.emptySet();
    }
    return inner.getTableNames();
  }


  @Override
  public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns, StorageStrategy strategy) {
    return new CreateTableEntry() {

      @Override
      public Writer getWriter(PhysicalOperator child) throws IOException {
        String tableWithSchema = "";
        try {
          tableWithSchema = JdbcQueryBuilder.buildCompleteTableName(plugin.getDataSource().getConnection(), tableName);
        } catch (SQLException e) {
          tableWithSchema = tableName;
        }
        return new JdbcWriter(child, tableWithSchema, inner, plugin);
      }

      @Override
      public List<String> getPartitionColumns() {
        return Collections.emptyList();
      }
    };
  }

  @Override
  public void dropTable(String tableName) {
    // TODO Test this
    String dropTableQuery = String.format("DROP TABLE %s", tableName);
    try {
      Connection conn = inner.getDataSource().getConnection();
      Statement stmt = conn.createStatement();

      logger.debug("Executing drop table query: {}", dropTableQuery);
      boolean successfullyDropped = stmt.execute(dropTableQuery);

      if (!successfullyDropped) {
        throw UserException.dataWriteError()
          .message("Error while dropping table " + tableName)
          .build(logger);
      }
    } catch (SQLException e) {
      throw UserException.dataWriteError(e)
        .message("Failure while trying to drop table '%s'.", tableName)
        .addContext("plugin", name)
        .build(logger);
    }
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public Table getTable(String name) {
    if (isCatalogSchema()) {
      logger.warn("Failed attempt to find table '{}' in catalog schema '{}'", name, getName());
      return null;
    }
    Table table = inner.getTable(name);
    if (table == null && !areTableNamesCaseSensitive()) {
      // Oracle and H2 changes unquoted identifiers to uppercase.
      table = inner.getTable(name.toUpperCase());
      if (table == null) {
        // Postgres changes unquoted identifiers to lowercase.
        table = inner.getTable(name.toLowerCase());
      }
    }
    return table;
  }

  @Override
  public boolean areTableNamesCaseSensitive() {
    return caseSensitive;
  }

  @Override
  public CapitalizingJdbcSchema getDefaultSchema() {
    return isCatalogSchema()
        ? schemaMap.values().iterator().next().getDefaultSchema()
        : this;
  }

  private boolean isCatalogSchema() {
    return !schemaMap.isEmpty();
  }

  void addSubSchema(CapitalizingJdbcSchema subSchema) {
    schemaMap.put(subSchema.getName(), subSchema);
  }
}
