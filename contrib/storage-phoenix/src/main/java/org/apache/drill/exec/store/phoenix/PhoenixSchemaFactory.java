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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

public class PhoenixSchemaFactory extends AbstractSchemaFactory {

  private final PhoenixStoragePlugin plugin;
  private final Map<String, PhoenixSchema> schemaMap;
  private PhoenixSchema rootSchema;

  public PhoenixSchemaFactory(PhoenixStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
    this.schemaMap = Maps.newHashMap();
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    rootSchema = new PhoenixSchema(plugin, Collections.emptyList(), plugin.getName());
    locateSchemas();
    parent.add(getName(), rootSchema); // resolve the top-level schema.
    for (String schemaName : rootSchema.getSubSchemaNames()) {
      PhoenixSchema schema = (PhoenixSchema) rootSchema.getSubSchema(schemaName);
      parent.add(schemaName, schema); // provide all available schemas for calcite.
    }
  }

  private void locateSchemas() {
    DataSource ds = plugin.getDataSource();
    try (Connection conn = ds.getConnection();
          ResultSet rs = ds.getConnection().getMetaData().getSchemas()) {
      while (rs.next()) {
        final String schemaName = rs.getString(1); // lookup the schema (or called database).
        PhoenixSchema schema = new PhoenixSchema(plugin, Arrays.asList(getName()), schemaName);
        schemaMap.put(schemaName, schema);
      }
      rootSchema.addSchemas(schemaMap);
    } catch (SQLException e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }

  protected static class PhoenixSchema extends AbstractSchema {

    private final JdbcSchema jdbcSchema;
    private final Map<String, PhoenixSchema> schemaMap = CaseInsensitiveMap.newHashMap();

    public PhoenixSchema(PhoenixStoragePlugin plugin, List<String> parentSchemaPath, String schemaName) {
      super(parentSchemaPath, schemaName);
      this.jdbcSchema = new JdbcSchema(plugin.getDataSource(), plugin.getDialect(), plugin.getConvention(), null, schemaName);
    }

    @Override
    public Schema getSubSchema(String name) {
      return schemaMap.get(name);
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemaMap.keySet();
    }

    @Override
    public Table getTable(String name) {
      Table table = jdbcSchema.getTable(StringUtils.upperCase(name));
      return table;
    }

    @Override
    public Set<String> getTableNames() {
      Set<String> tables = jdbcSchema.getTableNames();
      return tables;
    }

    @Override
    public String getTypeName() {
      return PhoenixStoragePluginConfig.NAME;
    }

    public void addSchemas(Map<String, PhoenixSchema> schemas) {
      schemaMap.putAll(schemas);
    }

    @Override
    public boolean areTableNamesCaseSensitive() {
      return false;
    }
  }
}
