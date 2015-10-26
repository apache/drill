/**
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
package org.apache.drill.exec.store.sys;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.pojo.PojoDataType;
import org.apache.drill.exec.store.SchemaConfig;

/**
 * A "storage" plugin for system tables.
 */
public class SystemTablePlugin extends AbstractStoragePlugin {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTablePlugin.class);

  public static final String SYS_SCHEMA_NAME = "sys";

  private final DrillbitContext context;
  private final String name;
  private final SystemTablePluginConfig config;
  private final SystemSchema schema = new SystemSchema();

  public SystemTablePlugin(SystemTablePluginConfig config, DrillbitContext context, String name) {
    this.config = config;
    this.context = context;
    this.name = name;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return config;
  }

  @JsonIgnore
  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    parent.add(schema.getName(), schema);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns)
      throws IOException {
    SystemTable table = selection.getWith(context.getLpPersistence(), SystemTable.class);
    return new SystemTableScan(table, this);
  }

  /**
   * This class defines a namespace for {@link org.apache.drill.exec.store.sys.SystemTable}s
   */
  private class SystemSchema extends AbstractSchema {

    private final Set<String> tableNames;

    public SystemSchema() {
      super(ImmutableList.<String>of(), SYS_SCHEMA_NAME);
      Set<String> names = Sets.newHashSet();
      for (SystemTable t : SystemTable.values()) {
        names.add(t.getTableName());
      }
      this.tableNames = ImmutableSet.copyOf(names);
    }

    @Override
    public Set<String> getTableNames() {
      return tableNames;
    }

    @Override
    public DrillTable getTable(String name) {
      for (SystemTable table : SystemTable.values()) {
        if (table.getTableName().equalsIgnoreCase(name)) {
          return new StaticDrillTable(SystemTablePlugin.this.name, SystemTablePlugin.this, table,
            new PojoDataType(table.getPojoClass()));
        }
      }
      return null;
    }

    @Override
    public String getTypeName() {
      return SystemTablePluginConfig.NAME;
    }

  }
}
