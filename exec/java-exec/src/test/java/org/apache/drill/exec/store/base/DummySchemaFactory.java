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
package org.apache.drill.exec.store.base;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class DummySchemaFactory extends AbstractSchemaFactory {

  public static final String MY_TABLE = "myTable";
  public static final String ALL_TYPES_TABLE = "allTypes";

  private final DummyStoragePlugin plugin;

  public DummySchemaFactory(DummyStoragePlugin plugin) {
    super(plugin.getName());
    this.plugin = plugin;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    parent.add(getName(), new DefaultSchema(getName()));
  }

  class DefaultSchema extends AbstractSchema {

    private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

    DefaultSchema(String name) {
      super(Collections.emptyList(), name);
    }

    @Override
    public Table getTable(String name) {
      DynamicDrillTable table = activeTables.get(name);
      if (table != null) {
        return table;
      }
      if (getTableNames().contains(name)) {
        DummyScanSpec scanSpec = new DummyScanSpec(BaseStoragePlugin.DEFAULT_SCHEMA_NAME, name);
        return registerTable(name,
            new DynamicDrillTable(plugin, plugin.getName(), scanSpec));
      }
      return null; // Unknown table
    }

    private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
      activeTables.put(name, table);
      return table;
    }

    @Override
    public Set<String> getTableNames() {
      return Sets.newHashSet(MY_TABLE, ALL_TYPES_TABLE);
    }

    @Override
    public String getTypeName() {
      return DummyStoragePluginConfig.NAME;
    }
  }
}
