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
package org.apache.drill.exec.store.cassandra;

import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.cassandra.DrillCassandraSchemaFactory.DrillCassandraSchema;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassandraDatabaseSchema extends AbstractSchema {
  private static final Logger logger = LoggerFactory.getLogger(CassandraDatabaseSchema.class);

  private final DrillCassandraSchema drillCassandraSchema;
  private final CassandraStoragePlugin plugin;
  private final Set<String> tables;
  private final String pluginName;
  private final String keyspaceName;

  private final Map<String, DynamicDrillTable> activeTables = new HashMap<>();

  public CassandraDatabaseSchema(CassandraStoragePlugin plugin, List<String> tableList, DrillCassandraSchema drillCassandraSchema, String name) {
    super(drillCassandraSchema.getSchemaPath(), name);
    this.drillCassandraSchema = drillCassandraSchema;
    this.tables = Sets.newHashSet(tableList);
    this.plugin = plugin;
    this.keyspaceName = name;
    this.pluginName = plugin.getName();
  }

  @Override
  public Table getTable(String tableName) {
    DynamicDrillTable table = activeTables.get(tableName);
    if (table != null) {
      return table;
    } else {
      return registerTable(tableName, new DynamicDrillTable(plugin, pluginName, new CassandraScanSpec(keyspaceName, tableName, plugin.getConfig())));
    }
  }

  @Override
  public Set<String> getTableNames() {
    return activeTables.keySet();
  }

  private DynamicDrillTable registerTable(String name, DynamicDrillTable table) {
    activeTables.put(name, table);
    return table;
  }

  @Override
  public String getTypeName() {
    return CassandraStoragePluginConfig.NAME;
  }
}
