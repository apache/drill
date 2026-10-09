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

package org.apache.drill.exec.store.sentinel;

import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.plan.rel.PluginDrillTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SentinelSchema extends AbstractSchema {
  private final SentinelStoragePlugin plugin;
  private final String schemaName;
  private final String workspaceId;
  private final Map<String, Table> tableCache;

  public SentinelSchema(SentinelStoragePlugin plugin, String schemaName) {
    this(plugin, schemaName, null);
  }

  public SentinelSchema(SentinelStoragePlugin plugin, String schemaName, String workspaceId) {
    super(Collections.emptyList(), schemaName);
    this.plugin = plugin;
    this.schemaName = schemaName;
    this.workspaceId = workspaceId;
    this.tableCache = new HashMap<>();
  }

  @Override
  public Table getTable(String name) {
    if (tableCache.containsKey(name)) {
      return tableCache.get(name);
    }

    SentinelScanSpec scanSpec = new SentinelScanSpec(
        plugin.getName(),
        name,
        name);

    PluginDrillTable table = new PluginDrillTable(
        plugin,
        plugin.getName(),
        null,
        scanSpec,
        plugin.getConvention());

    tableCache.put(name, table);
    return table;
  }

  @Override
  public Set<String> getTableNames() {
    return Set.copyOf(plugin.getTableNames());
  }

  @Override
  public String getTypeName() {
    return "sentinel";
  }
}
