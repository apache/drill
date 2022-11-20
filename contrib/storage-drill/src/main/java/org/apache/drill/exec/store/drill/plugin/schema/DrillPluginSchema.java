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
package org.apache.drill.exec.store.drill.plugin.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.drill.plugin.DrillScanSpec;
import org.apache.drill.exec.store.drill.plugin.DrillStoragePlugin;
import org.apache.drill.exec.store.drill.plugin.DrillStoragePluginConfig;
import org.apache.drill.exec.store.plan.rel.PluginDrillTable;
import org.apache.drill.exec.util.ImpersonationUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DrillPluginSchema extends AbstractSchema {

  private final DrillStoragePlugin plugin;

  private final Map<String, DrillPluginSchema> schemaMap = new HashMap<>();

  private final Map<String, DrillTable> drillTables = new HashMap<>();
  private final String userName;

  public DrillPluginSchema(DrillStoragePlugin plugin, String name, String userName) {
    super(Collections.emptyList(), name);
    this.plugin = plugin;
    this.userName = Optional.ofNullable(userName).orElse(ImpersonationUtil.getProcessUserName());

    getSchemasList().stream()
      .map(UserProtos.SchemaMetadata::getSchemaName)
      .map(String::toLowerCase)
      .map(SchemaPath::parseFromString)
      .forEach(this::addSubSchema);
  }

  private DrillPluginSchema(DrillStoragePlugin plugin, List<String> parentSchemaPath, String name, String userName) {
    super(parentSchemaPath, name);
    this.plugin = plugin;
    this.userName = userName;
  }

  private void addSubSchema(SchemaPath path) {
    DrillPluginSchema drillSchema = new DrillPluginSchema(plugin, getSchemaPath(), path.getRootSegmentPath(), userName);
    schemaMap.put(path.getRootSegmentPath(), drillSchema);
    while (!path.isLeaf()) {
      path = new SchemaPath(path.getRootSegment().getChild().getNameSegment());
      DrillPluginSchema child = new DrillPluginSchema(plugin, drillSchema.getSchemaPath(), path.getRootSegmentPath(), userName);
      drillSchema.schemaMap.put(path.getRootSegmentPath(), child);
      drillSchema = child;
    }
  }

  private List<UserProtos.SchemaMetadata> getSchemasList() {
    try {
      return plugin.getClient(userName).getSchemas(null, null)
        .get().getSchemasList();
    } catch (InterruptedException | ExecutionException e) {
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public AbstractSchema getSubSchema(String name) {
    return schemaMap.get(name);
  }

  void setHolder(SchemaPlus plusOfThis) {
    getSubSchemaNames().forEach(s -> plusOfThis.add(s, getSubSchema(s)));
  }

  @Override
  public boolean showInInformationSchema() {
    return true;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return schemaMap.keySet();
  }

  @Override
  public Table getTable(String name) {
    return drillTables.computeIfAbsent(name,
      key -> {
        DrillScanSpec drillScanSpec = new DrillScanSpec(getSchemaPath(), key);
        return new PluginDrillTable(plugin, getName(), null, drillScanSpec, plugin.convention());
      });
  }

  @Override
  public String getTypeName() {
    return DrillStoragePluginConfig.NAME;
  }

  @Override
  public Set<String> getTableNames() {
    try {
      List<String> schemaPaths = getSchemaPath();
      String schemaPath = SchemaPath.getCompoundPath(
        schemaPaths.subList(1, schemaPaths.size()).toArray(new String[0])).getAsUnescapedPath();
      UserProtos.LikeFilter schemaNameFilter = UserProtos.LikeFilter.newBuilder()
        .setPattern(schemaPath)
        .build();
      return plugin.getClient(userName).getTables(null, schemaNameFilter, null, null)
        .get().getTablesList().stream()
        .map(UserProtos.TableMetadata::getTableName)
        .collect(Collectors.toSet());
    } catch (InterruptedException | ExecutionException e) {
      throw new DrillRuntimeException(e);
    }
  }
}
