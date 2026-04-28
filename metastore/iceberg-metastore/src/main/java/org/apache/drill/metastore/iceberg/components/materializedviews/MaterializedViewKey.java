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
package org.apache.drill.metastore.iceberg.components.materializedviews;

import org.apache.drill.metastore.MetastoreColumn;
import org.apache.drill.metastore.components.materializedviews.MaterializedViewMetadataUnit;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Materialized View key that identifies MV by storage plugin, workspace and MV name.
 * Used for grouping MV metadata when writing to Iceberg table.
 */
public class MaterializedViewKey {

  private final String storagePlugin;
  private final String workspace;
  private final String name;

  private MaterializedViewKey(String storagePlugin, String workspace, String name) {
    this.storagePlugin = storagePlugin;
    this.workspace = workspace;
    this.name = name;
  }

  public static MaterializedViewKey of(MaterializedViewMetadataUnit unit) {
    return new MaterializedViewKey(unit.storagePlugin(), unit.workspace(), unit.name());
  }

  public String storagePlugin() {
    return storagePlugin;
  }

  public String workspace() {
    return workspace;
  }

  public String name() {
    return name;
  }

  /**
   * Constructs location path for this MV key relative to the base table location.
   *
   * @param baseLocation base Iceberg table location
   * @return location path for this MV
   */
  public String toLocation(String baseLocation) {
    return new Path(new Path(new Path(baseLocation, storagePlugin), workspace), name).toUri().getPath();
  }

  /**
   * Converts this MV key to filter conditions map.
   *
   * @return map of filter conditions
   */
  public Map<MetastoreColumn, Object> toFilterConditions() {
    Map<MetastoreColumn, Object> conditions = new HashMap<>();
    conditions.put(MetastoreColumn.STORAGE_PLUGIN, storagePlugin);
    conditions.put(MetastoreColumn.WORKSPACE, workspace);
    conditions.put(MetastoreColumn.MV_NAME, name);
    return conditions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MaterializedViewKey that = (MaterializedViewKey) o;
    return Objects.equals(storagePlugin, that.storagePlugin)
        && Objects.equals(workspace, that.workspace)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storagePlugin, workspace, name);
  }

  @Override
  public String toString() {
    return "MaterializedViewKey{" +
        "storagePlugin='" + storagePlugin + '\'' +
        ", workspace='" + workspace + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
