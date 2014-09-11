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
package org.apache.drill.exec.planner.logical;

import java.io.IOException;

import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.Table;

import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;

public abstract class DrillTable implements Table {

  private final String storageEngineName;
  public final StoragePluginConfig storageEngineConfig;
  private Object selection;
  private StoragePlugin plugin;
  private GroupScan scan;

  /** Creates a DrillTable. */
  public DrillTable(String storageEngineName, StoragePlugin plugin, Object selection) {
    this.selection = selection;
    this.plugin = plugin;

    this.storageEngineConfig = plugin.getConfig();
    this.storageEngineName = storageEngineName;
  }

  public GroupScan getGroupScan() throws IOException{
    if (scan == null) {
      this.scan = plugin.getPhysicalScan(new JSONOptions(selection));
    }
    return scan;
  }

  public StoragePluginConfig getStorageEngineConfig() {
    return storageEngineConfig;
  }

  public StoragePlugin getPlugin() {
    return plugin;
  }

  public Object getSelection() {
    return selection;
  }

  public String getStorageEngineName() {
    return storageEngineName;
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
    return new DrillScanRel(context.getCluster(),
        context.getCluster().traitSetOf(DrillRel.DRILL_LOGICAL),
        table);
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((selection == null) ? 0 : selection.hashCode());
    result = prime * result + ((storageEngineConfig == null) ? 0 : storageEngineConfig.hashCode());
    result = prime * result + ((storageEngineName == null) ? 0 : storageEngineName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DrillTable other = (DrillTable) obj;
    if (selection == null) {
      if (other.selection != null) {
        return false;
      }
    } else if (!selection.equals(other.selection)) {
      return false;
    }
    if (storageEngineConfig == null) {
      if (other.storageEngineConfig != null) {
        return false;
      }
    } else if (!storageEngineConfig.equals(other.storageEngineConfig)) {
      return false;
    }
    if (storageEngineName == null) {
      if (other.storageEngineName != null) {
        return false;
      }
    } else if (!storageEngineName.equals(other.storageEngineName)) {
      return false;
    }
    return true;
  }

}
