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
package org.apache.drill.exec.store.hive;

import java.util.List;

import org.apache.calcite.schema.Schema.TableType;

import org.apache.drill.exec.store.hive.HiveTable.HivePartition;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

public class HiveReadEntry {

  @JsonProperty("table")
  public HiveTable table;
  @JsonProperty("partitions")
  public List<HiveTable.HivePartition> partitions;

  @JsonIgnore
  private List<Partition> partitionsUnwrapped = Lists.newArrayList();

  @JsonCreator
  public HiveReadEntry(@JsonProperty("table") HiveTable table,
                       @JsonProperty("partitions") List<HiveTable.HivePartition> partitions) {
    this.table = table;
    this.partitions = partitions;
    if (partitions != null) {
      for(HiveTable.HivePartition part : partitions) {
        partitionsUnwrapped.add(part.getPartition());
      }
    }
  }

  @JsonIgnore
  public Table getTable() {
    return table.getTable();
  }

  @JsonIgnore
  public List<Partition> getPartitions() {
    return partitionsUnwrapped;
  }

  @JsonIgnore
  public HiveTable getHiveTableWrapper() {
    return table;
  }

  @JsonIgnore
  public List<HivePartition> getHivePartitionWrappers() {
    return partitions;
  }

  @JsonIgnore
  public TableType getJdbcTableType() {
    if (table.getTable().getTableType().equals(org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW.toString())) {
      return TableType.VIEW;
    }

    return TableType.TABLE;
  }

  public String getPartitionLocation(HiveTable.HivePartition partition) {
    String partitionPath = table.getTable().getSd().getLocation();

    for (String value: partition.values) {
      partitionPath += "/" + value;
    }

    return partitionPath;
  }
}

