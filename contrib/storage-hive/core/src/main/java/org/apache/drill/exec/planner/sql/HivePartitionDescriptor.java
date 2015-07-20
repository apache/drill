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
package org.apache.drill.exec.planner.sql;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.planner.PartitionLocation;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.hive.HiveTable;
import org.apache.drill.exec.vector.ValueVector;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Partition descriptor for hive tables
public class HivePartitionDescriptor implements PartitionDescriptor {

  private final Map<String, Integer> partitionMap = new HashMap<>();
  private final int MAX_NESTED_SUBDIRS;

  public HivePartitionDescriptor(List<HiveTable.FieldSchemaWrapper> partitionName) {
    int i = 0;
    for (HiveTable.FieldSchemaWrapper wrapper : partitionName) {
      partitionMap.put(wrapper.name, i);
      i++;
    }
    MAX_NESTED_SUBDIRS = i;
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    return partitionMap.get(partitionName);
  }

  @Override
  public boolean isPartitionName(String name) {
    return (partitionMap.get(name) != null);
  }

  @Override
  public int getMaxHierarchyLevel() {
    return MAX_NESTED_SUBDIRS;
  }

  @Override
  public Integer getIdIfValid(String name) {
    return partitionMap.get(name);
  }

  /*
   * Following method stubs are just added to satisfy the interface implementation.
   * Actual implementation will be added when hive partition pruning is plugged in
   * as part of DRILL-3121
   */
  private String getBaseTableLocation() {
    return null;
  }

  @Override
  public GroupScan createNewGroupScan(List<String> newFiles) throws Exception {
    return null;
  }

  @Override
  public List<PartitionLocation> getPartitions() {
    return null;
  }

  @Override
  public void populatePartitionVectors(ValueVector[] vectors, List<PartitionLocation> partitions, BitSet partitionColumnBitSet, Map<Integer, String> fieldNameMap) {

  }

  @Override
  public TypeProtos.MajorType getVectorType(SchemaPath column, PlannerSettings plannerSettings) {
    return null;
  }
}
