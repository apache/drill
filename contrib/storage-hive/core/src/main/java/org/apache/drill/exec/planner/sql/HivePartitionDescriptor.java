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

import org.apache.drill.exec.planner.PartitionDescriptor;
import org.apache.drill.exec.store.hive.HiveTable;

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
}
