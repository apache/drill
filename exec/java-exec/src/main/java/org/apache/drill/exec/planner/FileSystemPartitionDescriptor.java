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
package org.apache.drill.exec.planner;

import java.util.Map;

import com.google.common.collect.Maps;


// partition descriptor for file system based tables
public class FileSystemPartitionDescriptor implements PartitionDescriptor {

  static final int MAX_NESTED_SUBDIRS = 10;          // allow up to 10 nested sub-directories

  private final String partitionLabel;
  private final int partitionLabelLength;
  private final Map<String, Integer> partitions = Maps.newHashMap();

  public FileSystemPartitionDescriptor(String partitionLabel) {
    this.partitionLabel = partitionLabel;
    this.partitionLabelLength = partitionLabel.length();
    for(int i =0; i < 10; i++){
      partitions.put(partitionLabel + i, i);
    }
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    String suffix = partitionName.substring(partitionLabelLength); // get the numeric suffix from 'dir<N>'
    return Integer.parseInt(suffix);
  }

  @Override
  public boolean isPartitionName(String name) {
    return partitions.containsKey(name);
  }

  @Override
  public Integer getIdIfValid(String name) {
    return partitions.get(name);
  }

  @Override
  public int getMaxHierarchyLevel() {
    return MAX_NESTED_SUBDIRS;
  }

  public String getName(int index){
    return partitionLabel + index;
  }


}
