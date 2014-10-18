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

// partition descriptor for file system based tables
public class FileSystemPartitionDescriptor implements PartitionDescriptor {

  static final int MAX_NESTED_SUBDIRS = 10;          // allow up to 10 nested sub-directories

  private final String partitionLabel;
  private final int partitionLabelLength;

  public FileSystemPartitionDescriptor(String partitionLabel) {
    this.partitionLabel = partitionLabel;
    this.partitionLabelLength = partitionLabel.length();
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    String suffix = partitionName.substring(partitionLabelLength); // get the numeric suffix from 'dir<N>'
    return Integer.parseInt(suffix);
  }

  @Override
  public boolean isPartitionName(String name) {
    return name.matches(partitionLabel +"[0-9]");
  }

  @Override
  public int getMaxHierarchyLevel() {
    return MAX_NESTED_SUBDIRS;
  }
}
