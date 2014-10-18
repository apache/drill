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

// Interface used to describe partitions. Currently used by file system based partitions and hive partitions
public interface PartitionDescriptor {

  /* Get the hierarchy index of the given partition
   * For eg: if we have the partition laid out as follows
   * 1997/q1/jan
   *
   * then getPartitionHierarchyIndex("jan") => 2
   */
  public int getPartitionHierarchyIndex(String partitionName);

  // Given a column name return boolean to indicate if its a partition column or not
  public boolean isPartitionName(String name);

  // Maximum level of partition nesting/ hierarchy supported
  public int getMaxHierarchyLevel();
}
