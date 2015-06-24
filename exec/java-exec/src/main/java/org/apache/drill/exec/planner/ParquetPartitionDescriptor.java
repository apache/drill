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

import com.google.common.collect.Maps;
import org.apache.drill.common.expression.SchemaPath;

import java.util.List;
import java.util.Map;


/**
 * PartitionDescriptor that describes partitions based on column names instead of directory structure
 */
public class ParquetPartitionDescriptor implements PartitionDescriptor {

  private final List<SchemaPath> partitionColumns;

  public ParquetPartitionDescriptor(List<SchemaPath> partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  @Override
  public int getPartitionHierarchyIndex(String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionName(String name) {
    return partitionColumns.contains(name);
  }

  @Override
  public Integer getIdIfValid(String name) {
    SchemaPath schemaPath = SchemaPath.getSimplePath(name);
    int id = partitionColumns.indexOf(schemaPath);
    if (id == -1) {
      return null;
    }
    return id;
  }

  @Override
  public int getMaxHierarchyLevel() {
    return partitionColumns.size();
  }
}
