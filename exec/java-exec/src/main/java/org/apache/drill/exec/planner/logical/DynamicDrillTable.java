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

import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.drill.exec.store.StoragePlugin;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class DynamicDrillTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamicDrillTable.class);

  private RelDataTypeHolder holder = new RelDataTypeHolder();

  public DynamicDrillTable(StoragePlugin plugin, String storageEngineName, Object selection) {
    super(storageEngineName, plugin, selection);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return new RelDataTypeDrillImpl(holder, typeFactory);
  }
}
