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
package org.apache.drill.exec.store.ischema;

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class InfoSchemaDrillTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaDrillTable.class);

  private final SelectedTable table;

  public InfoSchemaDrillTable(InfoSchemaStoragePlugin plugin, String storageEngineName, SelectedTable selection, StoragePluginConfig storageEngineConfig) {
    super(storageEngineName, plugin, selection);
    this.table = selection;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return table.getRowType(typeFactory);
  }

}
