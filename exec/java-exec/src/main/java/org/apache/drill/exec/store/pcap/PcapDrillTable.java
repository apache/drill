/*
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
package org.apache.drill.exec.store.pcap;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.drill.exec.store.pcap.dto.ColumnDto;
import org.apache.drill.exec.store.pcap.schema.PcapTypes;
import org.apache.drill.exec.store.pcap.schema.Schema;

import java.util.List;

public class PcapDrillTable extends DrillTable {

  private final Schema schema;


  public PcapDrillTable(String storageEngineName, FileSystemPlugin plugin, String userName, FormatSelection selection) {
    super(storageEngineName, plugin, userName, selection);
    this.schema = new Schema();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> typeList = Lists.newArrayList();
    List<String> fieldNameList = Lists.newArrayList();
    convertToRelDataType(typeFactory, fieldNameList, typeList);
    return typeFactory.createStructType(typeList, fieldNameList);
  }

  private RelDataType getSqlTypeFromPcapType(RelDataTypeFactory typeFactory, PcapTypes type) {
    switch (type) {
      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case INTEGER:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      default:
        throw new UnsupportedOperationException("Unsupported type.");
    }
  }

  private void convertToRelDataType(RelDataTypeFactory typeFactory, List<String> names, List<RelDataType> types) {
    for (ColumnDto column : schema.getColumns()) {
      names.add(column.getColumnName());
      RelDataType type = getSqlTypeFromPcapType(typeFactory, column.getColumnType());
      type = typeFactory.createTypeWithNullability(type, column.isNullable());
      types.add(type);
    }
  }
}
