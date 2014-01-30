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
package org.apache.drill.jdbc;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

import net.hydromatic.avatica.ColumnMetaData;

public class DrillColumnMetaDataList extends BasicList<ColumnMetaData>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillColumnMetaDataList.class);

  private ColumnMetaData[] columns = new ColumnMetaData[0];
  
  @Override
  public int size() {
    return columns.length;
  }
  
  @Override
  public ColumnMetaData get(int index) {
    return columns[index];
  }
  
  public void updateColumnMetaData(String catalogName, String schemaName, String tableName, BatchSchema schema){

    columns = new ColumnMetaData[schema.getFieldCount()];
    for(int i = 0; i < schema.getFieldCount(); i++){
      MaterializedField f = schema.getColumn(i);
      MajorType t = f.getType();
      ColumnMetaData col = new ColumnMetaData( //
          i, // ordinal
          false, // autoIncrement
          true, // caseSensitive
          false, // searchable
          false, // currency
          f.getDataMode() == DataMode.OPTIONAL ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls, //nullability 
          !Types.isUnSigned(t), // signed
          10, // display size.
          f.getName(), // label
          f.getName(), // columnname
          schemaName, // schemaname
          t.hasPrecision() ? t.getPrecision() : 0, // precision
          t.hasScale() ? t.getScale() : 0, // scale
          null, // tablename is null so sqlline doesn't try to retrieve primary keys.
          catalogName, // catalogname
          Types.getSqlType(t),  // sql type
          t.getMinorType().name()+":" + t.getMode().name(), // typename
          true, // readonly
          false, // writable
          false, // definitely writable
          "none", // column class name
          ColumnMetaData.Rep.BOOLEAN // Dummy value for representation as it doesn't apply in drill's case.
          );
      columns[i] =col;
    }
  }
  
}
