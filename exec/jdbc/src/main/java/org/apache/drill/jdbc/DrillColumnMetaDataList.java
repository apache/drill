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
import net.hydromatic.avatica.ColumnMetaData.AvaticaType;
import net.hydromatic.avatica.ColumnMetaData.Rep;

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
          f.getAsSchemaPath().getRootSegment().getPath(), // label
          f.getAsSchemaPath().getRootSegment().getPath(), // columnname
          schemaName, // schemaname
          t.hasPrecision() ? t.getPrecision() : 0, // precision
          t.hasScale() ? t.getScale() : 0, // scale
          null, // tablename is null so sqlline doesn't try to retrieve primary keys.
          catalogName, // catalogname
          getAvaticaType(t),  // sql type
          true, // readonly
          false, // writable
          false, // definitely writable
          "none" // column class name
          );
      columns[i] =col;
    }
  }

  private static AvaticaType getAvaticaType(MajorType t){
    int sqlTypeId = Types.getSqlType(t);
    return ColumnMetaData.scalar(sqlTypeId, getSqlTypeName(sqlTypeId), Rep.BOOLEAN /* dummy value, unused */);
  }

  private static String getSqlTypeName(int type) {
    switch (type) {
    case java.sql.Types.BIT:
        return "BIT";
    case java.sql.Types.TINYINT:
        return "TINYINT";
    case java.sql.Types.SMALLINT:
        return "SMALLINT";
    case java.sql.Types.INTEGER:
        return "INTEGER";
    case java.sql.Types.BIGINT:
        return "BIGINT";
    case java.sql.Types.FLOAT:
        return "FLOAT";
    case java.sql.Types.REAL:
        return "REAL";
    case java.sql.Types.DOUBLE:
        return "DOUBLE";
    case java.sql.Types.NUMERIC:
        return "NUMERIC";
    case java.sql.Types.DECIMAL:
        return "DECIMAL";
    case java.sql.Types.CHAR:
        return "CHAR";
    case java.sql.Types.VARCHAR:
        return "VARCHAR";
    case java.sql.Types.LONGVARCHAR:
        return "LONGVARCHAR";
    case java.sql.Types.DATE:
        return "DATE";
    case java.sql.Types.TIME:
        return "TIME";
    case java.sql.Types.TIMESTAMP:
        return "TIMESTAMP";
    case java.sql.Types.BINARY:
        return "BINARY";
    case java.sql.Types.VARBINARY:
        return "VARBINARY";
    case java.sql.Types.LONGVARBINARY:
        return "LONGVARBINARY";
    case java.sql.Types.NULL:
        return "NULL";
    case java.sql.Types.OTHER:
        return "OTHER";
    case java.sql.Types.JAVA_OBJECT:
        return "JAVA_OBJECT";
    case java.sql.Types.DISTINCT:
        return "DISTINCT";
    case java.sql.Types.STRUCT:
        return "STRUCT";
    case java.sql.Types.ARRAY:
        return "ARRAY";
    case java.sql.Types.BLOB:
        return "BLOB";
    case java.sql.Types.CLOB:
        return "CLOB";
    case java.sql.Types.REF:
        return "REF";
    case java.sql.Types.DATALINK:
        return "DATALINK";
    case java.sql.Types.BOOLEAN:
        return "BOOLEAN";
    case java.sql.Types.ROWID:
        return "ROWID";
    case java.sql.Types.NCHAR:
        return "NCHAR";
    case java.sql.Types.NVARCHAR:
        return "NVARCHAR";
    case java.sql.Types.LONGNVARCHAR:
        return "LONGNVARCHAR";
    case java.sql.Types.NCLOB:
        return "NCLOB";
    case java.sql.Types.SQLXML:
        return "SQLXML";
    }

    return "?";
}
}
