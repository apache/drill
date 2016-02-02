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
package org.apache.drill.exec.store.shp;

import java.io.IOException;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.hadoop.fs.Path;
import org.jamel.dbf.DbfReader;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;

import com.google.common.collect.Lists;

public class ShpDrillTable extends DrillTable {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShpDrillTable.class);

  private DbfReader reader = null;

  public ShpDrillTable(DrillFileSystem dfs,
                       String storageEngineName,
                       FileSystemPlugin plugin,
                       String userName,
                       FormatSelection selection) {
    super(storageEngineName, plugin, userName, selection);
    List<String> asFiles = selection.getAsFiles();
    Path path = new Path(asFiles.get(0).replace("shp", "dbf"));

    try {
      reader = new DbfReader(dfs.open(path));
    } catch (IOException e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    DbfHeader dbfHeader = reader.getHeader();

    List<RelDataType> typeList = Lists.newArrayList();
    List<String> fieldNameList = Lists.newArrayList();

    typeList.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
    typeList.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
    typeList.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
    typeList.add(typeFactory.createSqlType(SqlTypeName.VARBINARY));

    fieldNameList.add("gid");
    fieldNameList.add("srid");
    fieldNameList.add("shapeType");
    fieldNameList.add("geom");

    for(int i = 0; i < dbfHeader.getFieldsCount(); i++){
      DbfField field = dbfHeader.getField(i);
      fieldNameList.add(field.getName());

      typeList.add(getSqlType(typeFactory, field));
    }

    return typeFactory.createStructType(typeList, fieldNameList);
  }

  private RelDataType getSqlType(RelDataTypeFactory typeFactory, DbfField field) {
    RelDataType sqlType;
    switch(field.getDataType()){
    case CHAR:
      sqlType = typeFactory.createSqlType(SqlTypeName.VARCHAR, field.getFieldLength());
      break;
    case FLOAT:
      sqlType = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case DATE:
      sqlType = typeFactory.createSqlType(SqlTypeName.DATE);
      break;
    case LOGICAL:
      sqlType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      break;
    case NUMERIC:
      if(field.getDecimalCount() == 0){
        sqlType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      } else {
        sqlType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
      }
      break;
    default:
      sqlType = typeFactory.createSqlType(SqlTypeName.NULL);
      break;
    }
    return sqlType;
  }
}