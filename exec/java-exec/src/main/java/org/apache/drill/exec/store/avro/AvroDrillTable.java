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
package org.apache.drill.exec.store.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Lists;

public class AvroDrillTable extends DrillTable {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroDrillTable.class);
  private DataFileReader<GenericContainer> reader = null;

  public AvroDrillTable(String storageEngineName,
                       FileSystemPlugin plugin,
                       String userName,
                       FormatSelection selection) {
    super(storageEngineName, plugin, userName, selection);
    List<String> asFiles = selection.getAsFiles();
    Path path = new Path(asFiles.get(0));
    try {
      reader = new DataFileReader<>(new FsInput(path, plugin.getFsConf()), new GenericDatumReader<GenericContainer>());
    } catch (IOException e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> typeList = Lists.newArrayList();
    List<String> fieldNameList = Lists.newArrayList();

    Schema schema = reader.getSchema();
    for (Field field : schema.getFields()) {
      fieldNameList.add(field.name());
      typeList.add(getNullableRelDataTypeFromAvroType(typeFactory, field.schema()));
    }

    return typeFactory.createStructType(typeList, fieldNameList);
  }

  private RelDataType getNullableRelDataTypeFromAvroType(
      RelDataTypeFactory typeFactory, Schema fieldSchema) {
    RelDataType relDataType = null;
    switch (fieldSchema.getType()) {
    case ARRAY:
      RelDataType eleType = getNullableRelDataTypeFromAvroType(typeFactory, fieldSchema.getElementType());
      relDataType = typeFactory.createArrayType(eleType, -1);
      break;
    case BOOLEAN:
      relDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      break;
    case BYTES:
      relDataType = typeFactory.createSqlType(SqlTypeName.BINARY);
      break;
    case DOUBLE:
      relDataType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
      break;
    case FIXED:
      logger.error("{} type not supported", fieldSchema.getType());
      throw UserException.unsupportedError().message("FIXED type not supported yet").build(logger);
    case FLOAT:
      relDataType = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case INT:
      relDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      break;
    case LONG:
      relDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      break;
    case MAP:
      RelDataType valueType = getNullableRelDataTypeFromAvroType(typeFactory, fieldSchema.getValueType());
      RelDataType keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      relDataType = typeFactory.createMapType(keyType, valueType);
      break;
    case NULL:
      relDataType = typeFactory.createSqlType(SqlTypeName.NULL);
      break;
    case RECORD:
//      List<String> fieldNameList = Lists.newArrayList();
//      List<RelDataType> fieldRelDataTypeList = Lists.newArrayList();
//      for(Field field : fieldSchema.getFields()) {
//        fieldNameList.add(field.name());
//        fieldRelDataTypeList.add(getNullableRelDataTypeFromAvroType(typeFactory, field.schema()));
//      }
//      relDataType = typeFactory.createStructType(fieldRelDataTypeList, fieldNameList);

      //TODO This has to be mapped to struct type but because of calcite issue,
      //for now mapping it to map type.
      keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      valueType = typeFactory.createSqlType(SqlTypeName.ANY);
      relDataType = typeFactory.createMapType(keyType, valueType);
      break;
    case ENUM:
    case STRING:
      relDataType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      break;
    case UNION:
      RelDataType optinalType = getNullableRelDataTypeFromAvroType(typeFactory, fieldSchema.getTypes().get(1));
      relDataType = typeFactory.createTypeWithNullability(optinalType, true);
      break;
    }
    return relDataType;
  }
}