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
package org.apache.drill.exec.store.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.LogicalType;
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
import org.apache.drill.exec.planner.logical.ExtendableRelDataType;
import org.apache.drill.exec.planner.types.ExtendableRelDataTypeHolder;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatSelection;
import org.apache.hadoop.fs.Path;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class AvroDrillTable extends DrillTable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroDrillTable.class);

  private final DataFileReader<GenericContainer> reader;
  private final SchemaConfig schemaConfig;
  private ExtendableRelDataTypeHolder holder;

  public AvroDrillTable(String storageEngineName,
                       FileSystemPlugin plugin,
                       SchemaConfig schemaConfig,
                       FormatSelection selection) {
    super(storageEngineName, plugin, schemaConfig.getUserName(), selection);
    List<String> asFiles = selection.getAsFiles();
    Path path = new Path(asFiles.get(0));
    this.schemaConfig = schemaConfig;
    try {
      reader = new DataFileReader<>(new FsInput(path, plugin.getFsConf()), new GenericDatumReader<GenericContainer>());
    } catch (IOException e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // ExtendableRelDataTypeHolder is reused to preserve previously added implicit columns
    if (holder == null) {
      List<RelDataType> typeList = Lists.newArrayList();
      List<String> fieldNameList = Lists.newArrayList();

      // adds partition columns to RowType since they always present in star queries
      List<String> partitions =
          ColumnExplorer.getPartitionColumnNames(((FormatSelection) getSelection()).getSelection(), schemaConfig);
      for (String partitionName : partitions) {
        fieldNameList.add(partitionName);
        typeList.add(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true));
      }

      // adds non-partition table columns to RowType
      Schema schema = reader.getSchema();
      for (Field field : schema.getFields()) {
        fieldNameList.add(field.name());
        typeList.add(getNullableRelDataTypeFromAvroType(typeFactory, field.schema()));
      }

      holder = new ExtendableRelDataTypeHolder(
          typeFactory.createStructType(typeList, fieldNameList).getFieldList(),
          ColumnExplorer.getImplicitColumnsNames(schemaConfig));
    }

    return new ExtendableRelDataType(holder, typeFactory);
  }

  private RelDataType getNullableRelDataTypeFromAvroType(
      RelDataTypeFactory typeFactory, Schema fieldSchema) {
    LogicalType logicalType = fieldSchema.getLogicalType();
    String logicalTypeName = logicalType != null ? logicalType.getName() : "";
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
      switch (logicalTypeName) {
        case "decimal":
          relDataType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
          break;
        default:
          relDataType = typeFactory.createSqlType(SqlTypeName.BINARY);
      }
      break;
    case DOUBLE:
      relDataType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
      break;
    case FIXED:
      switch (logicalTypeName) {
        case "decimal":
          relDataType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
          break;
        default:
          logger.error("{} type not supported", fieldSchema.getType());
          throw UserException.unsupportedError().message("FIXED type not supported yet").build(logger);
      }
      break;
    case FLOAT:
      relDataType = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case INT:
      switch (logicalTypeName) {
        case "date":
          relDataType = typeFactory.createSqlType(SqlTypeName.DATE);
          break;
        case "time-millis":
          relDataType = typeFactory.createSqlType(SqlTypeName.TIME);
          break;
        default:
          relDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      }
      break;
    case LONG:
      switch (logicalTypeName) {
        case "date":
          relDataType = typeFactory.createSqlType(SqlTypeName.DATE);
          break;
        case "time-micros":
          relDataType = typeFactory.createSqlType(SqlTypeName.TIME);
          break;
        case "timestamp-millis":
        case "timestamp-micros":
          relDataType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
          break;
        default:
          relDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
      }
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
