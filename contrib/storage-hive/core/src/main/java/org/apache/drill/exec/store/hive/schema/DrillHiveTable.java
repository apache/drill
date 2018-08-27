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
package org.apache.drill.exec.store.hive.schema;

import org.apache.calcite.util.Util;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.hive.HiveReadEntry;
import org.apache.drill.exec.store.hive.HiveStoragePlugin;
import org.apache.drill.exec.store.hive.HiveTableWithColumnCache;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

public class DrillHiveTable extends DrillTable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillHiveTable.class);

  protected final HiveTableWithColumnCache hiveTable;

  public DrillHiveTable(String storageEngineName, HiveStoragePlugin plugin, String userName, HiveReadEntry readEntry) {
    super(storageEngineName, plugin, userName, readEntry);
    this.hiveTable = new HiveTableWithColumnCache(readEntry.getTable());
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<RelDataType> typeList = Lists.newArrayList();
    List<String> fieldNameList = Lists.newArrayList();

    List<FieldSchema> hiveFields = hiveTable.getColumnListsCache().getColumns(0);
    for(FieldSchema hiveField : hiveFields) {
      fieldNameList.add(hiveField.getName());
      typeList.add(getNullableRelDataTypeFromHiveType(
          typeFactory, TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType())));
    }

    for (FieldSchema field : hiveTable.getPartitionKeys()) {
      fieldNameList.add(field.getName());
      typeList.add(getNullableRelDataTypeFromHiveType(
          typeFactory, TypeInfoUtils.getTypeInfoFromTypeString(field.getType())));
    }

    return typeFactory.createStructType(typeList, fieldNameList);
  }

  private RelDataType getNullableRelDataTypeFromHiveType(RelDataTypeFactory typeFactory, TypeInfo typeInfo) {
    RelDataType relDataType = getRelDataTypeFromHiveType(typeFactory, typeInfo);
    return typeFactory.createTypeWithNullability(relDataType, true);
  }

  private RelDataType getRelDataTypeFromHivePrimitiveType(RelDataTypeFactory typeFactory, PrimitiveTypeInfo pTypeInfo) {
    switch(pTypeInfo.getPrimitiveCategory()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);

      case BYTE:
      case SHORT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);

      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);

      case LONG:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);

      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);

      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);

      case DATE:
        return typeFactory.createSqlType(SqlTypeName.DATE);

      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

      case BINARY:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);

      case DECIMAL: {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo)pTypeInfo;
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, decimalTypeInfo.precision(), decimalTypeInfo.scale());
      }

      case STRING:
      case VARCHAR: {
        int maxLen = TypeInfoUtils.getCharacterLengthForType(pTypeInfo);
        return typeFactory.createTypeWithCharsetAndCollation(
          typeFactory.createSqlType(SqlTypeName.VARCHAR, maxLen), /*input type*/
          Util.getDefaultCharset(),
          SqlCollation.IMPLICIT /* TODO: need to decide if implicit is the correct one */
        );
      }

      case CHAR: {
        int maxLen = TypeInfoUtils.getCharacterLengthForType(pTypeInfo);
        return typeFactory.createTypeWithCharsetAndCollation(
          typeFactory.createSqlType(SqlTypeName.CHAR, maxLen), /*input type*/
          Util.getDefaultCharset(),
          SqlCollation.IMPLICIT
        );
      }

      case UNKNOWN:
      case VOID:
      default:
        throwUnsupportedHiveDataTypeError(pTypeInfo.getPrimitiveCategory().toString());
    }

    return null;
  }

  private RelDataType getRelDataTypeFromHiveType(RelDataTypeFactory typeFactory, TypeInfo typeInfo) {
    switch(typeInfo.getCategory()) {
      case PRIMITIVE:
        return getRelDataTypeFromHivePrimitiveType(typeFactory, ((PrimitiveTypeInfo) typeInfo));

      case LIST: {
        ListTypeInfo listTypeInfo = (ListTypeInfo)typeInfo;
        RelDataType listElemTypeInfo = getRelDataTypeFromHiveType(typeFactory, listTypeInfo.getListElementTypeInfo());
        return typeFactory.createArrayType(listElemTypeInfo, -1);
      }

      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo)typeInfo;
        RelDataType keyType = getRelDataTypeFromHiveType(typeFactory, mapTypeInfo.getMapKeyTypeInfo());
        RelDataType valueType = getRelDataTypeFromHiveType(typeFactory, mapTypeInfo.getMapValueTypeInfo());
        return typeFactory.createMapType(keyType, valueType);
      }

      case STRUCT: {
        StructTypeInfo structTypeInfo = (StructTypeInfo)typeInfo;
        ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        ArrayList<TypeInfo> fieldHiveTypeInfoList = structTypeInfo.getAllStructFieldTypeInfos();
        List<RelDataType> fieldRelDataTypeList = Lists.newArrayList();
        for(TypeInfo fieldHiveType : fieldHiveTypeInfoList) {
          fieldRelDataTypeList.add(getRelDataTypeFromHiveType(typeFactory, fieldHiveType));
        }
        return typeFactory.createStructType(fieldRelDataTypeList, fieldNames);
      }

      case UNION:
        logger.warn("There is no UNION data type in SQL. Converting it to Sql type OTHER to avoid " +
            "breaking INFORMATION_SCHEMA queries");
        return typeFactory.createSqlType(SqlTypeName.OTHER);
    }

    throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    return null;
  }

  private void throwUnsupportedHiveDataTypeError(String hiveType) {
    StringBuilder errMsg = new StringBuilder();
    errMsg.append(String.format("Unsupported Hive data type %s. ", hiveType));
    errMsg.append(System.getProperty("line.separator"));
    errMsg.append("Following Hive data types are supported in Drill INFORMATION_SCHEMA: ");
    errMsg.append("BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, BINARY, DECIMAL, STRING, " +
        "VARCHAR, CHAR, LIST, MAP, STRUCT and UNION");

    throw new RuntimeException(errMsg.toString());
  }
}
