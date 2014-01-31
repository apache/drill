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
package org.apache.drill.sql.client.full;

import java.nio.charset.Charset;
import java.util.*;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.MethodCallExpression;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.drill.exec.store.hive.HiveStorageEngineConfig;
import org.apache.drill.jdbc.DrillTable;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.SqlTypeName;

public class HiveDatabaseSchema implements Schema{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveDatabaseSchema.class);

  private final JavaTypeFactory typeFactory;
  private final HiveSchema parentSchema;
  private final String name;
  private final Expression expression;
  private final QueryProvider queryProvider;
  private final SchemaProvider schemaProvider;
  private final DrillClient client;
  private final HiveStorageEngineConfig config;

  public HiveDatabaseSchema(DrillClient client, HiveStorageEngineConfig config, SchemaProvider schemaProvider,
                    JavaTypeFactory typeFactory, HiveSchema parentSchema, String name,
                    Expression expression, QueryProvider queryProvider) {
    super();
    this.client = client;
    this.typeFactory = typeFactory;
    this.parentSchema = parentSchema;
    this.name = name;
    this.expression = expression;
    this.queryProvider = queryProvider;
    this.schemaProvider = schemaProvider;
    this.config = config;
  }

  @Override
  public Schema getSubSchema(String name) {
    return null;
  }

  @Override
  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  @Override
  public Schema getParentSchema() {
    return parentSchema;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  @Override
  public QueryProvider getQueryProvider() {
    return queryProvider;
  }

  // TODO: Need to integrates UDFs?
  @Override
  public Collection<TableFunctionInSchema> getTableFunctions(String name) {
    return Collections.EMPTY_LIST;
  }

  // TODO: Need to integrates UDFs?
  @Override
  public Multimap<String, TableFunctionInSchema> getTableFunctions() {
    return ArrayListMultimap.create();
  }

  @Override
  /**
   * No more sub schemas within a database schema
   */
  public Collection<String> getSubSchemaNames() {
    return Collections.EMPTY_LIST;
  }

  static Map<PrimitiveObjectInspector.PrimitiveCategory, SqlTypeName> mapPrimHive2Sql = new HashMap<>();

  private RelDataType getRelDataTypeFromHivePrimitiveType(PrimitiveObjectInspector poi) {
    switch(poi.getPrimitiveCategory()) {
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case BYTE:
        return typeFactory.createSqlType(SqlTypeName.TINYINT);

      case SHORT:
        return typeFactory.createSqlType(SqlTypeName.SMALLINT);

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
        return typeFactory.createSqlType(SqlTypeName.BINARY);

      case DECIMAL:
        return typeFactory.createSqlType(SqlTypeName.DECIMAL);

      case STRING:
      case VARCHAR: {
        return typeFactory.createTypeWithCharsetAndCollation(
          typeFactory.createSqlType(SqlTypeName.VARCHAR), /*input type*/
          Charset.forName("UTF-16"), /*unicode char set*/
          SqlCollation.IMPLICIT /* TODO: need to decide if implicit is the correct one */
        );
      }

      case UNKNOWN:
      case VOID:
      default:
        throw new RuntimeException("Unknown or unsupported hive type");
    }
  }

  private RelDataType getRelDataTypeFromHiveType(ObjectInspector oi) {
    switch(oi.getCategory()) {
      case PRIMITIVE:
        return getRelDataTypeFromHivePrimitiveType(((PrimitiveObjectInspector) oi));
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        throw new RuntimeException("Unknown or unsupported hive type");
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <E> Table<E> getTable(String name, Class<E> elementType) {
    try {
      org.apache.hadoop.hive.ql.metadata.Table hiveTable =
        parentSchema.getHiveDb().getTable(getName(), name, false /*throwException*/);

      if (hiveTable == null) {
        logger.debug("Table name {} is invalid", name);
        return null;
      }

      Object selection = schemaProvider.getSelectionBaseOnName(name);
      if(selection == null) return null;

      final MethodCallExpression call = Expressions.call(getExpression(), //
        BuiltinMethod.DATA_CONTEXT_GET_TABLE.method, //
        Expressions.constant(name), //
        Expressions.constant(Object.class));

      ArrayList<RelDataType> typeList = new ArrayList<>();
      ArrayList<String> fieldNameList = new ArrayList<>();

      ArrayList<StructField> hiveFields = hiveTable.getFields();
      for(StructField hiveField : hiveFields) {
        fieldNameList.add(hiveField.getFieldName());
        typeList.add(getRelDataTypeFromHiveType(hiveField.getFieldObjectInspector()));
      }

      final RelDataType rowType = typeFactory.createStructType(typeList, fieldNameList);
      return (Table<E>) new DrillTable(
        client,
        this,
        Object.class,
        call,
        rowType,
        name,
        null /*storageEngineName*/,
        selection,
        config /*storageEngineConfig*/);
    } catch (HiveException ex) {
      logger.error("getTable failed", ex);
      return null;
    }
  }

  @Override
  public Map<String, TableInSchema> getTables() {
    Map<String, TableInSchema> tables = Maps.newHashMap();

    try {
      List<String> dbTables = parentSchema.getHiveDb().getAllTables(name);
      for(String table : dbTables) {
        TableInfo tableInfo = new TableInfo(this, table);
        tables.put(tableInfo.name, tableInfo);
      }
    } catch (HiveException ex) {
      throw new RuntimeException("Failed to get tables from HiveMetaStore", ex);
    }

    return tables;
  }

  private class TableInfo extends TableInSchema{

    public TableInfo(HiveDatabaseSchema schema, String tableName) {
      super(schema, schema.getName() + "." + tableName, TableType.TABLE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <E> Table<E> getTable(Class<E> elementType) {
      if( !elementType.isAssignableFrom(DrillTable.class)) throw new UnsupportedOperationException();
      return null;
    }
  }
}
