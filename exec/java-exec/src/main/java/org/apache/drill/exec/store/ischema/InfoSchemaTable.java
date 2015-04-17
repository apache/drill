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

import java.util.List;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.*;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Base class for tables in INFORMATION_SCHEMA.  Defines the table (fields and
 * types).
 */
public abstract class InfoSchemaTable {

  public static class Field {
    public String name;
    public MajorType type;

    public static Field create(String name, MajorType type) {
      Field field = new Field();
      field.name = name;
      field.type = type;
      return field;
    }
  }

  public static final MajorType VARCHAR = Types.required(MinorType.VARCHAR);
  public static final MajorType INT = Types.required(MinorType.INT);

  private final String tableName;
  private final List<Field> fields;

  public InfoSchemaTable(String tableName, List<Field> fields) {
    this.tableName = tableName;
    this.fields = fields;
  }

  static public RelDataType getRelDataType(RelDataTypeFactory typeFactory, MajorType type) {
    switch (type.getMinorType()) {
    case INT:
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case VARCHAR:
      // Note:  Remember to not default to "VARCHAR(1)":
      return typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
    default:
      throw new UnsupportedOperationException(
          "Only INT and VARCHAR types are supported in INFORMATION_SCHEMA");
    }
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {

    // Convert the array of Drill types to an array of Optiq types
    List<RelDataType> relTypes = Lists.newArrayList();
    List<String> fieldNames = Lists.newArrayList();
    for (Field field : fields) {
      relTypes.add(getRelDataType(typeFactory, field.type));
      fieldNames.add(field.name);
    }

    return typeFactory.createStructType(relTypes, fieldNames);
  }

  public abstract RecordGenerator getRecordGenerator();

  /** Layout for the CATALOGS table. */
  static public class Catalogs extends InfoSchemaTable {
    private static final List<Field> fields = ImmutableList.of(
        Field.create(CATS_COL_CATALOG_NAME, VARCHAR),
        Field.create(CATS_COL_CATALOG_DESCRIPTION, VARCHAR),
        Field.create(CATS_COL_CATALOG_CONNECT, VARCHAR));

    Catalogs() {
      super(TAB_CATALOGS, fields);
    }

    @Override
    public RecordGenerator getRecordGenerator() {
      return new RecordGenerator.Catalogs();
    }
  }

  /** Layout for the SCHEMATA table. */
  public static class Schemata extends InfoSchemaTable {
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SCHS_COL_CATALOG_NAME, VARCHAR),
        Field.create(SCHS_COL_SCHEMA_NAME, VARCHAR),
        Field.create(SCHS_COL_SCHEMA_OWNER, VARCHAR),
        Field.create(SCHS_COL_TYPE, VARCHAR),
        Field.create(SCHS_COL_IS_MUTABLE, VARCHAR));

    public Schemata() {
      super(TAB_SCHEMATA, fields);
    }

    @Override
    public RecordGenerator getRecordGenerator() {
      return new RecordGenerator.Schemata();
    }
  }

  /** Layout for the TABLES table. */
  public static class Tables extends InfoSchemaTable {
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(TBLS_COL_TABLE_TYPE, VARCHAR));

    public Tables() {
      super(TAB_TABLES, fields);
    }

    @Override
    public RecordGenerator getRecordGenerator() {
      return new RecordGenerator.Tables();
    }
  }

  /** Layout for the VIEWS table. */
  static public class Views extends InfoSchemaTable {
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(VIEWS_COL_VIEW_DEFINITION, VARCHAR));

    public Views() {
      super(TAB_VIEWS, fields);
    }

    @Override
    public RecordGenerator getRecordGenerator() {
      return new RecordGenerator.Views();
    }
  }

  /** Layout for the COLUMNS table. */
  public static class Columns extends InfoSchemaTable {
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(COLS_COL_COLUMN_NAME, VARCHAR),
        Field.create(COLS_COL_ORDINAL_POSITION, INT),
        Field.create(COLS_COL_IS_NULLABLE, VARCHAR),
        Field.create(COLS_COL_DATA_TYPE, VARCHAR),
        Field.create(COLS_COL_CHARACTER_MAXIMUM_LENGTH, INT),
        Field.create(COLS_COL_NUMERIC_PRECISION_RADIX, INT),
        Field.create(COLS_COL_NUMERIC_SCALE, INT),
        Field.create(COLS_COL_NUMERIC_PRECISION, INT));

    public Columns() {
      super(TAB_COLUMNS, fields);
    }

    @Override
    public RecordGenerator getRecordGenerator() {
      return new RecordGenerator.Columns();
    }
  }
}
