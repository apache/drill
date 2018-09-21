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
package org.apache.drill.exec.store.ischema;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_CONNECT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_DESCRIPTION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_CHARACTER_MAXIMUM_LENGTH;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_CHARACTER_OCTET_LENGTH;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_COLUMN_DEFAULT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_COLUMN_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_DATA_TYPE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_DATETIME_PRECISION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_INTERVAL_PRECISION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_INTERVAL_TYPE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_IS_NULLABLE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_NUMERIC_PRECISION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_NUMERIC_PRECISION_RADIX;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_NUMERIC_SCALE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_ORDINAL_POSITION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_ACCESS_TIME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_FILE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_GROUP;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_IS_DIRECTORY;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_IS_FILE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_LENGTH;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_MODIFICATION_TIME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_OWNER;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_PERMISSION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_RELATIVE_PATH;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_ROOT_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_WORKSPACE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_CATALOG_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_IS_MUTABLE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_OWNER;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_TYPE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_CATALOG;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.TBLS_COL_TABLE_TYPE;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.VIEWS_COL_VIEW_DEFINITION;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.server.options.OptionManager;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

/**
 * Base class for tables in INFORMATION_SCHEMA.  Defines the table (fields and types).
 */
public abstract class InfoSchemaTable<S> {

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

  public static final MajorType INT = Types.required(MinorType.INT);
  public static final MajorType BIGINT = Types.required(MinorType.BIGINT);
  public static final MajorType VARCHAR = Types.required(MinorType.VARCHAR);
  public static final MajorType BIT = Types.required(MinorType.BIT);
  public static final MajorType TIMESTAMP = Types.required(MinorType.TIMESTAMP);

  private final List<Field> fields;

  public InfoSchemaTable(List<Field> fields) {
    this.fields = fields;
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

  private RelDataType getRelDataType(RelDataTypeFactory typeFactory, MajorType type) {
    switch (type.getMinorType()) {
      case INT:
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case BIGINT:
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case VARCHAR:
        // Note:  Remember to not default to "VARCHAR(1)":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
      case BIT:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case TIMESTAMP:
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      default:
        throw new UnsupportedOperationException("Only INT, BIGINT, VARCHAR, BOOLEAN and TIMESTAMP types are supported in " +
            InfoSchemaConstants.IS_SCHEMA_NAME);
    }
  }

  public abstract InfoSchemaRecordGenerator<S> getRecordGenerator(OptionManager optionManager);

  /** Layout for the CATALOGS table. */
  public static class Catalogs extends InfoSchemaTable<Records.Catalog> {
    // NOTE:  Nothing seems to verify that the types here (apparently used
    // by SQL validation) match the types of the fields in Records.Catalogs).
    private static final List<Field> fields = ImmutableList.of(
        Field.create(CATS_COL_CATALOG_NAME, VARCHAR),
        Field.create(CATS_COL_CATALOG_DESCRIPTION, VARCHAR),
        Field.create(CATS_COL_CATALOG_CONNECT, VARCHAR));

    public Catalogs() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.Catalog> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Catalogs(optionManager);
    }
  }

  /** Layout for the SCHEMATA table. */
  public static class Schemata extends InfoSchemaTable<Records.Schema> {
    // NOTE:  Nothing seems to verify that the types here (apparently used
    // by SQL validation) match the types of the fields in Records.Schemata).
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SCHS_COL_CATALOG_NAME, VARCHAR),
        Field.create(SCHS_COL_SCHEMA_NAME, VARCHAR),
        Field.create(SCHS_COL_SCHEMA_OWNER, VARCHAR),
        Field.create(SCHS_COL_TYPE, VARCHAR),
        Field.create(SCHS_COL_IS_MUTABLE, VARCHAR));

    public Schemata() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.Schema> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Schemata(optionManager);
    }
  }

  /** Layout for the TABLES table. */
  public static class Tables extends InfoSchemaTable<Records.Table> {
    // NOTE:  Nothing seems to verify that the types here (apparently used
    // by SQL validation) match the types of the fields in Records.Tables).
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(TBLS_COL_TABLE_TYPE, VARCHAR));

    public Tables() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.Table> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Tables(optionManager);
    }
  }

  /** Layout for the VIEWS table. */
  public static class Views extends InfoSchemaTable<Records.View> {
    // NOTE:  Nothing seems to verify that the types here (apparently used
    // by SQL validation) match the types of the fields in Records.Views).
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(VIEWS_COL_VIEW_DEFINITION, VARCHAR));

    public Views() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.View> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Views(optionManager);
    }
  }

  /** Layout for the COLUMNS table. */
  public static class Columns extends InfoSchemaTable<Records.Column> {
    // COLUMNS columns, from SQL standard:
    // 1. TABLE_CATALOG
    // 2. TABLE_SCHEMA
    // 3. TABLE_NAME
    // 4. COLUMN_NAME
    // 5. ORDINAL_POSITION
    // 6. COLUMN_DEFAULT
    // 7. IS_NULLABLE
    // 8. DATA_TYPE
    // 9. CHARACTER_MAXIMUM_LENGTH
    // 10. CHARACTER_OCTET_LENGTH
    // 11. NUMERIC_PRECISION
    // 12. NUMERIC_PRECISION_RADIX
    // 13. NUMERIC_SCALE
    // 14. DATETIME_PRECISION
    // 15. INTERVAL_TYPE
    // 16. INTERVAL_PRECISION
    // 17. CHARACTER_SET_CATALOG ...

    // NOTE:  Nothing seems to verify that the types here (apparently used
    // by SQL validation) match the types of the fields in Records.Columns).
    private static final List<Field> fields = ImmutableList.of(
        Field.create(SHRD_COL_TABLE_CATALOG, VARCHAR),
        Field.create(SHRD_COL_TABLE_SCHEMA, VARCHAR),
        Field.create(SHRD_COL_TABLE_NAME, VARCHAR),
        Field.create(COLS_COL_COLUMN_NAME, VARCHAR),
        Field.create(COLS_COL_ORDINAL_POSITION, INT),
        Field.create(COLS_COL_COLUMN_DEFAULT, VARCHAR),
        Field.create(COLS_COL_IS_NULLABLE, VARCHAR),
        Field.create(COLS_COL_DATA_TYPE, VARCHAR),
        Field.create(COLS_COL_CHARACTER_MAXIMUM_LENGTH, INT),
        Field.create(COLS_COL_CHARACTER_OCTET_LENGTH, INT),
        Field.create(COLS_COL_NUMERIC_PRECISION, INT),
        Field.create(COLS_COL_NUMERIC_PRECISION_RADIX, INT),
        Field.create(COLS_COL_NUMERIC_SCALE, INT),
        Field.create(COLS_COL_DATETIME_PRECISION, INT),
        Field.create(COLS_COL_INTERVAL_TYPE, VARCHAR),
        Field.create(COLS_COL_INTERVAL_PRECISION, INT)
        );

    public Columns() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.Column> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Columns(optionManager);
    }
  }

  /** Layout for the FILES table. */
  public static class Files extends InfoSchemaTable<Records.File> {

    private static final List<Field> fields = ImmutableList.of(
        Field.create(FILES_COL_SCHEMA_NAME, VARCHAR),
        Field.create(FILES_COL_ROOT_SCHEMA_NAME, VARCHAR),
        Field.create(FILES_COL_WORKSPACE_NAME, VARCHAR),
        Field.create(FILES_COL_FILE_NAME, VARCHAR),
        Field.create(FILES_COL_RELATIVE_PATH, VARCHAR),
        Field.create(FILES_COL_IS_DIRECTORY, BIT),
        Field.create(FILES_COL_IS_FILE, BIT),
        Field.create(FILES_COL_LENGTH, BIGINT),
        Field.create(FILES_COL_OWNER, VARCHAR),
        Field.create(FILES_COL_GROUP, VARCHAR),
        Field.create(FILES_COL_PERMISSION, VARCHAR),
        Field.create(FILES_COL_ACCESS_TIME, TIMESTAMP),
        Field.create(FILES_COL_MODIFICATION_TIME, TIMESTAMP)
    );

    public Files() {
      super(fields);
    }

    @Override
    public InfoSchemaRecordGenerator<Records.File> getRecordGenerator(OptionManager optionManager) {
      return new InfoSchemaRecordGenerator.Files(optionManager);
    }
  }

}
