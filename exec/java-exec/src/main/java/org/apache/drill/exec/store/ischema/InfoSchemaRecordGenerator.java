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

import static org.apache.drill.exec.planner.types.DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.COLS_COL_COLUMN_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_ROOT_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.FILES_COL_WORKSPACE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_CONNECT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_DESCRIPTION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.TBLS_COL_TABLE_TYPE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.DrillViewInfoProvider;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.dfs.WorkspaceSchemaFactory;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.Result;
import org.apache.drill.exec.store.pojo.PojoRecordReader;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.exec.util.FileSystemUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Generates records for POJO RecordReader by scanning the given schema. At every level (catalog, schema, table, field),
 * level specific object is visited and decision is taken to visit the contents of the object. Object here is catalog,
 * schema, table or field.
 */
public abstract class InfoSchemaRecordGenerator<S> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InfoSchemaRecordGenerator.class);
  protected InfoSchemaFilter filter;

  protected OptionManager optionManager;
  public InfoSchemaRecordGenerator(OptionManager optionManager) {
    this.optionManager = optionManager;
  }

  public void setInfoSchemaFilter(InfoSchemaFilter filter) {
    this.filter = filter;
  }

  /**
   * Visit the catalog. Drill has only one catalog.
   *
   * @return Whether to continue exploring the contents of the catalog or not. Contents are schema/schema tree.
   */
  public boolean visitCatalog() {
    return true;
  }

  /**
   * Visit the given schema.
   *
   * @param schemaName Name of the schema
   * @param schema Schema object
   * @return Whether to continue exploring the contents of the schema or not. Contents are tables within the schema.
   */
  public boolean visitSchema(String schemaName, SchemaPlus schema) {
    return true;
  }

  /**
   * Visit the given table.
   *
   * @param schemaName Name of the schema where the table is present
   * @param tableName Name of the table
   * @param table Table object
   * @return Whether to continue exploring the contents of the table or not. Contents are fields within the table.
   */
  public boolean visitTable(String schemaName, String tableName, Table table) {
    return true;
  }

  /**
   * Visit the given field.
   *
   * @param schemaName Schema where the table of the field is present
   * @param tableName Table name
   * @param field Field object
   */
  public void visitField(String schemaName, String tableName, RelDataTypeField field) {
  }

  public void visitFiles(String schemaName, SchemaPlus schema) {
  }

  protected boolean shouldVisitCatalog() {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues = ImmutableMap.of(CATS_COL_CATALOG_NAME, IS_CATALOG_NAME);

    // If the filter evaluates to false then we don't need to visit the catalog.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the catalog.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  protected boolean shouldVisitSchema(String schemaName, SchemaPlus schema) {
    try {
      // if the schema path is null or empty (try for root schema)
      if (schemaName == null || schemaName.isEmpty()) {
        return false;
      }

      AbstractSchema drillSchema = schema.unwrap(AbstractSchema.class);
      if (!drillSchema.showInInformationSchema()) {
        return false;
      }

      if (filter == null) {
        return true;
      }

      final Map<String, String> recordValues =
          ImmutableMap.of(
              CATS_COL_CATALOG_NAME, IS_CATALOG_NAME,
              SHRD_COL_TABLE_SCHEMA, schemaName,
              SCHS_COL_SCHEMA_NAME, schemaName);

      // If the filter evaluates to false then we don't need to visit the schema.
      // For other two results (TRUE, INCONCLUSIVE) continue to visit the schema.
      return filter.evaluate(recordValues) != Result.FALSE;
    } catch(ClassCastException e) {
      // ignore and return true as this is not a Drill schema
    }
    return true;
  }

  protected boolean shouldVisitTable(String schemaName, String tableName, TableType tableType) {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues =
        ImmutableMap.of(
            CATS_COL_CATALOG_NAME, IS_CATALOG_NAME,
            SHRD_COL_TABLE_SCHEMA, schemaName,
            SCHS_COL_SCHEMA_NAME, schemaName,
            SHRD_COL_TABLE_NAME, tableName,
            TBLS_COL_TABLE_TYPE, tableType.toString());

    // If the filter evaluates to false then we don't need to visit the table.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the table.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  protected boolean shouldVisitColumn(String schemaName, String tableName, String columnName) {
    if (filter == null) {
      return true;
    }

    final Map<String, String> recordValues =
        ImmutableMap.of(
            CATS_COL_CATALOG_NAME, IS_CATALOG_NAME,
            SHRD_COL_TABLE_SCHEMA, schemaName,
            SCHS_COL_SCHEMA_NAME, schemaName,
            SHRD_COL_TABLE_NAME, tableName,
            COLS_COL_COLUMN_NAME, columnName);

    // If the filter evaluates to false then we don't need to visit the column.
    // For other two results (TRUE, INCONCLUSIVE) continue to visit the column.
    return filter.evaluate(recordValues) != Result.FALSE;
  }

  protected boolean shouldVisitFiles(String schemaName, SchemaPlus schemaPlus) {
    if (filter == null) {
      return true;
    }

    AbstractSchema schema;
    try {
      schema = schemaPlus.unwrap(AbstractSchema.class);
    } catch (ClassCastException e) {
      return false;
    }

    if (!(schema instanceof WorkspaceSchemaFactory.WorkspaceSchema)) {
      return false;
    }

    WorkspaceSchemaFactory.WorkspaceSchema wsSchema = (WorkspaceSchemaFactory.WorkspaceSchema) schema;

    Map<String, String> recordValues = new HashMap<>();
    recordValues.put(FILES_COL_SCHEMA_NAME, schemaName);
    recordValues.put(FILES_COL_ROOT_SCHEMA_NAME, wsSchema.getSchemaPath().get(0));
    recordValues.put(FILES_COL_WORKSPACE_NAME, wsSchema.getName());

    return filter.evaluate(recordValues) != Result.FALSE;
  }

  public abstract PojoRecordReader<S> getRecordReader();

  public void scanSchema(SchemaPlus root) {
    if (shouldVisitCatalog() && visitCatalog()) {
      scanSchema(root.getName(), root);
    }
  }

  /**
   * Recursively scans the given schema, invoking the visitor as appropriate.
   * @param  schemaPath  the path to the given schema, so far
   * @param  schema  the given schema
   */
  private void scanSchema(String schemaPath, SchemaPlus schema) {

    // Recursively scan any subschema.
    for (String name: schema.getSubSchemaNames()) {
      scanSchema(schemaPath +
          ("".equals(schemaPath) ? "" : ".") + // If we have an empty schema path, then don't insert a leading dot.
          name, schema.getSubSchema(name));
    }

    // Visit this schema and if requested ...
    if (shouldVisitSchema(schemaPath, schema) && visitSchema(schemaPath, schema)) {
      visitTables(schemaPath, schema);
    }

    if (shouldVisitFiles(schemaPath, schema)) {
      visitFiles(schemaPath, schema);
    }
  }

  /**
   * Visit the tables in the given schema. The
   * @param  schemaPath  the path to the given schema
   * @param  schema  the given schema
   */
  public void visitTables(String schemaPath, SchemaPlus schema) {
    final AbstractSchema drillSchema = schema.unwrap(AbstractSchema.class);
    final List<String> tableNames = Lists.newArrayList(schema.getTableNames());
    for(Pair<String, ? extends Table> tableNameToTable : drillSchema.getTablesByNames(tableNames)) {
      final String tableName = tableNameToTable.getKey();
      final Table table = tableNameToTable.getValue();
      final TableType tableType = table.getJdbcTableType();
      // Visit the table, and if requested ...
      if(shouldVisitTable(schemaPath, tableName, tableType) && visitTable(schemaPath, tableName, table)) {
        // ... do for each of the table's fields.
        final RelDataType tableRow = table.getRowType(new JavaTypeFactoryImpl(DRILL_REL_DATATYPE_SYSTEM));
        for (RelDataTypeField field: tableRow.getFieldList()) {
          if (shouldVisitColumn(schemaPath, tableName, field.getName())) {
            visitField(schemaPath, tableName, field);
          }
        }
      }
    }
  }

  public static class Catalogs extends InfoSchemaRecordGenerator<Records.Catalog> {
    List<Records.Catalog> records = ImmutableList.of();

    public Catalogs(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.Catalog> getRecordReader() {
      return new PojoRecordReader<>(Records.Catalog.class, records);
    }

    @Override
    public boolean visitCatalog() {
      records = ImmutableList.of(new Records.Catalog(IS_CATALOG_NAME, IS_CATALOG_DESCRIPTION, IS_CATALOG_CONNECT));
      return false;
    }
  }

  public static class Schemata extends InfoSchemaRecordGenerator<Records.Schema> {
    List<Records.Schema> records = Lists.newArrayList();

    public Schemata(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.Schema> getRecordReader() {
      return new PojoRecordReader<>(Records.Schema.class, records);
    }

    @Override
    public boolean visitSchema(String schemaName, SchemaPlus schema) {
      AbstractSchema as = schema.unwrap(AbstractSchema.class);
      records.add(new Records.Schema(IS_CATALOG_NAME, schemaName, "<owner>",
                                     as.getTypeName(), as.isMutable()));
      return false;
    }
  }

  public static class Tables extends InfoSchemaRecordGenerator<Records.Table> {
    List<Records.Table> records = Lists.newArrayList();

    public Tables(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.Table> getRecordReader() {
      return new PojoRecordReader<>(Records.Table.class, records);
    }

    @Override
    public void visitTables(String schemaPath, SchemaPlus schema) {
      final AbstractSchema drillSchema = schema.unwrap(AbstractSchema.class);
      final List<Pair<String, TableType>> tableNamesAndTypes = drillSchema
          .getTableNamesAndTypes(optionManager.getOption(ExecConstants.ENABLE_BULK_LOAD_TABLE_LIST),
              (int)optionManager.getOption(ExecConstants.BULK_LOAD_TABLE_LIST_BULK_SIZE));

      for (Pair<String, TableType> tableNameAndType : tableNamesAndTypes) {
        final String tableName = tableNameAndType.getKey();
        final TableType tableType = tableNameAndType.getValue();
        // Visit the table, and if requested ...
        if (shouldVisitTable(schemaPath, tableName, tableType)) {
          visitTableWithType(schemaPath, tableName, tableType);
        }
      }
    }

    private void visitTableWithType(String schemaName, String tableName, TableType type) {
      Preconditions
          .checkNotNull(type, "Error. Type information for table %s.%s provided is null.", schemaName,
              tableName);
      records.add(new Records.Table(IS_CATALOG_NAME, schemaName, tableName, type.toString()));
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      Preconditions.checkNotNull(table, "Error. Table %s.%s provided is null.", schemaName, tableName);

      // skip over unknown table types
      if (table.getJdbcTableType() != null) {
        records.add(new Records.Table(IS_CATALOG_NAME, schemaName, tableName,
            table.getJdbcTableType().toString()));
      }

      return false;
    }
  }

  public static class Views extends InfoSchemaRecordGenerator<Records.View> {
    List<Records.View> records = Lists.newArrayList();

    public Views(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.View> getRecordReader() {
      return new PojoRecordReader<>(Records.View.class, records);
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      if (table.getJdbcTableType() == TableType.VIEW) {
        records.add(new Records.View(IS_CATALOG_NAME, schemaName, tableName,
                    ((DrillViewInfoProvider) table).getViewSql()));
      }
      return false;
    }
  }

  public static class Columns extends InfoSchemaRecordGenerator<Records.Column> {
    List<Records.Column> records = Lists.newArrayList();
    public Columns(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.Column> getRecordReader() {
      return new PojoRecordReader<>(Records.Column.class, records);
    }

    @Override
    public void visitField(String schemaName, String tableName, RelDataTypeField field) {
      records.add(new Records.Column(IS_CATALOG_NAME, schemaName, tableName, field));
    }
  }

  public static class Files extends InfoSchemaRecordGenerator<Records.File> {

    List<Records.File> records = new ArrayList<>();

    public Files(OptionManager optionManager) {
      super(optionManager);
    }

    @Override
    public PojoRecordReader<Records.File> getRecordReader() {
      return new PojoRecordReader<>(Records.File.class, records);
    }

    @Override
    public void visitFiles(String schemaName, SchemaPlus schemaPlus) {
      try {
        AbstractSchema schema = schemaPlus.unwrap(AbstractSchema.class);
        if (schema instanceof WorkspaceSchemaFactory.WorkspaceSchema) {
          WorkspaceSchemaFactory.WorkspaceSchema wsSchema = (WorkspaceSchemaFactory.WorkspaceSchema) schema;
          String defaultLocation = wsSchema.getDefaultLocation();
          FileSystem fs = wsSchema.getFS();
          boolean recursive = optionManager.getBoolean(ExecConstants.LIST_FILES_RECURSIVELY);
          // add URI to the path to ensure that directory objects are skipped (see S3AFileSystem.listStatus method)
          FileSystemUtil.listAllSafe(fs, new Path(fs.getUri().toString(), defaultLocation), recursive).forEach(
              fileStatus -> records.add(new Records.File(schemaName, wsSchema, fileStatus))
          );
        }
      } catch (ClassCastException | UnsupportedOperationException e) {
        // ignore the exception since either this is not a Drill schema or schema does not support files listing
      }
    }
  }

}
