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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import org.apache.drill.exec.planner.logical.DrillViewInfoProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.pojo.PojoRecordReader;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

import java.util.List;

/** Generates records for POJO RecordReader by scanning the given schema */
public abstract class RecordGenerator {

  public boolean visitSchema(String schemaName, SchemaPlus schema) {
    return shouldVisitSchema(schema);
  }

  public boolean visitTable(String schemaName, String tableName, Table table) {
    return true;
  }

  public boolean visitField(String schemaName, String tableName, RelDataTypeField field) {
    return true;
  }

  protected boolean shouldVisitSchema(SchemaPlus schema) {
    try {
      AbstractSchema drillSchema = schema.unwrap(AbstractSchema.class);
      return drillSchema.showInInformationSchema();
    } catch(ClassCastException e) {
      // ignore and return true as this is not a drill schema
    }
    return true;
  }

  public abstract RecordReader getRecordReader();

  public void scanSchema(SchemaPlus root) {
    scanSchema(root.getName(), root);
  }

  /**
   * Recursively scan the schema, invoking the visitor as appropriate.
   * @param schemaPath - the path to the current schema, so far,
   * @param schema - the current schema.
   */
  private void scanSchema(String schemaPath, SchemaPlus schema) {

    // Recursively scan the subschema.
    for (String name: schema.getSubSchemaNames()) {
      scanSchema(schemaPath +
          (schemaPath == "" ? "" : ".") + // If we have an empty schema path, then don't insert a leading dot.
          name, schema.getSubSchema(name));
    }

    // Visit this schema and if requested ...
    if (visitSchema(schemaPath, schema)) {

      // ... do for each of the schema's tables.
      for (String tableName: schema.getTableNames()) {
        Table table = schema.getTable(tableName);
        // Visit the table, and if requested ...
        if (visitTable(schemaPath,  tableName, table)) {

          // ... do for each of the table's fields.
          RelDataType tableRow = table.getRowType(new JavaTypeFactoryImpl());
          for (RelDataTypeField field: tableRow.getFieldList()) {
            visitField(schemaPath,  tableName, field);
          }
        }
      }
    }
  }

  public static class Catalogs extends RecordGenerator {
    @Override
    public RecordReader getRecordReader() {
      Records.Catalog catalogRecord = new Records.Catalog("DRILL", "The internal metadata used by Drill", "");
      return new PojoRecordReader<>(Records.Catalog.class, ImmutableList.of(catalogRecord).iterator());
    }
  }

  public static class Schemata extends RecordGenerator {
    List<Records.Schema> records = Lists.newArrayList();

    @Override
    public RecordReader getRecordReader() {
      return new PojoRecordReader<>(Records.Schema.class, records.iterator());
    }

    @Override
    public boolean visitSchema(String schemaName, SchemaPlus schema) {
      if (shouldVisitSchema(schema) && schemaName != null && !schemaName.isEmpty()) {
        AbstractSchema as = schema.unwrap(AbstractSchema.class);
        records.add(new Records.Schema("DRILL", schemaName, "<owner>", as.getTypeName(), as.isMutable()));
      }
      return false;
    }
  }

  public static class Tables extends RecordGenerator {
    List<Records.Table> records = Lists.newArrayList();

    @Override
    public RecordReader getRecordReader() {
      return new PojoRecordReader<>(Records.Table.class, records.iterator());
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      records.add(new Records.Table("DRILL", schemaName, tableName, table.getJdbcTableType().toString()));
      return false;
    }
  }

  public static class Views extends RecordGenerator {
    List<Records.View> records = Lists.newArrayList();

    @Override
    public RecordReader getRecordReader() {
      return new PojoRecordReader<>(Records.View.class, records.iterator());
    }

    @Override
    public boolean visitTable(String schemaName, String tableName, Table table) {
      if (table.getJdbcTableType() == TableType.VIEW) {
        records.add(new Records.View("DRILL", schemaName, tableName, ((DrillViewInfoProvider) table).getViewSql()));
      }
      return false;
    }
  }

  public static class Columns extends RecordGenerator {
    List<Records.Column> records = Lists.newArrayList();

    @Override
    public RecordReader getRecordReader() {
      return new PojoRecordReader<>(Records.Column.class, records.iterator());
    }

    @Override
    public boolean visitField(String schemaName, String tableName, RelDataTypeField field) {
      records.add(new Records.Column("DRILL", schemaName, tableName, field));
      return false;
    }
  }
}
