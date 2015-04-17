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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.*;
import org.apache.drill.exec.planner.logical.DrillViewInfoProvider;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.Result;
import org.apache.drill.exec.store.pojo.PojoRecordReader;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Generates records for POJO RecordReader by scanning the given schema.
 */
public abstract class RecordGenerator {
  protected InfoSchemaFilter filter;

  public void setInfoSchemaFilter(InfoSchemaFilter filter) {
    this.filter = filter;
  }

  public boolean visitSchema(String schemaName, SchemaPlus schema) {
    return true;
  }

  public boolean visitTable(String schemaName, String tableName, Table table) {
    return true;
  }

  public boolean visitField(String schemaName, String tableName, RelDataTypeField field) {
    return true;
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

      Map<String, String> recordValues =
          ImmutableMap.of(SHRD_COL_TABLE_SCHEMA, schemaName,
                          SCHS_COL_SCHEMA_NAME, schemaName);
      if (filter != null && filter.evaluate(recordValues) == Result.FALSE) {
        // If the filter evaluates to false then we don't need to visit the schema.
        // For other two results (TRUE, INCONCLUSIVE) continue to visit the schema.
        return false;
      }
    } catch(ClassCastException e) {
      // ignore and return true as this is not a Drill schema
    }
    return true;
  }

  protected boolean shouldVisitTable(String schemaName, String tableName) {
    Map<String, String> recordValues =
        ImmutableMap.of( SHRD_COL_TABLE_SCHEMA, schemaName,
                         SCHS_COL_SCHEMA_NAME, schemaName,
                         SHRD_COL_TABLE_NAME, tableName);
    if (filter != null && filter.evaluate(recordValues) == Result.FALSE) {
      return false;
    }

    return true;
  }

  public abstract RecordReader getRecordReader();

  public void scanSchema(SchemaPlus root) {
    scanSchema(root.getName(), root);
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
          (schemaPath == "" ? "" : ".") + // If we have an empty schema path, then don't insert a leading dot.
          name, schema.getSubSchema(name));
    }

    // Visit this schema and if requested ...
    if (shouldVisitSchema(schemaPath, schema) && visitSchema(schemaPath, schema)) {
      // ... do for each of the schema's tables.
      for (String tableName: schema.getTableNames()) {
        Table table = schema.getTable(tableName);

        if (table == null) {
          // Schema may return NULL for table if the query user doesn't have permissions to load the table. Ignore such
          // tables as INFO SCHEMA is about showing tables which the use has access to query.
          continue;
        }

        // Visit the table, and if requested ...
        if (shouldVisitTable(schemaPath, tableName) && visitTable(schemaPath,  tableName, table)) {
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
      Records.Catalog catalogRecord =
          new Records.Catalog(IS_CATALOG_NAME,
                              "The internal metadata used by Drill", "");
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
      AbstractSchema as = schema.unwrap(AbstractSchema.class);
      records.add(new Records.Schema(IS_CATALOG_NAME, schemaName, "<owner>",
                                     as.getTypeName(), as.isMutable()));
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
      records.add(new Records.Table(IS_CATALOG_NAME, schemaName, tableName,
                                    table.getJdbcTableType().toString()));
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
        records.add(new Records.View(IS_CATALOG_NAME, schemaName, tableName,
                    ((DrillViewInfoProvider) table).getViewSql()));
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
      records.add(new Records.Column(IS_CATALOG_NAME, schemaName, tableName, field));
      return false;
    }
  }
}
