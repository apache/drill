/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.metadata;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_CONNECT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_DESCR;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.IS_CATALOG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.OptionsTest;
import org.apache.drill.exec.proto.UserProtos.CatalogMetadata;
import org.apache.drill.exec.proto.UserProtos.ColumnMetadata;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsResp;
import org.apache.drill.exec.proto.UserProtos.GetColumnsResp;
import org.apache.drill.exec.proto.UserProtos.GetSchemasResp;
import org.apache.drill.exec.proto.UserProtos.GetTablesResp;
import org.apache.drill.exec.proto.UserProtos.LikeFilter;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.SchemaMetadata;
import org.apache.drill.exec.proto.UserProtos.TableMetadata;
import org.apache.drill.exec.store.sys.SystemTable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for metadata provider APIs.
 */
@Category(OptionsTest.class)
public class TestMetadataProvider extends BaseTestQuery {

  @Test
  public void catalogs() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS"); // SQL equivalent

    GetCatalogsResp resp = client.getCatalogs(null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS " +
    //    "WHERE CATALOG_NAME LIKE '%DRI%' ESCAPE '\\'"); // SQL equivalent
    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setPattern("%DRI%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(1, catalogs.size());

    CatalogMetadata c = catalogs.get(0);
    assertEquals(IS_CATALOG_NAME, c.getCatalogName());
    assertEquals(IS_CATALOG_DESCR, c.getDescription());
    assertEquals(IS_CATALOG_CONNECT, c.getConnect());
  }

  @Test
  public void catalogsWithFilterNegative() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.CATALOGS
    //     WHERE CATALOG_NAME LIKE '%DRIj\\\\hgjh%' ESCAPE '\\'"); // SQL equivalent

    GetCatalogsResp resp =
        client.getCatalogs(LikeFilter.newBuilder().setPattern("%DRIj\\%hgjh%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<CatalogMetadata> catalogs = resp.getCatalogsList();
    assertEquals(0, catalogs.size());
  }

  @Test
  public void schemas() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(6, schemas.size());

    verifySchema("INFORMATION_SCHEMA", schemas);
    verifySchema("cp.default", schemas);
    verifySchema("dfs.default", schemas);
    verifySchema("dfs.root", schemas);
    verifySchema("dfs.tmp", schemas);
    verifySchema("sys", schemas);
  }

  @Test
  public void schemasWithSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE '%y%'"); // SQL equivalent

    GetSchemasResp resp = client.getSchemas(null, LikeFilter.newBuilder().setPattern("%y%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(1, schemas.size());

    verifySchema("sys", schemas);
  }

  @Test
  public void schemasWithCatalogNameFilterAndSchemaNameFilter() throws Exception {
    GetSchemasResp resp = client.getSchemas(
        LikeFilter.newBuilder().setPattern("%RI%").build(),
        LikeFilter.newBuilder().setPattern("%dfs%").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<SchemaMetadata> schemas = resp.getSchemasList();
    assertEquals(3, schemas.size());

    verifySchema("dfs.default", schemas);
    verifySchema("dfs.root", schemas);
    verifySchema("dfs.tmp", schemas);
  }

  @Test
  public void tables() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES`"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(17, tables.size());

    verifyTable("INFORMATION_SCHEMA", "CATALOGS", tables);
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", tables);
    verifyTable("INFORMATION_SCHEMA", "SCHEMATA", tables);
    verifyTable("INFORMATION_SCHEMA", "TABLES", tables);
    verifyTable("INFORMATION_SCHEMA", "VIEWS", tables);
    verifyTable("sys", "boot", tables);
    verifyTable("sys", "drillbits", tables);
    verifyTable("sys", "memory", tables);
    verifyTable("sys", SystemTable.OPTION.getTableName(), tables);
    verifyTable("sys", SystemTable.OPTION_VAL.getTableName(), tables);
    verifyTable("sys", "threads", tables);
    verifyTable("sys", "version", tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS.getTableName(), tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS_VAL.getTableName(), tables);
  }

  @Test
  public void tablesWithTableFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_TYPE IN ('TABLE')"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, Arrays.asList("TABLE")).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertTrue(tables.isEmpty());
  }

  @Test
  public void tablesWithSystemTableFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_TYPE IN ('SYSTEM_TABLE')"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null, null, Arrays.asList("SYSTEM_TABLE")).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(17, tables.size());

    verifyTable("INFORMATION_SCHEMA", "CATALOGS", tables);
    verifyTable("INFORMATION_SCHEMA", "COLUMNS", tables);
    verifyTable("INFORMATION_SCHEMA", "SCHEMATA", tables);
    verifyTable("INFORMATION_SCHEMA", "TABLES", tables);
    verifyTable("INFORMATION_SCHEMA", "VIEWS", tables);
    verifyTable("sys", "boot", tables);
    verifyTable("sys", "drillbits", tables);
    verifyTable("sys", "memory", tables);
    verifyTable("sys", SystemTable.OPTION.getTableName(), tables);
    verifyTable("sys", SystemTable.OPTION_VAL.getTableName(), tables);
    verifyTable("sys", "threads", tables);
    verifyTable("sys", "version", tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS.getTableName(), tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS_VAL.getTableName(), tables);
  }

  @Test
  public void tablesWithTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` WHERE TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null, null,
        LikeFilter.newBuilder().setPattern("%o%").build(),
        null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(10, tables.size());

    verifyTable("sys", "boot", tables);
    verifyTable("sys", "memory", tables);
    verifyTable("sys", SystemTable.OPTION.getTableName(), tables);
    verifyTable("sys", SystemTable.OPTION_VAL.getTableName(), tables);
    verifyTable("sys", "version", tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS.getTableName(), tables);
    verifyTable("sys", SystemTable.INTERNAL_OPTIONS_VAL.getTableName(), tables);
  }

  @Test
  public void tablesWithTableNameFilterAndSchemaNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.`TABLES` " +
    //    "WHERE TABLE_SCHEMA LIKE '%N\\_S%' ESCAPE '\\' AND TABLE_NAME LIKE '%o%'"); // SQL equivalent

    GetTablesResp resp = client.getTables(null,
        LikeFilter.newBuilder().setPattern("%N\\_S%").setEscape("\\").build(),
        LikeFilter.newBuilder().setPattern("%o%").build(),
        null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<TableMetadata> tables = resp.getTablesList();
    assertEquals(0, tables.size());
  }

  @Test
  public void columns() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null, null, null).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(117, columns.size());
    // too many records to verify the output.
  }

  @Test
  public void columnsWithColumnNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null, null,
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(5, columns.size());

    verifyColumn("sys", "drillbits", "user_port", columns);
    verifyColumn("sys", "drillbits", "control_port", columns);
    verifyColumn("sys", "drillbits", "data_port", columns);
    verifyColumn("sys", "memory", "user_port", columns);
    verifyColumn("sys", "threads", "user_port", columns);
  }

  @Test
  public void columnsWithColumnNameFilterAndTableNameFilter() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS
    //     WHERE TABLE_NAME LIKE '%bits' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(null, null,
        LikeFilter.newBuilder().setPattern("%bits").build(),
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(3, columns.size());

    verifyColumn("sys", "drillbits", "user_port", columns);
    verifyColumn("sys", "drillbits", "control_port", columns);
    verifyColumn("sys", "drillbits", "data_port", columns);
  }

  @Test
  public void columnsWithAllSupportedFilters() throws Exception {
    // test("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
    //    "TABLE_CATALOG LIKE '%ILL' AND TABLE_SCHEMA LIKE 'sys' AND " +
    //    "TABLE_NAME LIKE '%bits' AND COLUMN_NAME LIKE '%\\_p%' ESCAPE '\\'"); // SQL equivalent

    GetColumnsResp resp = client.getColumns(
        LikeFilter.newBuilder().setPattern("%ILL").build(),
        LikeFilter.newBuilder().setPattern("sys").build(),
        LikeFilter.newBuilder().setPattern("%bits").build(),
        LikeFilter.newBuilder().setPattern("%\\_p%").setEscape("\\").build()).get();

    assertEquals(RequestStatus.OK, resp.getStatus());
    List<ColumnMetadata> columns = resp.getColumnsList();
    assertEquals(3, columns.size());

    verifyColumn("sys", "drillbits", "user_port", columns);
    verifyColumn("sys", "drillbits", "control_port", columns);
    verifyColumn("sys", "drillbits", "data_port", columns);
  }

  /** Helper method to verify schema contents */
  private static void verifySchema(String schemaName, List<SchemaMetadata> schemas) {
    for(SchemaMetadata schema : schemas) {
      if (schemaName.equals(schema.getSchemaName())) {
        assertEquals(IS_CATALOG_NAME, schema.getCatalogName());
        return;
      }
    }

    fail("Failed to find schema '" + schemaName + "' in results: " + schemas);
  }

  /** Helper method to verify table contents */
  private static void verifyTable(String schemaName, String tableName, List<TableMetadata> tables) {

    for(TableMetadata table : tables) {
      if (tableName.equals(table.getTableName()) && schemaName.equals(table.getSchemaName())) {
        assertEquals(IS_CATALOG_NAME, table.getCatalogName());
        return;
      }
    }

    fail(String.format("Failed to find table '%s.%s' in results: %s", schemaName, tableName, tables));
  }

  /** Helper method to verify column contents */
  private static void verifyColumn(String schemaName, String tableName, String columnName,
      List<ColumnMetadata> columns) {

    for(ColumnMetadata column : columns) {
      if (schemaName.equals(column.getSchemaName()) && tableName.equals(column.getTableName()) &&
          columnName.equals(column.getColumnName())) {
        assertEquals(IS_CATALOG_NAME, column.getCatalogName());
        return;
      }
    }

    fail(String.format("Failed to find column '%s.%s.%s' in results: %s", schemaName, tableName, columnName, columns));
  }
}
