/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.sql;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_CONNECT;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_DESCRIPTION;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.TestBuilder;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Contains tests for
 * -- InformationSchema
 * -- Queries on InformationSchema such as SHOW TABLES, SHOW SCHEMAS or DESCRIBE table
 * -- USE schema
 * -- SHOW FILES
 */
public class TestInfoSchema extends BaseTestQuery {

  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);

  @Test
  public void selectFromAllTables() throws Exception{
    test("select * from INFORMATION_SCHEMA.SCHEMATA");
    test("select * from INFORMATION_SCHEMA.CATALOGS");
    test("select * from INFORMATION_SCHEMA.VIEWS");
    test("select * from INFORMATION_SCHEMA.`TABLES`");
    test("select * from INFORMATION_SCHEMA.COLUMNS");
  }

  @Test
  public void catalogs() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM INFORMATION_SCHEMA.CATALOGS")
        .unOrdered()
        .baselineColumns(CATS_COL_CATALOG_NAME, CATS_COL_CATALOG_DESCRIPTION, CATS_COL_CATALOG_CONNECT)
        .baselineValues("DRILL", "The internal metadata used by Drill", "")
        .go();
  }

  @Test
  public void showTablesFromDb() throws Exception{
    final List<String[]> expected =
        ImmutableList.of(
            new String[] { "INFORMATION_SCHEMA", "VIEWS" },
            new String[] { "INFORMATION_SCHEMA", "COLUMNS" },
            new String[] { "INFORMATION_SCHEMA", "TABLES" },
            new String[] { "INFORMATION_SCHEMA", "CATALOGS" },
            new String[] { "INFORMATION_SCHEMA", "SCHEMATA" }
        );

    final TestBuilder t1 = testBuilder()
        .sqlQuery("SHOW TABLES FROM INFORMATION_SCHEMA")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME");
    for(String[] expectedRow : expected) {
      t1.baselineValues(expectedRow);
    }
    t1.go();

    final TestBuilder t2 = testBuilder()
        .sqlQuery("SHOW TABLES IN INFORMATION_SCHEMA")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME");
    for(String[] expectedRow : expected) {
      t2.baselineValues(expectedRow);
    }
    t2.go();
  }

  @Test
  public void showTablesFromDbWhere() throws Exception{
    testBuilder()
        .sqlQuery("SHOW TABLES FROM INFORMATION_SCHEMA WHERE TABLE_NAME='VIEWS'")
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("INFORMATION_SCHEMA", "VIEWS")
        .go();
  }

  @Test
  public void showTablesLike() throws Exception{
    testBuilder()
        .sqlQuery("SHOW TABLES LIKE '%CH%'")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME")
        .baselineValues("INFORMATION_SCHEMA", "SCHEMATA")
        .go();
  }

  @Test
  public void showDatabases() throws Exception{
    final List<String[]> expected =
        ImmutableList.of(
            new String[] { "dfs.default" },
            new String[] { "dfs.root" },
            new String[] { "dfs.tmp" },
            new String[] { "cp.default" },
            new String[] { "sys" },
            new String[] { "dfs_test.home" },
            new String[] { "dfs_test.default" },
            new String[] { "dfs_test.tmp" },
            new String[] { "INFORMATION_SCHEMA" }
        );

    final TestBuilder t1 = testBuilder()
        .sqlQuery("SHOW DATABASES")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME");
    for(String[] expectedRow : expected) {
      t1.baselineValues(expectedRow);
    }
    t1.go();

    final TestBuilder t2 = testBuilder()
        .sqlQuery("SHOW SCHEMAS")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME");
    for(String[] expectedRow : expected) {
      t2.baselineValues(expectedRow);
    }
    t2.go();
  }

  @Test
  public void showDatabasesWhere() throws Exception{
    testBuilder()
        .sqlQuery("SHOW DATABASES WHERE SCHEMA_NAME='dfs_test.tmp'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("dfs_test.tmp")
        .go();
  }

  @Test
  public void showDatabasesLike() throws Exception{
    testBuilder()
        .sqlQuery("SHOW DATABASES LIKE '%y%'")
        .unOrdered()
        .baselineColumns("SCHEMA_NAME")
        .baselineValues("sys")
        .go();
  }

  @Test
  public void describeTable() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE CATALOGS")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("CATALOG_NAME", "CHARACTER VARYING", "NO")
        .baselineValues("CATALOG_DESCRIPTION", "CHARACTER VARYING", "NO")
        .baselineValues("CATALOG_CONNECT", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void describeTableWithSchema() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.`TABLES`")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "NO")
        .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "NO")
        .baselineValues("TABLE_NAME", "CHARACTER VARYING", "NO")
        .baselineValues("TABLE_TYPE", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void describeWhenSameTableNameExistsInMultipleSchemas() throws Exception{
    try {
      test("USE dfs_test.tmp");
      test("CREATE OR REPLACE VIEW `TABLES` AS SELECT full_name FROM cp.`employee.json`");

      testBuilder()
          .sqlQuery("DESCRIBE `TABLES`")
          .unOrdered()
          .optionSettingQueriesForTestQuery("USE dfs_test.tmp")
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("full_name", "ANY", "YES")
          .go();

      testBuilder()
          .sqlQuery("DESCRIBE INFORMATION_SCHEMA.`TABLES`")
          .unOrdered()
          .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
          .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "NO")
          .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "NO")
          .baselineValues("TABLE_NAME", "CHARACTER VARYING", "NO")
          .baselineValues("TABLE_TYPE", "CHARACTER VARYING", "NO")
          .go();
    } finally {
      test("DROP VIEW dfs_test.tmp.`TABLES`");
    }
  }

  @Test
  public void describeTableWithColumnName() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE `TABLES` TABLE_CATALOG")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void describeTableWithSchemaAndColumnName() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.`TABLES` TABLE_CATALOG")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void describeTableWithColQualifier() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE COLUMNS 'TABLE%'")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE INFORMATION_SCHEMA")
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("TABLE_CATALOG", "CHARACTER VARYING", "NO")
        .baselineValues("TABLE_SCHEMA", "CHARACTER VARYING", "NO")
        .baselineValues("TABLE_NAME", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void describeTableWithSchemaAndColQualifier() throws Exception{
    testBuilder()
        .sqlQuery("DESCRIBE INFORMATION_SCHEMA.SCHEMATA 'SCHEMA%'")
        .unOrdered()
        .baselineColumns("COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE")
        .baselineValues("SCHEMA_NAME", "CHARACTER VARYING", "NO")
        .baselineValues("SCHEMA_OWNER", "CHARACTER VARYING", "NO")
        .go();
  }

  @Test
  public void defaultSchemaDfs() throws Exception{
    testBuilder()
        .sqlQuery("SELECT R_REGIONKEY FROM `[WORKING_PATH]/../../sample-data/region.parquet` LIMIT 1")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE dfs_test")
        .baselineColumns("R_REGIONKEY")
        .baselineValues(0L)
        .go();
  }

  @Test
  public void defaultSchemaClasspath() throws Exception{
    testBuilder()
        .sqlQuery("SELECT full_name FROM `employee.json` LIMIT 1")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE cp")
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();
  }


  @Test
  public void queryFromNonDefaultSchema() throws Exception{
    testBuilder()
        .sqlQuery("SELECT full_name FROM cp.`employee.json` LIMIT 1")
        .unOrdered()
        .optionSettingQueriesForTestQuery("USE dfs_test")
        .baselineColumns("full_name")
        .baselineValues("Sheri Nowmer")
        .go();
  }

  @Test
  public void useSchema() throws Exception{
    testBuilder()
        .sqlQuery("USE dfs_test.`default`")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs_test.default]")
        .go();
  }

  @Test
  public void useSubSchemaWithinSchema() throws Exception{
    testBuilder()
        .sqlQuery("USE dfs_test")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs_test]")
        .go();

    testBuilder()
        .sqlQuery("USE tmp")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs_test.tmp]")
        .go();

    testBuilder()
        .sqlQuery("USE dfs.`default`")
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, "Default schema changed to [dfs.default]")
        .go();
  }

  @Test
  public void useSchemaNegative() throws Exception{
    errorMsgTestHelper("USE invalid.schema",
        "Schema [invalid.schema] is not valid with respect to either root schema or current default schema.");
  }

  // Tests using backticks around the complete schema path
  // select * from `dfs_test.tmp`.`/tmp/nation.parquet`;
  @Test
  public void completeSchemaRef1() throws Exception {
    test("SELECT * FROM `cp.default`.`employee.json` limit 2");
  }

  @Test
  public void showFiles() throws Exception {
    test("show files from dfs_test.`/tmp`");
    test("show files from `dfs_test.default`.`/tmp`");
  }

  @Test
  public void showFilesWithDefaultSchema() throws Exception{
    test("USE dfs_test.`default`");
    test("SHOW FILES FROM `/tmp`");
  }

  @Test
  public void describeSchemaSyntax() throws Exception {
    test("describe schema dfs_test");
    test("describe schema dfs_test.`default`");
    test("describe database dfs_test.`default`");
  }

  @Test
  public void describeSchemaOutput() throws Exception {
    final List<QueryDataBatch> result = testSqlWithResults("describe schema dfs_test.tmp");
    assertTrue(result.size() == 1);
    final QueryDataBatch batch = result.get(0);
    final RecordBatchLoader loader = new RecordBatchLoader(getDrillbitContext().getAllocator());
    loader.load(batch.getHeader().getDef(), batch.getData());

    // check schema column value
    final VectorWrapper schemaValueVector = loader.getValueAccessorById(
        NullableVarCharVector.class,
        loader.getValueVectorId(SchemaPath.getCompoundPath("schema")).getFieldIds());
    String schema = schemaValueVector.getValueVector().getAccessor().getObject(0).toString();
    assertEquals("dfs_test.tmp", schema);

    // check properties column value
    final VectorWrapper propertiesValueVector = loader.getValueAccessorById(
        NullableVarCharVector.class,
        loader.getValueVectorId(SchemaPath.getCompoundPath("properties")).getFieldIds());
    String properties = propertiesValueVector.getValueVector().getAccessor().getObject(0).toString();
    final Map configMap = mapper.readValue(properties, Map.class);

    // check some stable properties existence
    assertTrue(configMap.containsKey("connection"));
    assertTrue(configMap.containsKey("config"));
    assertTrue(configMap.containsKey("formats"));
    assertFalse(configMap.containsKey("workspaces"));

    // check some stable properties values
    assertEquals("file", configMap.get("type"));

    final FileSystemConfig testConfig = (FileSystemConfig) bits[0].getContext().getStorage().getPlugin("dfs_test").getConfig();
    final String tmpSchemaLocation = testConfig.workspaces.get("tmp").getLocation();
    assertEquals(tmpSchemaLocation, configMap.get("location"));

    batch.release();
    loader.clear();
  }

  @Test
  public void describeSchemaInvalid() throws Exception {
    errorMsgTestHelper("describe schema invalid.schema", "Invalid schema name [invalid.schema]");
  }

}
