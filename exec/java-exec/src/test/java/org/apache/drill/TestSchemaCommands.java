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
package org.apache.drill;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.schema.PathSchemaProvider;
import org.apache.drill.exec.record.metadata.schema.SchemaContainer;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(SqlTest.class)
public class TestSchemaCommands extends ClusterTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testCreateWithoutSchema() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Lexical error");

    run("create schema for");
  }

  @Test
  public void testCreateWithForAndPath() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Encountered \"path\"");

    run("create schema ( col1 int, col2 int) for table tbl path '/tmp/schema.file'");
  }

  @Test
  public void testCreateWithPathAndOrReplace() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: <OR REPLACE> cannot be used with <PATH> property");

    run("create or replace schema (col1 int, col2 int) path '/tmp/schema.file'");
  }

  @Test
  public void testCreateForMissingTable() throws Exception {
    String table = "dfs.tmp.tbl";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Table [tbl] was not found");

    run("create schema (col1 int, col2 int) for table %s", table);
  }

  @Test
  public void testCreateForTemporaryTable() throws Exception {
    String table = "temp_create";
    try {
      run("create temporary table %s as select 'a' as c from (values(1))", table);
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Indicated table [%s] is temporary table", table));

      run("create schema (col1 int, col2 int) for table %s", table);
    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testCreateForImmutableSchema() throws Exception {
    String table = "sys.version";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Unable to create or drop objects. Schema [sys] is immutable");

    run("create schema (col1 int, col2 int) for table %s", table);
  }

  @Test
  public void testMissingDirectory() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    Path schema = new Path(Paths.get(tmpDir.getPath(), "missing_parent_directory", "file.schema").toFile().getPath());

    thrown.expect(UserException.class);
    thrown.expectMessage(String.format("RESOURCE ERROR: Parent path for schema file [%s] does not exist", schema.toUri().getPath()));

    run("create schema (col1 int, col2 int) path '%s'", schema.toUri().getPath());
  }

  @Test
  public void testTableAsFile() throws Exception {
    File tmpDir = dirTestWatcher.getDfsTestTmpDir();
    String table = "test_table_as_file.json";
    File tablePath = new File(tmpDir, table);
    assertTrue(tablePath.createNewFile());

    thrown.expect(UserException.class);
    thrown.expectMessage(String.format("RESOURCE ERROR: Indicated table [%s] must be a directory",
      String.format("dfs.tmp.%s", table)));

    try {
      run("create schema (col1 int, col2 int) for table %s.`%s`", "dfs.tmp", table);
    } finally {
      assertTrue(tablePath.delete());
    }
  }

  @Test
  public void testCreateSimpleForPathWithExistingSchema() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schema = new File(tmpDir, "simple_for_path.schema");
    assertTrue(schema.createNewFile());

    thrown.expect(UserException.class);
    thrown.expectMessage(String.format("VALIDATION ERROR: Schema already exists for [%s]", schema.getPath()));

    try {
      run("create schema (col1 int, col2 int) path '%s'", schema.getPath());
    } finally {
      assertTrue(schema.delete());
    }
  }

  @Test
  public void testCreateSimpleForTableWithExistingSchema() throws Exception {
    String table = "dfs.tmp.table_for_simple_existing_schema";
    try {
      run("create table %s as select 'a' as c from (values(1))", table);
      testBuilder()
        .sqlQuery("create schema (c varchar not null) for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", table))
        .go();

      thrown.expect(UserRemoteException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Schema already exists for [%s]", table));
      run("create schema (c varchar not null) for table %s", table);
    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testSuccessfulCreateForPath() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schemaFile = new File(tmpDir, "schema_for_successful_create_for_path.schema");
    assertFalse(schemaFile.exists());
    try {
      testBuilder()
        .sqlQuery("create schema (i int not null, v varchar) path '%s'", schemaFile.getPath())
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", schemaFile.getPath()))
        .go();

      SchemaProvider schemaProvider = new PathSchemaProvider(new Path(schemaFile.getPath()));
      assertTrue(schemaProvider.exists());

      SchemaContainer schemaContainer = schemaProvider.read();

      assertNull(schemaContainer.getTable());
      assertNotNull(schemaContainer.getSchema());

      TupleMetadata schema = schemaContainer.getSchema();
      ColumnMetadata intColumn = schema.metadata("i");
      assertFalse(intColumn.isNullable());
      assertEquals(TypeProtos.MinorType.INT, intColumn.type());

      ColumnMetadata varcharColumn = schema.metadata("v");
      assertTrue(varcharColumn.isNullable());
      assertEquals(TypeProtos.MinorType.VARCHAR, varcharColumn.type());
    } finally {
      if (schemaFile.exists()) {
        assertTrue(schemaFile.delete());
      }
    }
  }

  @Test
  public void testSuccessfulCreateOrReplaceForTable() throws Exception {
    String tableName = "table_for_successful_create_or_replace_for_table";
    String table = String.format("dfs.tmp.%s", tableName);
    try {
      run("create table %s as select 'a' as c from (values(1))", table);

      File schemaPath = Paths.get(dirTestWatcher.getDfsTestTmpDir().getPath(),
        tableName, SchemaProvider.DEFAULT_SCHEMA_NAME).toFile();

      assertFalse(schemaPath.exists());

      testBuilder()
        .sqlQuery("create schema (c varchar not null) for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", table))
        .go();

      SchemaProvider schemaProvider = new PathSchemaProvider(new Path(schemaPath.getPath()));
      assertTrue(schemaProvider.exists());

      SchemaContainer schemaContainer = schemaProvider.read();
      assertNotNull(schemaContainer.getTable());
      assertEquals(String.format("dfs.tmp.`%s`", tableName), schemaContainer.getTable());

      assertNotNull(schemaContainer.getSchema());
      ColumnMetadata column = schemaContainer.getSchema().metadata("c");
      assertFalse(column.isNullable());
      assertEquals(TypeProtos.MinorType.VARCHAR, column.type());

      testBuilder()
        .sqlQuery("create or replace schema (c varchar) for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", table))
        .go();

      assertTrue(schemaProvider.exists());

      SchemaContainer updatedSchemaContainer = schemaProvider.read();
      assertNotNull(updatedSchemaContainer.getTable());
      assertEquals(String.format("dfs.tmp.`%s`", tableName), updatedSchemaContainer.getTable());

      assertNotNull(updatedSchemaContainer.getSchema());
      ColumnMetadata updatedColumn = updatedSchemaContainer.getSchema().metadata("c");
      assertTrue(updatedColumn.isNullable());
      assertEquals(TypeProtos.MinorType.VARCHAR, updatedColumn.type());

    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testCreateWithProperties() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schemaFile = new File(tmpDir, "schema_for_create_with_properties.schema");
    assertFalse(schemaFile.exists());
    try {
      testBuilder()
        .sqlQuery("create schema (i int not null) path '%s' " +
            "properties ('k1' = 'v1', 'k2' = 'v2', 'k3' = 'v3')", schemaFile.getPath())
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", schemaFile.getPath()))
        .go();

      SchemaProvider schemaProvider = new PathSchemaProvider(new Path(schemaFile.getPath()));
      assertTrue(schemaProvider.exists());

      SchemaContainer schemaContainer = schemaProvider.read();

      assertNull(schemaContainer.getTable());
      assertNotNull(schemaContainer.getSchema());
      assertNotNull(schemaContainer.getProperties());

      Map<String, String> properties = new LinkedHashMap<>();
      properties.put("k1", "v1");
      properties.put("k2", "v2");
      properties.put("k3", "v3");

      assertEquals(properties.size(), schemaContainer.getProperties().size());
      assertEquals(properties, schemaContainer.getProperties());

    } finally {
      if (schemaFile.exists()) {
        assertTrue(schemaFile.delete());
      }
    }
  }

  @Test
  public void testCreateWithoutProperties() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File schemaFile = new File(tmpDir, "schema_for_create_without_properties.schema");
    assertFalse(schemaFile.exists());
    try {
      testBuilder()
        .sqlQuery("create schema (i int not null) path '%s'", schemaFile.getPath())
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", schemaFile.getPath()))
        .go();

      SchemaProvider schemaProvider = new PathSchemaProvider(new Path(schemaFile.getPath()));
      assertTrue(schemaProvider.exists());

      SchemaContainer schemaContainer = schemaProvider.read();

      assertNull(schemaContainer.getTable());
      assertNotNull(schemaContainer.getSchema());
      assertNotNull(schemaContainer.getProperties());
      assertEquals(0, schemaContainer.getProperties().size());
    } finally {
      if (schemaFile.exists()) {
        assertTrue(schemaFile.delete());
      }
    }
  }

  @Test
  public void testCreateUsingLoadFromMissingFile() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("RESOURCE ERROR: File with raw schema [path/to/file] does not exist");

    run("create schema load 'path/to/file' for table dfs.tmp.t");
  }

  @Test
  public void testCreateUsingLoad() throws Exception {
    File tmpDir = dirTestWatcher.getTmpDir();
    File rawSchema = new File(tmpDir, "raw.schema");
    File schemaFile = new File(tmpDir, "schema_for_create_using_load.schema");
    try {
      Files.write(rawSchema.toPath(), Arrays.asList(
        "i int,",
        "v varchar"
      ));

      assertTrue(rawSchema.exists());

      testBuilder()
        .sqlQuery("create schema load '%s' path '%s' properties ('k1'='v1', 'k2' = 'v2')",
          rawSchema.getPath(), schemaFile.getPath())
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", schemaFile.getPath()))
        .go();

      SchemaProvider schemaProvider = new PathSchemaProvider(new Path(schemaFile.getPath()));
      assertTrue(schemaFile.exists());

      SchemaContainer schemaContainer = schemaProvider.read();

      assertNull(schemaContainer.getTable());

      TupleMetadata schema = schemaContainer.getSchema();
      assertNotNull(schema);

      assertEquals(2, schema.size());
      assertEquals(TypeProtos.MinorType.INT, schema.metadata("i").type());
      assertEquals(TypeProtos.MinorType.VARCHAR, schema.metadata("v").type());

      assertNotNull(schemaContainer.getProperties());
      assertEquals(2, schemaContainer.getProperties().size());
    } finally {
      if (rawSchema.exists()) {
        assertTrue(rawSchema.delete());
      }
    }
  }

  @Test
  public void testDropWithoutTable() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("PARSE ERROR: Encountered \"<EOF>\"");

    run("drop schema");
  }

  @Test
  public void testDropForMissingTable() throws Exception {
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Table [t] was not found");

    run("drop schema for table dfs.t");
  }

  @Test
  public void testDropForTemporaryTable() throws Exception {
    String table = "temp_drop";
    try {
      run("create temporary table %s as select 'a' as c from (values(1))", table);
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Indicated table [%s] is temporary table", table));

      run("drop schema for table %s", table);
    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testDropForImmutableSchema() throws Exception {
    String table = "sys.version";
    thrown.expect(UserException.class);
    thrown.expectMessage("VALIDATION ERROR: Unable to create or drop objects. Schema [sys] is immutable");

    run("drop schema for table %s", table);
  }

  @Test
  public void testDropForMissingSchema() throws Exception {
    String table = "dfs.tmp.table_with_missing_schema";
    try {
      run("create table %s as select 'a' as c from (values(1))", table);
      thrown.expect(UserException.class);
      thrown.expectMessage(String.format("VALIDATION ERROR: Schema [%s] " +
        "does not exist in table [%s] root directory", SchemaProvider.DEFAULT_SCHEMA_NAME, table));

      run("drop schema for table %s", table);
    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testDropForMissingSchemaIfExists() throws Exception {
    String table = "dfs.tmp.table_with_missing_schema_if_exists";
    try {
      run("create table %s as select 'a' as c from (values(1))", table);

      testBuilder()
        .sqlQuery("drop schema if exists for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Schema [%s] does not exist in table [%s] root directory",
          SchemaProvider.DEFAULT_SCHEMA_NAME, table))
        .go();
    } finally {
      run("drop table if exists %s", table);
    }
  }

  @Test
  public void testSuccessfulDrop() throws Exception {
    String tableName = "table_for_successful_drop";
    String table = String.format("dfs.tmp.%s", tableName);

    try {
      run("create table %s as select 'a' as c from (values(1))", table);

      File schemaPath = Paths.get(dirTestWatcher.getDfsTestTmpDir().getPath(),
        tableName, SchemaProvider.DEFAULT_SCHEMA_NAME).toFile();

      assertFalse(schemaPath.exists());

      testBuilder()
        .sqlQuery("create schema (c varchar not null) for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Created schema for [%s]", table))
        .go();

      assertTrue(schemaPath.exists());

      testBuilder()
        .sqlQuery("drop schema for table %s", table)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Dropped schema for table [%s]", table))
        .go();

      assertFalse(schemaPath.exists());
    } finally {
      run("drop table if exists %s", table);
    }
  }

}
