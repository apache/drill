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

package org.apache.drill.exec.store.daffodil;

import org.apache.commons.io.FileUtils;
import org.apache.drill.categories.RowSetTest;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.drill.test.HadoopUtils.hadoopToJavaPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test for the complete Daffodil schema registration workflow.
 * This test verifies:
 * 1. Registering a DFDL schema using CREATE DAFFODIL SCHEMA (both JAR and XSD formats)
 * 2. Querying data files using the registered schema
 * 3. Unregistering the schema using DROP DAFFODIL SCHEMA
 */
@Category(RowSetTest.class)
public class TestDaffodilSchemaRegistrationIntegration extends ClusterTest {

  private static Path stagingArea;
  private static File schemaResourceDir;

  @BeforeClass
  public static void setup() throws Exception {
    // Start the test cluster
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Define the Daffodil format
    DaffodilFormatConfig formatConfig = new DaffodilFormatConfig(List.of("dat"), "", "", "", "", false);
    cluster.defineFormat("dfs", "daffodil", formatConfig);

    // Copy test data and schema files to the test directory
    dirTestWatcher.copyResourceToRoot(Paths.get("data/"));
    dirTestWatcher.copyResourceToRoot(Paths.get("schema/"));

    // Get the staging area for schema files
    stagingArea = hadoopToJavaPath(cluster.drillbit().getContext()
        .getRemoteDaffodilSchemaRegistry().getStagingArea());

    // Locate the schema resource directory
    try {
      schemaResourceDir = Paths.get(
          TestDaffodilSchemaRegistrationIntegration.class
              .getClassLoader()
              .getResource("schema/")
              .toURI()
      ).toFile();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to locate test schema directory", e);
    }
  }

  /**
   * End-to-end test that:
   * 1. Registers a DFDL schema XSD file using CREATE DAFFODIL SCHEMA
   * 2. Queries a data file using the registered schema
   * 3. Verifies the query results
   */
  @Test
  public void testRegisterXsdSchemaAndQuery() throws Exception {
    String schemaFileName = "simple.dfdl.xsd";
    File sourceSchema = new File(schemaResourceDir, schemaFileName);

    // Copy the schema XSD to the staging area
    copyFileToStaging(sourceSchema, stagingArea, schemaFileName);

    // Step 1: Register the schema
    client.testBuilder()
        .sqlQuery("CREATE DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been registered successfully.", schemaFileName))
        .go();

    // Verify schema was moved from staging to registry
    File stagingFile = stagingArea.resolve(schemaFileName).toFile();
    Path registryArea = hadoopToJavaPath(cluster.drillbit().getContext()
        .getRemoteDaffodilSchemaRegistry().getRegistryArea());
    File registryFile = registryArea.resolve(schemaFileName).toFile();

    assertFalse("Schema file should be removed from staging after registration", stagingFile.exists());
    assertTrue("Schema file should exist in registry after registration", registryFile.exists());

    // Step 2: Query data using the registered schema
    // After CREATE DAFFODIL SCHEMA, the schema file is moved from staging to the registry area
    // Use schemaFile parameter (just the filename) - Drill will automatically look it up in the registry
    String query = "SELECT * FROM table(dfs.`data/data01Int.dat` " +
        "(type => 'daffodil', " +
        "validationMode => 'true', " +
        "schemaFile => '" + schemaFileName + "', " +
        "rootName => 'row', " +
        "rootNamespace => null))";

    QueryBuilder qb = client.queryBuilder();
    RowSet results = qb.sql(query).rowSet();

    // Step 3: Verify results
    assertEquals(1, results.rowCount());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(0x00000101) // aka 257
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);

    // Clean up - drop the schema
    client.testBuilder()
        .sqlQuery("DROP DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been unregistered successfully.", schemaFileName))
        .go();
  }

  /**
   * Test the complete lifecycle: register, query, then unregister
   */
  @Test
  public void testCompleteSchemaLifecycle() throws Exception {
    String schemaFileName = "complex1.dfdl.xsd";
    File sourceSchema = new File(schemaResourceDir, schemaFileName);

    // Copy to staging area
    copyFileToStaging(sourceSchema, stagingArea, schemaFileName);

    // Register the schema
    client.testBuilder()
        .sqlQuery("CREATE DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been registered successfully.", schemaFileName))
        .go();

    // Unregister the schema
    client.testBuilder()
        .sqlQuery("DROP DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been unregistered successfully.", schemaFileName))
        .go();

    // Verify the file is removed from the registry
    Path registryArea = hadoopToJavaPath(cluster.drillbit().getContext()
        .getRemoteDaffodilSchemaRegistry().getRegistryArea());
    File registeredFile = registryArea.resolve(schemaFileName).toFile();
    assertTrue("Schema file should be removed from registry after dropping", !registeredFile.exists());
  }

  /**
   * Test querying with multiple rows of data using a registered schema
   */
  @Test
  public void testQueryMultipleRowsWithRegisteredSchema() throws Exception {
    String schemaFileName = "simple.dfdl.xsd";
    File sourceSchema = new File(schemaResourceDir, schemaFileName);

    // Copy to staging area and register
    copyFileToStaging(sourceSchema, stagingArea, schemaFileName);

    client.testBuilder()
        .sqlQuery("CREATE DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been registered successfully.", schemaFileName))
        .go();

    // Query data with 6 rows using the registered schema
    // Use schemaFile parameter (just the filename) - Drill will automatically look it up in the registry
    String query = "SELECT * FROM table(dfs.`data/data06Int.dat` " +
        "(type => 'daffodil', " +
        "validationMode => 'true', " +
        "schemaFile => '" + schemaFileName + "', " +
        "rootName => 'row', " +
        "rootNamespace => null))";

    QueryBuilder qb = client.queryBuilder();
    RowSet results = qb.sql(query).rowSet();

    // Verify results
    assertEquals(6, results.rowCount());

    TupleMetadata expectedSchema = new SchemaBuilder()
        .add("col", MinorType.INT)
        .buildSchema();

    RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow(0x00000101)
        .addRow(0x00000102)
        .addRow(0x00000103)
        .addRow(0x00000104)
        .addRow(0x00000105)
        .addRow(0x00000106)
        .build();

    new RowSetComparison(expected).verifyAndClearAll(results);

    // Clean up - drop the schema
    client.testBuilder()
        .sqlQuery("DROP DAFFODIL SCHEMA USING JAR '%s'", schemaFileName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been unregistered successfully.", schemaFileName))
        .go();
  }

  /**
   * Helper method to copy a file to the staging area
   */
  private void copyFileToStaging(File sourceFile, Path destination, String fileName) throws IOException {
    File destFile = destination.resolve(fileName).toFile();

    // Ensure the staging directory exists
    if (!destination.toFile().exists()) {
      destination.toFile().mkdirs();
    }

    FileUtils.copyFile(sourceFile, destFile);
  }
}
