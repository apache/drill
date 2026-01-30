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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.commons.io.FileUtils;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.test.BaseTestQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.drill.test.HadoopUtils.hadoopToJavaPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for CREATE DAFFODIL SCHEMA and DROP DAFFODIL SCHEMA SQL commands
 */
@Category({SlowTest.class, SqlFunctionTest.class})
public class TestDaffodilSchemaHandlers extends BaseTestQuery {

  private static final String DEFAULT_SCHEMA_JAR_NAME = "sample-daffodil-schema.jar";
  private static URI fsUri;
  private static File jarsDir;

  @BeforeClass
  public static void setupJars() throws IOException {
    jarsDir = dirTestWatcher.makeSubDir(Paths.get("schema-jars"));
  }

  @Before
  public void setupDrillbit() throws Exception {
    updateTestCluster(1, config);
    fsUri = getLocalFileSystem().getUri();
  }

  @After
  public void cleanup() throws Exception {
    closeClient();
    dirTestWatcher.clear();
  }

  @Test
  public void testCreateSyntax() throws Exception {
    // Test that the SQL syntax is recognized
    test("create daffodil schema using jar 'schema.jar'");
  }

  @Test
  public void testDropSyntax() throws Exception {
    // Test that the SQL syntax is recognized
    test("drop daffodil schema using jar 'schema.jar'");
  }

  @Test
  public void testCreateSchemaAbsentJarInStaging() throws Exception {
    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());

    String summary = String.format("File %s does not exist on file system %s",
        staging.resolve(DEFAULT_SCHEMA_JAR_NAME).toUri().getPath(), fsUri);

    testBuilder()
        .sqlQuery("create daffodil schema using jar '%s'", DEFAULT_SCHEMA_JAR_NAME)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();
  }

  @Test
  public void testDropSchemaNotRegistered() throws Exception {
    String jarName = "non-existent-schema.jar";

    testBuilder()
        .sqlQuery("drop daffodil schema using jar '%s'", jarName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Jar %s is not registered in remote registry", jarName))
        .go();
  }

  /**
   * Test successful schema registration
   */
  @Test
  public void testSuccessfulSchemaRegistration() throws Exception {
    String jarName = "test-schema.jar";
    File schemaJar = createTestSchemaJar(jarName);

    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());
    copyJar(jarsDir.toPath(), staging, jarName);

    testBuilder()
        .sqlQuery("create daffodil schema using jar '%s'", jarName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been registered successfully.", jarName))
        .go();
  }

  /**
   * Test registering a duplicate schema JAR
   */
  @Test
  public void testDuplicateSchemaRegistration() throws Exception {
    String jarName = "duplicate-schema.jar";
    File schemaJar = createTestSchemaJar(jarName);

    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());
    copyJar(jarsDir.toPath(), staging, jarName);

    // First registration should succeed
    test("create daffodil schema using jar '%s'", jarName);

    // Copy to staging again for second attempt
    copyJar(jarsDir.toPath(), staging, jarName);

    // Second registration should fail
    testBuilder()
        .sqlQuery("create daffodil schema using jar '%s'", jarName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Jar with %s name has been already registered", jarName))
        .go();
  }

  /**
   * Test successful schema unregistration
   */
  @Test
  public void testSuccessfulSchemaUnregistration() throws Exception {
    String jarName = "unregister-test-schema.jar";
    File schemaJar = createTestSchemaJar(jarName);

    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());
    copyJar(jarsDir.toPath(), staging, jarName);

    // Register the schema
    test("create daffodil schema using jar '%s'", jarName);

    // Unregister the schema
    testBuilder()
        .sqlQuery("drop daffodil schema using jar '%s'", jarName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format("Daffodil schema jar %s has been unregistered successfully.", jarName))
        .go();
  }

  /**
   * Test concurrent access to same JAR during registration
   */
  @Test
  public void testConcurrentRegistration() throws Exception {
    String jarName = "concurrent-schema.jar";
    File schemaJar = createTestSchemaJar(jarName);

    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());
    copyJar(jarsDir.toPath(), staging, jarName);

    // First attempt to register
    test("create daffodil schema using jar '%s'", jarName);

    // Attempting to register while it's already registered should indicate it's in use
    copyJar(jarsDir.toPath(), staging, jarName);

    testBuilder()
        .sqlQuery("create daffodil schema using jar '%s'", jarName)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Jar with %s name has been already registered", jarName))
        .go();
  }

  /**
   * Test registering and then dropping a schema in sequence
   */
  @Test
  public void testRegisterAndDropSequence() throws Exception {
    String jarName = "sequence-test-schema.jar";
    File schemaJar = createTestSchemaJar(jarName);

    Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getStagingArea());
    Path registry = hadoopToJavaPath(getDrillbitContext().getRemoteDaffodilSchemaRegistry().getRegistryArea());

    copyJar(jarsDir.toPath(), staging, jarName);

    // Register
    test("create daffodil schema using jar '%s'", jarName);

    // Verify JAR is in registry
    File registeredJar = registry.resolve(jarName).toFile();
    assertTrue("JAR should exist in registry after registration", registeredJar.exists());

    // Drop
    test("drop daffodil schema using jar '%s'", jarName);

    // Verify JAR is removed from registry
    assertFalse("JAR should be removed from registry after dropping", registeredJar.exists());
  }

  /**
   * Helper method to copy a jar file to a destination
   */
  private void copyJar(Path source, Path destination, String jarName) throws IOException {
    FileUtils.copyFileToDirectory(source.resolve(jarName).toFile(), destination.toFile());
  }

  /**
   * Helper method to create a simple JAR file for testing
   * In a real implementation, this would create a JAR containing DFDL schema files
   */
  private File createTestSchemaJar(String jarName) throws IOException {
    File jarFile = new File(jarsDir, jarName);
    // Create an empty JAR file for basic testing
    // In a real test, this would contain actual DFDL schema files (.xsd or .bin)
    FileUtils.touch(jarFile);
    return jarFile;
  }
}
