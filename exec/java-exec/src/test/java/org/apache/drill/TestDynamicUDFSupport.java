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
package org.apache.drill;

import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.RemoteFunctionRegistry;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.util.JarUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestDynamicUDFSupport extends BaseTestQuery {

  private static final File jars = new File(TestTools.getWorkingPath() + "/src/test/resources/jars");
  private static final String jName = "DrillUDF-1.0.jar";
  private static String sName = JarUtil.getSourceName(jName);

  @Rule
  public final TemporaryFolder drillUdfDir = new TemporaryFolder();

  @Rule
  public final TemporaryFolder base = new TemporaryFolder();

  @Before
  public void setEnvVariables()
  {
    new NonStrictExpectations(System.class)
    {
      {
        invoke(System.class, "getenv", "DRILL_UDF_DIR");
        returns(drillUdfDir.getRoot().getPath());
      }
    };
  }

  @Before
  public void setup() {
    String root = base.getRoot().getPath();
    if (!root.equals(getDrillbitContext().getConfig().getString(ExecConstants.UDF_DIRECTORY_STAGING))) {
    Properties overrideProps = new Properties();
      overrideProps.setProperty(ExecConstants.UDF_DIRECTORY_BASE, root);
      updateTestCluster(1, DrillConfig.create(overrideProps));
    }
  }

  @Test
  public void testSyntax() throws Exception {
    test("create function using jar 'jar_name.jar'");
    test("drop function using jar 'jar_name.jar'");
  }

  @Test
  public void testEnableDynamicSupport() throws Exception {
    try {
      test("alter system set `exec.udf.enable_dynamic_support` = true");
      test("create function using jar 'jar_name.jar'");
      test("drop function using jar 'jar_name.jar'");
    } finally {
      test("alter system reset `exec.udf.enable_dynamic_support`");
    }
  }

  @Test
  public void testDisableDynamicSupport() throws Exception {
    try {
      test("alter system set `exec.udf.enable_dynamic_support` = false");
      String[] actions = new String[] {"create", "drop"};
      String query = "%s function using jar 'jar_name.jar'";
      for (String action : actions) {
        try {
          test(String.format(query, action));
        } catch (UserRemoteException e) {
          assertThat(e.getMessage(), containsString("Dynamic UDFs support is disabled."));
        }
      }
    } finally {
      test("alter system reset `exec.udf.enable_dynamic_support`");
    }
  }

  @Test
  public void testAbsentJarInStaging() throws Exception {
    String jar = "jar_name.jar";
    String staging = getDrillbitContext().getRemoteFunctionRegistry().getStagingArea().toUri().getPath();
    String summary = String.format("Binary [%s] or source [%s-sources.jar] is absent in udf staging area [%s].", jar, jar.split("\\.")[0], staging);

    testBuilder()
        .sqlQuery(String.format("create function using jar '%s'", jar))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();
  }

  @Test
  public void testSuccessfulCreate() throws Exception {
    copyJarsToStagingArea();

    String summary = "The following UDFs in jar %s have been registered:\n" +
        "[name: \"custom_lower\"\n" +
        "major_type {\n" +
        "  minor_type: VARCHAR\n" +
        "  mode: REQUIRED\n" +
        "}\n" +
        "]";

    testBuilder()
        .sqlQuery(String.format("create function using jar '%s'", jName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format(summary, jName))
        .go();

    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();
    FileSystem fs = remoteFunctionRegistry.getFs();

    assertFalse("Staging area should be empty", fs.listFiles(remoteFunctionRegistry.getStagingArea(), false).hasNext());
    assertFalse("Temporary area should be empty", fs.listFiles(remoteFunctionRegistry.getTmpArea(), false).hasNext());

    assertTrue("Binary should be present in registry area", fs.exists(new Path(remoteFunctionRegistry.getRegistryArea(), jName)));
    assertTrue("Source should be present in registry area", fs.exists(new Path(remoteFunctionRegistry.getRegistryArea(), sName)));

    Registry registry = remoteFunctionRegistry.getRegistry();
    assertEquals("Registry should contain one jar", registry.getJarList().size(), 1);
    assertEquals(registry.getJar(0).getName(), jName);
    test(String.format("drop function using jar '%s'", jName));
  }

  @Test
  public void testDuplicatedJar() throws Exception {
    copyJarsToStagingArea();
    test(String.format("create function using jar '%s'", jName));
    copyJarsToStagingArea();

    testBuilder()
        .sqlQuery(String.format("create function using jar '%s'", jName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Jar with %s name has been already registered", jName))
        .go();

    test(String.format("drop function using jar '%s'", jName));
  }

  @Test
  public void testDuplicatedFunctions() throws Exception {
    copyJarsToStagingArea();
    test(String.format("create function using jar '%s'", jName));
    copyJarsToStagingArea();

    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();
    FileSystem fs = remoteFunctionRegistry.getFs();
    String newJar = "DrillUDF-2.0.jar";
    String newSource = JarUtil.getSourceName(newJar);
    Path renamedBinary = new Path(remoteFunctionRegistry.getStagingArea(), newJar);
    Path renamedSource = new Path(remoteFunctionRegistry.getStagingArea(), newSource);
    fs.rename(new Path(remoteFunctionRegistry.getStagingArea(), jName), renamedBinary);
    fs.rename(new Path(remoteFunctionRegistry.getStagingArea(), sName), renamedSource);

    String summary = "Found duplicated function in %s - name: \"custom_lower\"\n" +
        "major_type {\n" +
        "  minor_type: VARCHAR\n" +
        "  mode: REQUIRED\n" +
        "}\n";

    testBuilder()
        .sqlQuery(String.format("create function using jar '%s'", newJar))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, jName))
        .go();

    fs.delete(renamedBinary, false);
    fs.delete(renamedSource, false);
    test(String.format("drop function using jar '%s'", jName));
  }

  @Test
  public void testLazyInit() throws Exception {
    try {
      test("select custom_lower(version) from sys.version");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature custom_lower(<CHARACTER>)"));
    }

    copyJarsToStagingArea();

    test(String.format("create function using jar '%s'", jName));
    test("select custom_lower(version) from sys.version");

    assertTrue("Binary should be present in local UDF area", new File(System.getenv("DRILL_UDF_DIR"), jName).exists());
    assertTrue("Source should be present in local UDF area", new File(System.getenv("DRILL_UDF_DIR"), sName).exists());
    test(String.format("drop function using jar '%s'", jName));
  }

  @Test
  public void testDropFunction() throws Exception {
    copyJarsToStagingArea();

    test(String.format("create function using jar '%s'", jName));
    test("select custom_lower(version) from sys.version");

    String summary = "The following UDFs in jar %s have been unregistered:\n" +
        "[name: \"custom_lower\"\n" +
        "major_type {\n" +
        "  minor_type: VARCHAR\n" +
        "  mode: REQUIRED\n" +
        "}\n" +
        "]";

    testBuilder()
        .sqlQuery(String.format("drop function using jar '%s'", jName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format(summary, jName))
        .go();

    try {
      test("select custom_lower(version) from sys.version");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature custom_lower(<CHARACTER>)"));
    }

    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();

    Registry registry = remoteFunctionRegistry.getRegistry();
    assertEquals("Remote registry should be empty", registry.getJarList().size(), 0);

    FileSystem fs = remoteFunctionRegistry.getFs();

    assertFalse("Binary should not be present in registry area", fs.exists(new Path(remoteFunctionRegistry.getRegistryArea(), jName)));
    assertFalse("Source should not be present in registry area", fs.exists(new Path(remoteFunctionRegistry.getRegistryArea(), sName)));

    assertFalse("Binary should not be present in local UDF area", new File(System.getenv("DRILL_UDF_DIR"), jName).exists());
    assertFalse("Source should not be present in local UDF area", new File(System.getenv("DRILL_UDF_DIR"), sName).exists());
    test(String.format("drop function using jar '%s'", jName));
  }

  @Test
  public void testDropAbsentJar() throws Exception {
    testBuilder().sqlQuery(String.format("drop function using jar '%s'", jName))
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format("Jar %s is not registered in remote registry", jName))
        .go();
  }

  private void copyJarsToStagingArea() throws IOException {
    Path jarPath = new Path(jars.toURI().getPath(), jName);
    Path sourcePath = new Path(jars.toURI().getPath(), sName);

    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();
    FileSystem fs = remoteFunctionRegistry.getFs();

    fs.copyFromLocalFile(jarPath, remoteFunctionRegistry.getStagingArea());
    fs.copyFromLocalFile(sourcePath, remoteFunctionRegistry.getStagingArea());
  }

}
