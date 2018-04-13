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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.test.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.VersionMismatchException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.registry.LocalFunctionRegistry;
import org.apache.drill.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.store.DataChangeVersion;
import org.apache.drill.exec.util.JarUtil;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.TestBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.drill.test.HadoopUtils.hadoopToJavaPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
@Category({SlowTest.class, SqlFunctionTest.class})
public class TestDynamicUDFSupport extends BaseTestQuery {

  private static final Path jars = TestTools.WORKING_PATH
    .resolve(TestTools.TEST_RESOURCES)
    .resolve("jars");
  private static final String default_binary_name = "DrillUDF-1.0.jar";
  private static final String UDF_SUB_DIR = "udf";
  private static final String default_source_name = JarUtil.getSourceName(default_binary_name);
  private static URI fsUri;
  private static File udfDir;

  @BeforeClass
  public static void setup() throws IOException {
    udfDir = dirTestWatcher.makeSubDir(Paths.get(UDF_SUB_DIR));

    Properties overrideProps = new Properties();
    overrideProps.setProperty(ExecConstants.UDF_DIRECTORY_ROOT, udfDir.getAbsolutePath());
    overrideProps.setProperty(ExecConstants.UDF_DIRECTORY_FS, FileSystem.DEFAULT_FS);
    updateTestCluster(1, DrillConfig.create(overrideProps));

    fsUri = getLocalFileSystem().getUri();
  }

  @Rule
  public final TestWatcher clearDirs = new TestWatcher() {
    @Override
    protected void succeeded(Description description) {
      reset();
    }

    @Override
    protected void failed(Throwable e, Description description) {
      reset();
    }

    private void reset() {
      try {
        closeClient();
        FileUtils.cleanDirectory(udfDir);
        dirTestWatcher.clear();
        setup();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

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
          test(query, action);
        } catch (UserRemoteException e) {
          assertThat(e.getMessage(), containsString("Dynamic UDFs support is disabled."));
        }
      }
    } finally {
      test("alter system reset `exec.udf.enable_dynamic_support`");
    }
  }

  @Test
  public void testAbsentBinaryInStaging() throws Exception {
    final Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteFunctionRegistry().getStagingArea());

    String summary = String.format("File %s does not exist on file system %s",
        staging.resolve(default_binary_name).toUri().getPath(), fsUri);

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();
  }

  @Test
  public void testAbsentSourceInStaging() throws Exception {
    final Path staging = hadoopToJavaPath(getDrillbitContext().getRemoteFunctionRegistry().getStagingArea());

    copyJar(jars, staging, default_binary_name);

    String summary = String.format("File %s does not exist on file system %s",
        staging.resolve(default_source_name).toUri().getPath(), fsUri);

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();
  }

  @Test
  public void testJarWithoutMarkerFile() throws Exception {
    String jarWithNoMarkerFile = "DrillUDF_NoMarkerFile-1.0.jar";
    copyJarsToStagingArea(jarWithNoMarkerFile, JarUtil.getSourceName(jarWithNoMarkerFile));

    String summary = "Marker file %s is missing in %s";

    testBuilder()
        .sqlQuery("create function using jar '%s'", jarWithNoMarkerFile)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary,
            CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, jarWithNoMarkerFile))
        .go();
  }

  @Test
  public void testJarWithoutFunctions() throws Exception {
    String jarWithNoFunctions = "DrillUDF_Empty-1.0.jar";
    copyJarsToStagingArea(jarWithNoFunctions, JarUtil.getSourceName(jarWithNoFunctions));

    String summary = "Jar %s does not contain functions";

    testBuilder()
        .sqlQuery("create function using jar '%s'", jarWithNoFunctions)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, jarWithNoFunctions))
        .go();
  }

  @Test
  public void testSuccessfulRegistration() throws Exception {
    copyDefaultJarsToStagingArea();

    String summary = "The following UDFs in jar %s have been registered:\n" +
        "[custom_lower(VARCHAR-REQUIRED)]";

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format(summary, default_binary_name))
        .go();

    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();
    FileSystem fs = remoteFunctionRegistry.getFs();

    assertFalse("Staging area should be empty", fs.listFiles(remoteFunctionRegistry.getStagingArea(), false).hasNext());
    assertFalse("Temporary area should be empty", fs.listFiles(remoteFunctionRegistry.getTmpArea(), false).hasNext());

    final Path path = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());

    assertTrue("Binary should be present in registry area",
      path.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should be present in registry area",
      path.resolve(default_source_name).toFile().exists());

    Registry registry = remoteFunctionRegistry.getRegistry(new DataChangeVersion());
    assertEquals("Registry should contain one jar", registry.getJarList().size(), 1);
    assertEquals(registry.getJar(0).getName(), default_binary_name);
  }

  @Test
  public void testDuplicatedJarInRemoteRegistry() throws Exception {
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);
    copyDefaultJarsToStagingArea();

    String summary = "Jar with %s name has been already registered";

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, default_binary_name))
        .go();
  }

  @Test
  public void testDuplicatedJarInLocalRegistry() throws Exception {
    copyDefaultJarsToStagingArea();

    test("create function using jar '%s'", default_binary_name);
    test("select custom_lower('A') from (values(1))");

    copyDefaultJarsToStagingArea();

    String summary = "Jar with %s name has been already registered";

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, default_binary_name))
        .go();
  }

  @Test
  public void testDuplicatedFunctionsInRemoteRegistry() throws Exception {
    String jarWithDuplicate = "DrillUDF_Copy-1.0.jar";
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);
    copyJarsToStagingArea(jarWithDuplicate, JarUtil.getSourceName(jarWithDuplicate));

    String summary = "Found duplicated function in %s: custom_lower(VARCHAR-REQUIRED)";

    testBuilder()
        .sqlQuery("create function using jar '%s'", jarWithDuplicate)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, default_binary_name))
        .go();
  }

  @Test
  public void testDuplicatedFunctionsInLocalRegistry() throws Exception {
    String jarWithDuplicate = "DrillUDF_DupFunc-1.0.jar";
    copyJarsToStagingArea(jarWithDuplicate, JarUtil.getSourceName(jarWithDuplicate));

    String summary = "Found duplicated function in %s: lower(VARCHAR-REQUIRED)";

    testBuilder()
        .sqlQuery("create function using jar '%s'", jarWithDuplicate)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, LocalFunctionRegistry.BUILT_IN))
        .go();
  }

  @Test
  public void testSuccessfulRegistrationAfterSeveralRetryAttempts() throws Exception {
    final RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    final Path registryPath = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());
    final Path stagingPath = hadoopToJavaPath(remoteFunctionRegistry.getStagingArea());
    final Path tmpPath = hadoopToJavaPath(remoteFunctionRegistry.getTmpArea());

    copyDefaultJarsToStagingArea();

    doThrow(new VersionMismatchException("Version mismatch detected", 1))
            .doThrow(new VersionMismatchException("Version mismatch detected", 1))
            .doCallRealMethod()
            .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    String summary = "The following UDFs in jar %s have been registered:\n" +
            "[custom_lower(VARCHAR-REQUIRED)]";

    testBuilder()
            .sqlQuery("create function using jar '%s'", default_binary_name)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(true, String.format(summary, default_binary_name))
            .go();

    verify(remoteFunctionRegistry, times(3))
            .updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    assertTrue("Staging area should be empty", ArrayUtils.isEmpty(stagingPath.toFile().listFiles()));
    assertTrue("Temporary area should be empty", ArrayUtils.isEmpty(tmpPath.toFile().listFiles()));

    assertTrue("Binary should be present in registry area",
      registryPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should be present in registry area",
      registryPath.resolve(default_source_name).toFile().exists());

    Registry registry = remoteFunctionRegistry.getRegistry(new DataChangeVersion());
    assertEquals("Registry should contain one jar", registry.getJarList().size(), 1);
    assertEquals(registry.getJar(0).getName(), default_binary_name);
  }

  @Test
  public void testSuccessfulUnregistrationAfterSeveralRetryAttempts() throws Exception {
    RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);

    reset(remoteFunctionRegistry);
    doThrow(new VersionMismatchException("Version mismatch detected", 1))
            .doThrow(new VersionMismatchException("Version mismatch detected", 1))
            .doCallRealMethod()
            .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    String summary = "The following UDFs in jar %s have been unregistered:\n" +
            "[custom_lower(VARCHAR-REQUIRED)]";

    testBuilder()
            .sqlQuery("drop function using jar '%s'", default_binary_name)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(true, String.format(summary, default_binary_name))
            .go();

    verify(remoteFunctionRegistry, times(3))
            .updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    FileSystem fs = remoteFunctionRegistry.getFs();

    assertFalse("Registry area should be empty", fs.listFiles(remoteFunctionRegistry.getRegistryArea(), false).hasNext());
    assertEquals("Registry should be empty",
        remoteFunctionRegistry.getRegistry(new DataChangeVersion()).getJarList().size(), 0);
  }

  @Test
  public void testExceedRetryAttemptsDuringRegistration() throws Exception {
    final RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    final Path registryPath = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());
    final Path stagingPath = hadoopToJavaPath(remoteFunctionRegistry.getStagingArea());
    final Path tmpPath = hadoopToJavaPath(remoteFunctionRegistry.getTmpArea());

    copyDefaultJarsToStagingArea();

    doThrow(new VersionMismatchException("Version mismatch detected", 1))
        .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    String summary = "Failed to update remote function registry. Exceeded retry attempts limit.";

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();

    verify(remoteFunctionRegistry, times(remoteFunctionRegistry.getRetryAttempts() + 1))
        .updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    assertTrue("Binary should be present in staging area",
            stagingPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should be present in staging area",
            stagingPath.resolve(default_source_name).toFile().exists());

    assertTrue("Registry area should be empty", ArrayUtils.isEmpty(registryPath.toFile().listFiles()));
    assertTrue("Temporary area should be empty", ArrayUtils.isEmpty(tmpPath.toFile().listFiles()));

    assertEquals("Registry should be empty",
        remoteFunctionRegistry.getRegistry(new DataChangeVersion()).getJarList().size(), 0);
  }

  @Test
  public void testExceedRetryAttemptsDuringUnregistration() throws Exception {
    final RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    final Path registryPath = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());

    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);

    reset(remoteFunctionRegistry);
    doThrow(new VersionMismatchException("Version mismatch detected", 1))
        .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    String summary = "Failed to update remote function registry. Exceeded retry attempts limit.";

    testBuilder()
        .sqlQuery("drop function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, summary)
        .go();

    verify(remoteFunctionRegistry, times(remoteFunctionRegistry.getRetryAttempts() + 1))
        .updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    assertTrue("Binary should be present in registry area",
      registryPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should be present in registry area",
      registryPath.resolve(default_source_name).toFile().exists());

    Registry registry = remoteFunctionRegistry.getRegistry(new DataChangeVersion());
    assertEquals("Registry should contain one jar", registry.getJarList().size(), 1);
    assertEquals(registry.getJar(0).getName(), default_binary_name);
  }

  @Test
  public void testLazyInit() throws Exception {
    try {
      test("select custom_lower('A') from (values(1))");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature custom_lower(<CHARACTER>)"));
    }

    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);
    testBuilder()
        .sqlQuery("select custom_lower('A') as res from (values(1))")
        .unOrdered()
        .baselineColumns("res")
        .baselineValues("a")
        .go();

    Path localUdfDirPath = hadoopToJavaPath((org.apache.hadoop.fs.Path)FieldUtils.readField(
      getDrillbitContext().getFunctionImplementationRegistry(), "localUdfDir", true));

    assertTrue("Binary should exist in local udf directory",
      localUdfDirPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should exist in local udf directory",
      localUdfDirPath.resolve(default_source_name).toFile().exists());
  }

  @Test
  public void testLazyInitWhenDynamicUdfSupportIsDisabled() throws Exception {
    try {
      test("select custom_lower('A') from (values(1))");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature custom_lower(<CHARACTER>)"));
    }

    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);

    try {
      testBuilder()
          .sqlQuery("select custom_lower('A') as res from (values(1))")
          .optionSettingQueriesForTestQuery("alter system set `exec.udf.enable_dynamic_support` = false")
          .unOrdered()
          .baselineColumns("res")
          .baselineValues("a")
          .go();
    } finally {
      test("alter system reset `exec.udf.enable_dynamic_support`");
    }
  }

  @Test
  public void testOverloadedFunctionPlanningStage() throws Exception {
    String jarName = "DrillUDF-overloading-1.0.jar";
    copyJarsToStagingArea(jarName, JarUtil.getSourceName(jarName));
    test("create function using jar '%s'", jarName);

    testBuilder()
        .sqlQuery("select abs('A', 'A') as res from (values(1))")
        .unOrdered()
        .baselineColumns("res")
        .baselineValues("ABS was overloaded. Input: A, A")
        .go();
  }

  @Test
  public void testOverloadedFunctionExecutionStage() throws Exception {
    String jarName = "DrillUDF-overloading-1.0.jar";
    copyJarsToStagingArea(jarName, JarUtil.getSourceName(jarName));
    test("create function using jar '%s'", jarName);

    testBuilder()
        .sqlQuery("select log('A') as res from (values(1))")
        .unOrdered()
        .baselineColumns("res")
        .baselineValues("LOG was overloaded. Input: A")
        .go();
  }

  @Test
  public void testDropFunction() throws Exception {
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);
    test("select custom_lower('A') from (values(1))");

    Path localUdfDirPath = hadoopToJavaPath((org.apache.hadoop.fs.Path)FieldUtils.readField(
        getDrillbitContext().getFunctionImplementationRegistry(), "localUdfDir", true));

    assertTrue("Binary should exist in local udf directory",
      localUdfDirPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should exist in local udf directory",
      localUdfDirPath.resolve(default_source_name).toFile().exists());

    String summary = "The following UDFs in jar %s have been unregistered:\n" +
        "[custom_lower(VARCHAR-REQUIRED)]";

    testBuilder()
        .sqlQuery("drop function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true, String.format(summary, default_binary_name))
        .go();

    try {
      test("select custom_lower('A') from (values(1))");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature custom_lower(<CHARACTER>)"));
    }

    final RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();
    final Path registryPath = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());

    assertEquals("Remote registry should be empty",
        remoteFunctionRegistry.getRegistry(new DataChangeVersion()).getJarList().size(), 0);

    assertFalse("Binary should not be present in registry area",
      registryPath.resolve(default_binary_name).toFile().exists());
    assertFalse("Source should not be present in registry area",
      registryPath.resolve(default_source_name).toFile().exists());

    assertFalse("Binary should not be present in local udf directory",
      localUdfDirPath.resolve(default_binary_name).toFile().exists());
    assertFalse("Source should not be present in local udf directory",
      localUdfDirPath.resolve(default_source_name).toFile().exists());
  }

  @Test
  public void testReRegisterTheSameJarWithDifferentContent() throws Exception {
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);
    testBuilder()
        .sqlQuery("select custom_lower('A') as res from (values(1))")
        .unOrdered()
        .baselineColumns("res")
        .baselineValues("a")
        .go();
    test("drop function using jar '%s'", default_binary_name);

    Thread.sleep(1000);

    Path src = jars.resolve("v2");
    copyJarsToStagingArea(src, default_binary_name, default_source_name);
    test("create function using jar '%s'", default_binary_name);
    testBuilder()
        .sqlQuery("select custom_lower('A') as res from (values(1))")
        .unOrdered()
        .baselineColumns("res")
        .baselineValues("a_v2")
        .go();
  }

  @Test
  public void testDropAbsentJar() throws Exception {
    String summary = "Jar %s is not registered in remote registry";

    testBuilder()
        .sqlQuery("drop function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, String.format(summary, default_binary_name))
        .go();
  }

  @Test
  public void testRegistrationFailDuringRegistryUpdate() throws Exception {
    final RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    final Path registryPath = hadoopToJavaPath(remoteFunctionRegistry.getRegistryArea());
    final Path stagingPath = hadoopToJavaPath(remoteFunctionRegistry.getStagingArea());
    final Path tmpPath = hadoopToJavaPath(remoteFunctionRegistry.getTmpArea());

    final String errorMessage = "Failure during remote registry update.";
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        assertTrue("Binary should be present in registry area",
            registryPath.resolve(default_binary_name).toFile().exists());
        assertTrue("Source should be present in registry area",
            registryPath.resolve(default_source_name).toFile().exists());
        throw new RuntimeException(errorMessage);
      }
    }).when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    copyDefaultJarsToStagingArea();

    testBuilder()
        .sqlQuery("create function using jar '%s'", default_binary_name)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(false, errorMessage)
        .go();

    assertTrue("Registry area should be empty", ArrayUtils.isEmpty(registryPath.toFile().listFiles()));
    assertTrue("Temporary area should be empty", ArrayUtils.isEmpty(tmpPath.toFile().listFiles()));

    assertTrue("Binary should be present in staging area", stagingPath.resolve(default_binary_name).toFile().exists());
    assertTrue("Source should be present in staging area", stagingPath.resolve(default_source_name).toFile().exists());
  }

  @Test
  public void testConcurrentRegistrationOfTheSameJar() throws Exception {
    RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    doAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        String result = (String) invocation.callRealMethod();
        latch2.countDown();
        latch1.await();
        return result;
      }
    })
        .doCallRealMethod()
        .doCallRealMethod()
        .when(remoteFunctionRegistry).addToJars(anyString(), any(RemoteFunctionRegistry.Action.class));


    final String query = String.format("create function using jar '%s'", default_binary_name);

    Thread thread = new Thread(new SimpleQueryRunner(query));
    thread.start();
    latch2.await();

    try {
      String summary = "Jar with %s name is used. Action: REGISTRATION";

      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, String.format(summary, default_binary_name))
          .go();

      testBuilder()
          .sqlQuery("drop function using jar '%s'", default_binary_name)
          .unOrdered()
          .baselineColumns("ok", "summary")
          .baselineValues(false, String.format(summary, default_binary_name))
          .go();

    } finally {
      latch1.countDown();
      thread.join();
    }
  }

  @Test
  public void testConcurrentRemoteRegistryUpdateWithDuplicates() throws Exception {
    RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    final CountDownLatch latch3 = new CountDownLatch(1);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        latch3.countDown();
        latch1.await();
        invocation.callRealMethod();
        latch2.countDown();
        return null;
      }
    }).doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        latch1.countDown();
        latch2.await();
        invocation.callRealMethod();
        return null;
      }
    })
        .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));


    final String jarName1 = default_binary_name;
    final String jarName2 = "DrillUDF_Copy-1.0.jar";
    final String query = "create function using jar '%s'";

    copyDefaultJarsToStagingArea();
    copyJarsToStagingArea(jarName2, JarUtil.getSourceName(jarName2));

    Thread thread1 = new Thread(new TestBuilderRunner(
        testBuilder()
        .sqlQuery(query, jarName1)
        .unOrdered()
        .baselineColumns("ok", "summary")
        .baselineValues(true,
            String.format("The following UDFs in jar %s have been registered:\n" +
            "[custom_lower(VARCHAR-REQUIRED)]", jarName1))
    ));

    Thread thread2 = new Thread(new TestBuilderRunner(
        testBuilder()
            .sqlQuery(query, jarName2)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(false,
                String.format("Found duplicated function in %s: custom_lower(VARCHAR-REQUIRED)", jarName1))
    ));

    thread1.start();
    latch3.await();
    thread2.start();

    thread1.join();
    thread2.join();

    DataChangeVersion version = new DataChangeVersion();
    Registry registry = remoteFunctionRegistry.getRegistry(version);
    assertEquals("Remote registry version should match", 1, version.getVersion());
    List<Jar> jarList = registry.getJarList();
    assertEquals("Only one jar should be registered", 1, jarList.size());
    assertEquals("Jar name should match", jarName1, jarList.get(0).getName());

    verify(remoteFunctionRegistry, times(2)).updateRegistry(any(Registry.class), any(DataChangeVersion.class));
  }

  @Test
  public void testConcurrentRemoteRegistryUpdateForDifferentJars() throws Exception {
    RemoteFunctionRegistry remoteFunctionRegistry = spyRemoteFunctionRegistry();
    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(2);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        latch2.countDown();
        latch1.await();
        invocation.callRealMethod();
        return null;
      }
    })
        .when(remoteFunctionRegistry).updateRegistry(any(Registry.class), any(DataChangeVersion.class));

    final String jarName1 = default_binary_name;
    final String jarName2 = "DrillUDF-2.0.jar";
    final String query = "create function using jar '%s'";

    copyDefaultJarsToStagingArea();
    copyJarsToStagingArea(jarName2, JarUtil.getSourceName(jarName2));


    Thread thread1 = new Thread(new TestBuilderRunner(
        testBuilder()
            .sqlQuery(query, jarName1)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(true,
                String.format("The following UDFs in jar %s have been registered:\n" +
                    "[custom_lower(VARCHAR-REQUIRED)]", jarName1))
    ));


    Thread thread2 = new Thread(new TestBuilderRunner(
        testBuilder()
            .sqlQuery(query, jarName2)
            .unOrdered()
            .baselineColumns("ok", "summary")
            .baselineValues(true, String.format("The following UDFs in jar %s have been registered:\n" +
                "[custom_upper(VARCHAR-REQUIRED)]", jarName2))
    ));

    thread1.start();
    thread2.start();

    latch2.await();
    latch1.countDown();

    thread1.join();
    thread2.join();

    DataChangeVersion version = new DataChangeVersion();
    Registry registry = remoteFunctionRegistry.getRegistry(version);
    assertEquals("Remote registry version should match", 2, version.getVersion());

    List<Jar> actualJars = registry.getJarList();
    List<String> expectedJars = Lists.newArrayList(jarName1, jarName2);

    assertEquals("Only one jar should be registered", 2, actualJars.size());
    for (Jar jar : actualJars) {
      assertTrue("Jar should be present in remote function registry", expectedJars.contains(jar.getName()));
    }

    verify(remoteFunctionRegistry, times(3)).updateRegistry(any(Registry.class), any(DataChangeVersion.class));
  }

  @Test
  public void testLazyInitConcurrent() throws Exception {
    FunctionImplementationRegistry functionImplementationRegistry = spyFunctionImplementationRegistry();
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    final String query = "select custom_lower('A') from (values(1))";

    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        latch1.await();
        boolean result = (boolean) invocation.callRealMethod();
        assertTrue("syncWithRemoteRegistry() should return true", result);
        latch2.countDown();
        return true;
      }
    })
        .doAnswer(new Answer() {
          @Override
          public Boolean answer(InvocationOnMock invocation) throws Throwable {
            latch1.countDown();
            latch2.await();
            boolean result = (boolean) invocation.callRealMethod();
            assertTrue("syncWithRemoteRegistry() should return true", result);
            return true;
          }
        })
        .when(functionImplementationRegistry).syncWithRemoteRegistry(anyLong());

    SimpleQueryRunner simpleQueryRunner = new SimpleQueryRunner(query);
    Thread thread1 = new Thread(simpleQueryRunner);
    Thread thread2 = new Thread(simpleQueryRunner);

    thread1.start();
    thread2.start();

    thread1.join();
    thread2.join();

    verify(functionImplementationRegistry, times(2)).syncWithRemoteRegistry(anyLong());
    LocalFunctionRegistry localFunctionRegistry = (LocalFunctionRegistry)FieldUtils.readField(
        functionImplementationRegistry, "localFunctionRegistry", true);
    assertEquals("Sync function registry version should match", 1L, localFunctionRegistry.getVersion());
  }

  @Test
  public void testLazyInitNoReload() throws Exception {
    FunctionImplementationRegistry functionImplementationRegistry = spyFunctionImplementationRegistry();
    copyDefaultJarsToStagingArea();
    test("create function using jar '%s'", default_binary_name);

    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        boolean result = (boolean) invocation.callRealMethod();
        assertTrue("syncWithRemoteRegistry() should return true", result);
        return true;
      }
    })
        .doAnswer(new Answer() {
          @Override
          public Boolean answer(InvocationOnMock invocation) throws Throwable {
            boolean result = (boolean) invocation.callRealMethod();
            assertFalse("syncWithRemoteRegistry() should return false", result);
            return false;
          }
        })
        .when(functionImplementationRegistry).syncWithRemoteRegistry(anyLong());

    test("select custom_lower('A') from (values(1))");

    try {
      test("select unknown_lower('A') from (values(1))");
    } catch (UserRemoteException e){
      assertThat(e.getMessage(), containsString("No match found for function signature unknown_lower(<CHARACTER>)"));
    }

    verify(functionImplementationRegistry, times(2)).syncWithRemoteRegistry(anyLong());
    LocalFunctionRegistry localFunctionRegistry = (LocalFunctionRegistry)FieldUtils.readField(
        functionImplementationRegistry, "localFunctionRegistry", true);
    assertEquals("Sync function registry version should match", 1L, localFunctionRegistry.getVersion());
  }

  private void copyDefaultJarsToStagingArea() throws IOException {
    copyJarsToStagingArea(jars, default_binary_name, default_source_name);
  }

  private void copyJarsToStagingArea(String binaryName, String sourceName) throws IOException  {
    copyJarsToStagingArea(jars, binaryName, sourceName);
  }

  private void copyJarsToStagingArea(Path src, String binaryName, String sourceName) throws IOException {
    RemoteFunctionRegistry remoteFunctionRegistry = getDrillbitContext().getRemoteFunctionRegistry();

    final Path path = hadoopToJavaPath(remoteFunctionRegistry.getStagingArea());

    copyJar(src, path, binaryName);
    copyJar(src, path, sourceName);
  }

  private void copyJar(Path src, Path dest, String name) throws IOException {
    final File destFile = dest.resolve(name).toFile();
    FileUtils.deleteQuietly(destFile);
    FileUtils.copyFile(src.resolve(name).toFile(), destFile);
  }

  private RemoteFunctionRegistry spyRemoteFunctionRegistry() throws IllegalAccessException {
    FunctionImplementationRegistry functionImplementationRegistry =
        getDrillbitContext().getFunctionImplementationRegistry();
    RemoteFunctionRegistry remoteFunctionRegistry = functionImplementationRegistry.getRemoteFunctionRegistry();
    RemoteFunctionRegistry spy = spy(remoteFunctionRegistry);
    FieldUtils.writeField(functionImplementationRegistry, "remoteFunctionRegistry", spy, true);
    return spy;
  }

  private FunctionImplementationRegistry spyFunctionImplementationRegistry() throws IllegalAccessException {
    DrillbitContext drillbitContext = getDrillbitContext();
    FunctionImplementationRegistry spy = spy(drillbitContext.getFunctionImplementationRegistry());
    FieldUtils.writeField(drillbitContext, "functionRegistry", spy, true);
    return spy;
  }

  private class SimpleQueryRunner implements Runnable {

    private final String query;

    SimpleQueryRunner(String query) {
      this.query = query;
    }

    @Override
    public void run() {
      try {
        test(query);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class TestBuilderRunner implements Runnable {

    private final TestBuilder testBuilder;

    TestBuilderRunner(TestBuilder testBuilder) {
      this.testBuilder = testBuilder;
    }

    @Override
    public void run() {
      try {
        testBuilder.go();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
