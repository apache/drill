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
package org.apache.drill.exec.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.QueryTestUtil;
import org.apache.drill.SingleRowListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillUserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.testing.ExceptionInjectionUtil;
import org.apache.drill.exec.testing.SimulatedExceptions.InjectionOption;
import org.apache.drill.exec.testing.SimulatedExceptions.InjectionOptions;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test how resilient drillbits are to throwing exceptions during various phases of query
 * execution by injecting exceptions at various points.
 */
public class TestDrillbitResilience extends ExecTest {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillbitResilience.class);

  private static ZookeeperHelper zkHelper;
  private static RemoteServiceSet remoteServiceSet;
  private static final Map<String, Drillbit> drillbits = new HashMap<>();
  private static DrillClient drillClient;

  private static void startDrillbit(final String name, final RemoteServiceSet remoteServiceSet) {
    if (drillbits.containsKey(name)) {
      throw new IllegalStateException("Drillbit named \"" + name + "\" already exists");
    }

    try {
      @SuppressWarnings("resource")
      final Drillbit drillbit = Drillbit.start(zkHelper.getConfig(), remoteServiceSet);
      drillbits.put(name, drillbit);
    } catch(DrillbitStartupException e) {
      throw new RuntimeException("Failed to start Drillbit \"" + name + "\"", e);
    }
  }

  /**
   * Shutdown the specified drillbit.
   *
   * @param name
   */
  private static void stopDrillbit(final String name) {
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(name);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + name + "\" found");
    }

    try {
      drillbit.close();
    } catch(Exception e) {
      final String message = "Error shutting down Drillbit \"" + name + "\"";
      System.err.println(message + '.');
      logger.warn(message, e);
    }
  }

  /**
   * Shutdown all the drillbits.
   */
  private static void stopAllDrillbits() {
    for(String name : drillbits.keySet()) {
      stopDrillbit(name);
    }
  }

  /*
   * Canned drillbit names.
   */
  private final static String DRILLBIT_ALPHA = "alpha";
  private final static String DRILLBIT_BETA = "beta";
  private final static String DRILLBIT_GAMMA = "gamma";

  @BeforeClass
  public static void startSomeDrillbits() throws Exception {
    // turn off the HTTP server to avoid port conflicts between the drill bits
    System.setProperty(ExecConstants.HTTP_ENABLE, "false");

    zkHelper = new ZookeeperHelper();
    zkHelper.startZookeeper(1);

    // use a non-null service set so that the drillbits can use port hunting
    remoteServiceSet = RemoteServiceSet.getLocalServiceSet();

    // create name-addressable drillbits
    startDrillbit(DRILLBIT_ALPHA, remoteServiceSet);
    startDrillbit(DRILLBIT_BETA, remoteServiceSet);
    startDrillbit(DRILLBIT_GAMMA, remoteServiceSet);
    clearAllInjections();

    // create a client
    final DrillConfig drillConfig = zkHelper.getConfig();
    drillClient = QueryTestUtil.createClient(drillConfig, remoteServiceSet, 1, null);
  }

  @AfterClass
  public static void shutdownAllDrillbits() {
    if (drillClient != null) {
      drillClient.close();
      drillClient = null;
    }

    stopAllDrillbits();

    if (remoteServiceSet != null) {
      AutoCloseables.close(remoteServiceSet, logger);
      remoteServiceSet = null;
    }

    zkHelper.stopZookeeper();
  }

  /**
   * Clear all injections from all drillbits.
   */
  private static void clearAllInjections() {
    for(Drillbit drillbit : drillbits.values()) {
      ExceptionInjectionUtil.clearInjections(drillbit);
    }
  }

  /**
   * Check that all the drillbits are ok.
   *
   * <p>The current implementation does this by counting the number of drillbits using a
   * query.
   */
  private static void assertDrillbitsOk() {
      final SingleRowListener listener = new SingleRowListener() {
          private final BufferAllocator bufferAllocator = new TopLevelAllocator(zkHelper.getConfig());
          private final RecordBatchLoader loader = new RecordBatchLoader(bufferAllocator);

          @Override
          public void rowArrived(final QueryDataBatch queryResultBatch) {
            // load the single record
            final QueryData queryData = queryResultBatch.getHeader();
            try {
              loader.load(queryData.getDef(), queryResultBatch.getData());
            } catch(SchemaChangeException e) {
              fail(e.toString());
            }
            assertEquals(1, loader.getRecordCount());

            // there should only be one column
            final BatchSchema batchSchema = loader.getSchema();
            assertEquals(1, batchSchema.getFieldCount());

            // the column should be an integer
            final MaterializedField countField = batchSchema.getColumn(0);
            final MinorType fieldType = countField.getType().getMinorType();
            assertEquals(MinorType.BIGINT, fieldType);

            // get the column value
            final VectorWrapper<?> vw = loader.iterator().next();
            final Object obj = vw.getValueVector().getAccessor().getObject(0);
            assertTrue(obj instanceof Long);
            final Long countValue = (Long) obj;

            // assume this means all the drillbits are still ok
            assertEquals(drillbits.size(), countValue.intValue());

            loader.clear();
          }

          @Override
          public void cleanup() {
            bufferAllocator.close();
          }
        };

    try {
      QueryTestUtil.testWithListener(
          drillClient, QueryType.SQL, "select count(*) from sys.drillbits", listener);
      listener.waitForCompletion();
    } catch(Exception e) {
      throw new RuntimeException("Couldn't query active drillbits", e);
    }

    final List<DrillPBError> errorList = listener.getErrorList();
    assertTrue(errorList.isEmpty());
  }

  @SuppressWarnings("static-method")
  @After
  public void checkDrillbits() {
    clearAllInjections(); // so that the drillbit check itself doesn't trigger anything
    assertDrillbitsOk(); // TODO we need a way to do this without using a query
  }

  /**
   * Set the given injections on a single named drillbit.
   *
   * @param bitName
   * @param injectionOptions the injections
   */
  private static void setInjections(final String bitName, final InjectionOptions injectionOptions) {
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(bitName);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + bitName + "\" found");
    }

    ExceptionInjectionUtil.setInjections(drillbit, injectionOptions);
  }

  /**
   * Set the given injections on all drillbits.
   *
   * @param injectionOptions the injections
   */
  private static void setInjectionsAll(final InjectionOptions injectionOptions) {
    for(Drillbit drillbit : drillbits.values()) {
      ExceptionInjectionUtil.setInjections(drillbit, injectionOptions);
    }
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClassName the name of the injection site class
   * @param desc the injection site description
   * @param exceptionClassName the name of the exception to throw
   * @return the created injection options POJO
   */
  private static InjectionOptions createSingleInjection(
      final String siteClassName, final String desc, final String exceptionClassName) {
    final InjectionOption injectionOption = new InjectionOption();
    injectionOption.nFire = 1;
    injectionOption.siteClass = siteClassName;
    injectionOption.desc = desc;
    injectionOption.exceptionClass = exceptionClassName;

    final InjectionOptions injectionOptions = new InjectionOptions();
    injectionOptions.injections = new InjectionOption[1];
    injectionOptions.injections[0] = injectionOption;

    return injectionOptions;
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass the injection site class
   * @param desc the injection site description
   * @param exceptionClass the class of the exception to throw
   * @return the created injection options POJO
   */
  private static InjectionOptions createSingleInjection(
      final Class<?> siteClass, final String desc, final Class<? extends Throwable> exceptionClass) {
    return createSingleInjection(siteClass.getName(), desc, exceptionClass.getName());
  }

  /**
   * Check that the injected exception is what we were expecting.
   *
   * @param caught the exception that was caught (by the test)
   * @param exceptionClass the expected exception class
   * @param desc the expected exception site description
   */
  private static void assertInjected(
      final DrillUserException caught, final Class<? extends Throwable> exceptionClass, final String desc) {
    ExceptionWrapper cause = caught.getOrCreatePBError(false).getException();
    assertEquals(exceptionClass.getName(), cause.getExceptionClass());
    assertEquals(desc, cause.getMessage());
  }

  @Test
  public void testSettingNoopInjectionsAndQuery() throws Exception {
    final InjectionOptions injectionOptions =
        createSingleInjection(getClass(), "noop", RuntimeException.class);
    setInjections(DRILLBIT_BETA, injectionOptions);
    QueryTestUtil.test(drillClient, "select * from sys.drillbits");
  }

  /**
   * Test throwing exceptions from sites within the Foreman class, as specified by the site
   * description
   *
   * @param desc site description
   * @throws Exception
   */
  private static void testForeman(final String desc) throws Exception {
    final InjectionOptions injectionOptions = createSingleInjection(Foreman.class, desc, ForemanException.class);
    setInjectionsAll(injectionOptions);
    try {
      QueryTestUtil.test(drillClient, "select * from sys.drillbits");
      fail();
    } catch(DrillUserException dre) {
      assertInjected(dre, ForemanException.class, desc);
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void testForeman_runTryBeginning() throws Exception {
    testForeman("run-try-beginning");
  }

  @SuppressWarnings("static-method")
  @Test
  public void testForeman_setInjectionViaAlterSystem() throws Exception {
    final String exceptionDesc = "run-try-beginning";
    final InjectionOptions injectionOptions =
        createSingleInjection(Foreman.class, exceptionDesc, ForemanException.class);
    final ObjectMapper objectMapper = new ObjectMapper();
    final String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(injectionOptions);
    final String alterSession = String.format(
        "alter system set `%s`='%s'",
        ExecConstants.DRILLBIT_EXCEPTION_INJECTIONS, jsonString);
    QueryTestUtil.test(drillClient, alterSession);
    try {
      QueryTestUtil.test(drillClient, "select * from sys.drillbits");
      fail();
    } catch(DrillUserException dre) {
      assertInjected(dre, ForemanException.class, exceptionDesc);
    }
  }

  /*
   * This test doesn't work because worker threads have returned the result to the client before
   * Foreman.run() has even finished executing. This might not happen if the results are larger.
   * This brings up the question of how we detect failed queries, because here a failure is happening
   * after the query starts running, yet apparently the query still succeeds.
   *
   * TODO I'm beginning to think that Foreman needs to gate output to its client in a similar way
   * that it gates input via stateListener. That could be tricky, since some results could be
   * queued up before Foreman has gotten through it's run(), and they would all have to be sent
   * before the gate is opened. There's also the question of what to do in case we detect failure
   * there after some data has been sent. Right now, this test doesn't work because that's
   * exactly what happens, and the client believes that the query succeeded, even though an exception
   * was thrown after setup completed, but data was asynchronously sent to the client before that.
   * This test also revealed that the QueryState never seems to make it to the client, so we can't
   * detect the failure that way (see SingleRowListener's getQueryState(), which I originally tried
   * to use here to detect query completion).
   */
  @SuppressWarnings("static-method")
  @Test
  @Ignore
  public void testForeman_runTryEnd() throws Exception {
    testForeman("run-try-end");
  }
}
