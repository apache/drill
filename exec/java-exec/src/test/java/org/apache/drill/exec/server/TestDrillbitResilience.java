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

import org.apache.commons.math3.util.Pair;
import org.apache.drill.QueryTestUtil;
import org.apache.drill.SingleRowListener;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ZookeeperHelper;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.physical.impl.ScreenCreator;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.ExceptionWrapper;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.testing.ControlsInjectionUtil;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test how resilient drillbits are to throwing exceptions during various phases of query
 * execution by injecting exceptions at various points. The test cases are mentioned in DRILL-2383.
 */
public class TestDrillbitResilience {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TestDrillbitResilience.class);

  private static ZookeeperHelper zkHelper;
  private static RemoteServiceSet remoteServiceSet;
  private static final Map<String, Drillbit> drillbits = new HashMap<>();
  private static DrillClient drillClient;

  /**
   * Note: Counting sys.memory executes a fragment on every drillbit. This is a better check in comparison to
   * counting sys.drillbits.
   */
  private static final String TEST_QUERY = "select * from sys.memory";
  private static final long PAUSE_TIME_MILLIS = 1000L;

  private static void startDrillbit(final String name, final RemoteServiceSet remoteServiceSet) {
    if (drillbits.containsKey(name)) {
      throw new IllegalStateException("Drillbit named \"" + name + "\" already exists");
    }

    try {
      @SuppressWarnings("resource")
      final Drillbit drillbit = Drillbit.start(zkHelper.getConfig(), remoteServiceSet);
      drillbits.put(name, drillbit);
    } catch (final DrillbitStartupException e) {
      throw new RuntimeException("Failed to start Drillbit \"" + name + "\"", e);
    }
  }

  /**
   * Shutdown the specified drillbit.
   *
   * @param name name of the drillbit
   */
  private static void stopDrillbit(final String name) {
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(name);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + name + "\" found");
    }

    try {
      drillbit.close();
    } catch (final Exception e) {
      final String message = "Error shutting down Drillbit \"" + name + "\"";
      System.err.println(message + '.');
      logger.warn(message, e);
    }
  }

  /**
   * Shutdown all the drillbits.
   */
  private static void stopAllDrillbits() {
    for (String name : drillbits.keySet()) {
      stopDrillbit(name);
    }
    drillbits.clear();
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

    // create a client
    final DrillConfig drillConfig = zkHelper.getConfig();
    drillClient = QueryTestUtil.createClient(drillConfig, remoteServiceSet, 1, null);
    clearAllInjections();
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
   * Clear all exceptions.
   */
  private static void clearAllInjections() {
    assertTrue(drillClient != null);
    ControlsInjectionUtil.clearControls(drillClient);
  }

  /**
   * Check that all the drillbits are ok.
   * <p/>
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
            } catch (final SchemaChangeException e) {
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
      QueryTestUtil.testWithListener(drillClient, QueryType.SQL, "select count(*) from sys.memory", listener);
      listener.waitForCompletion();
      assertTrue(listener.getQueryState() == QueryState.COMPLETED);
    } catch (final Exception e) {
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
   * Set the given exceptions.
   */
  private static void setExceptions(final String controlsString) {
    ControlsInjectionUtil.setControls(drillClient, controlsString);
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass      the injection site class
   * @param desc           the injection site description
   * @param exceptionClass the class of the exception to throw
   * @return the created controls JSON as string
   */
  private static String createSingleException(final Class<?> siteClass, final String desc,
                                              final Class<? extends Throwable> exceptionClass) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    return "{\"injections\":[{"
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":0,"
      + "\"nFire\":1,"
      + "\"exceptionClass\":\"" + exceptionClassName + "\""
      + "}]}";
  }

  /**
   * Create a single exception injection.
   *
   * @param siteClass      the injection site class
   * @param desc           the injection site description
   * @param exceptionClass the class of the exception to throw
   * @param bitName        the drillbit name which should be injected into
   * @return the created controls JSON as string
   */
  private static String createSingleExceptionOnBit(final Class<?> siteClass, final String desc,
                                                   final Class<? extends Throwable> exceptionClass,
                                                   final String bitName) {
    final String siteClassName = siteClass.getName();
    final String exceptionClassName = exceptionClass.getName();
    @SuppressWarnings("resource")
    final Drillbit drillbit = drillbits.get(bitName);
    if (drillbit == null) {
      throw new IllegalStateException("No Drillbit named \"" + bitName + "\" found");
    }

    final DrillbitEndpoint endpoint = drillbit.getContext().getEndpoint();
    return "{\"injections\":[{"
      + "\"address\":\"" + endpoint.getAddress() + "\","
      + "\"port\":\"" + endpoint.getUserPort() + "\","
      + "\"type\":\"exception\","
      + "\"siteClass\":\"" + siteClassName + "\","
      + "\"desc\":\"" + desc + "\","
      + "\"nSkip\":0,"
      + "\"nFire\":1,"
      + "\"exceptionClass\":\"" + exceptionClassName + "\""
      + "}]}";
  }

  /**
   * Check that the injected exception is what we were expecting.
   *
   * @param throwable      the throwable that was caught (by the test)
   * @param exceptionClass the expected exception class
   * @param desc           the expected exception site description
   */
  private static void assertExceptionInjected(final Throwable throwable,
                                              final Class<? extends Throwable> exceptionClass, final String desc) {
    assertTrue(throwable instanceof UserException);
    final ExceptionWrapper cause = ((UserException) throwable).getOrCreatePBError(false).getException();
    assertEquals(exceptionClass.getName(), cause.getExceptionClass());
    assertEquals(desc, cause.getMessage());
  }

  @Test
  public void settingNoopInjectionsAndQuery() {
    final String controls = createSingleExceptionOnBit(getClass(), "noop", RuntimeException.class, DRILLBIT_BETA);
    setExceptions(controls);
    try {
      QueryTestUtil.test(drillClient, TEST_QUERY);
    } catch (final Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * Test throwing exceptions from sites within the Foreman class, as specified by the site
   * description
   *
   * @param desc site description
   */
  private static void testForeman(final String desc) {
    final String controls = createSingleException(Foreman.class, desc, ForemanException.class);
    setExceptions(controls);
    try {
      QueryTestUtil.test(drillClient, TEST_QUERY);
      fail();
    } catch (final Exception e) {
      assertExceptionInjected(e, ForemanException.class, desc);
    }
  }

  @SuppressWarnings("static-method")
  @Test
  public void foreman_runTryBeginning() {
    testForeman("run-try-beginning");
  }

  /*
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
  public void foreman_runTryEnd() {
    testForeman("run-try-end");
  }

  private static class WaitUntilCompleteListener implements UserResultsListener {
    protected final CountDownLatch latch;
    protected QueryId queryId = null;
    protected Exception ex = null;
    protected QueryState state = null;

    public WaitUntilCompleteListener(final int count) {
      latch = new CountDownLatch(count);
    }

    @Override
    public void queryIdArrived(final QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void submissionFailed(final UserException ex) {
      this.ex = ex;
      state = QueryState.FAILED;
      latch.countDown();
    }

    @Override
    public void queryCompleted(final QueryState state) {
      this.state = state;
      latch.countDown();
    }

    @Override
    public void dataArrived(final QueryDataBatch result, final ConnectionThrottle throttle) {
      result.release();
    }

    public final Pair<QueryState, Exception> waitForCompletion() {
      try {
        latch.await();
      } catch (final InterruptedException e) {
        return new Pair<QueryState, Exception>(state, e);
      }
      return new Pair<>(state, ex);
    }
  }

  private static class CancellingThread extends Thread {

    private final QueryId queryId;

    public CancellingThread(final QueryId queryId) {
      this.queryId = queryId;
    }

    @Override
    public void run() {
      final DrillRpcFuture<Ack> cancelAck = drillClient.cancelQuery(queryId);
      try {
        cancelAck.checkedGet();
      } catch (final RpcException e) {
        fail(e.getMessage()); // currently this failure does not fail the test
      }
    }
  }

  /**
   * Given a set of controls, this method ensures that the TEST_QUERY completes with a CANCELED state.
   */
  private static void assertCancelled(final String controls, final WaitUntilCompleteListener listener) {
    ControlsInjectionUtil.setControls(drillClient, controls);

    QueryTestUtil.testWithListener(drillClient, QueryType.SQL, TEST_QUERY, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertTrue(result.getFirst() == QueryState.CANCELED);
    assertTrue(result.getSecond() == null);
  }

  @Test // Cancellation TC 1
  public void cancelBeforeAnyResultsArrive() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1) {

      @Override
      public void queryIdArrived(final QueryId queryId) {
        (new CancellingThread(queryId)).start();
      }
    };

    final String controls = "{\"injections\":[{"
      + "\"type\":\"pause\"," +
      "\"siteClass\":\"" + Foreman.class.getName() + "\","
      + "\"desc\":\"pause-run-plan\","
      + "\"millis\":" + PAUSE_TIME_MILLIS + ","
      + "\"nSkip\":0,"
      + "\"nFire\":1"
      + "}]}";

    assertCancelled(controls, listener);
  }

  @Test // Cancellation TC 2
  public void cancelInMiddleOfFetchingResults() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1) {
      private boolean cancelRequested = false;

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        if (! cancelRequested) {
          assertTrue(queryId != null);
          (new CancellingThread(queryId)).start();
          cancelRequested = true;
        }
        result.release();
      }
    };

    final String controls = "{\"injections\":[{"
      + "\"type\":\"pause\"," +
      "\"siteClass\":\"" + ScreenCreator.class.getName() + "\","
      + "\"desc\":\"sending-data\","
      + "\"millis\":" + PAUSE_TIME_MILLIS + ","
      + "\"nSkip\":0,"
      + "\"nFire\":1"
      + "}]}";

    assertCancelled(controls, listener);
  }


  @Test // Cancellation TC 3
  public void cancelAfterAllResultsProduced() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1) {
      private int count = 0;

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        if (++count == drillbits.size()) {
          assertTrue(queryId != null);
          (new CancellingThread(queryId)).start();
        }
        result.release();
      }
    };

    final String controls = "{\"injections\":[{"
      + "\"type\":\"pause\"," +
      "\"siteClass\":\"" + ScreenCreator.class.getName() + "\","
      + "\"desc\":\"send-complete\","
      + "\"millis\":" + PAUSE_TIME_MILLIS + ","
      + "\"nSkip\":0,"
      + "\"nFire\":1"
      + "}]}";

    assertCancelled(controls, listener);
  }

  @Test // Cancellation TC 4
  public void cancelAfterEverythingIsCompleted() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1) {
      private int count = 0;

      @Override
      public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
        if (++count == drillbits.size()) {
          assertTrue(queryId != null);
          (new CancellingThread(queryId)).start();
        }
        result.release();
      }
    };

    final String controls = "{\"injections\":[{"
      + "\"type\":\"pause\"," +
      "\"siteClass\":\"" + Foreman.class.getName() + "\","
      + "\"desc\":\"foreman-cleanup\","
      + "\"millis\":" + PAUSE_TIME_MILLIS + ","
      + "\"nSkip\":0,"
      + "\"nFire\":1"
      + "}]}";

    assertCancelled(controls, listener);
  }

  @Test // Completion TC 1
  public void successfullyCompletes() {
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1);
    QueryTestUtil.testWithListener(
      drillClient, QueryType.SQL, TEST_QUERY, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertTrue(result.getFirst() == QueryState.COMPLETED);
    assertTrue(result.getSecond() == null);
  }

  /**
   * Given a set of controls, this method ensures TEST_QUERY fails with the given class and desc.
   */
  private static void assertFailsWithException(final String controls, final Class<? extends Throwable> exceptionClass,
                                               final String exceptionDesc) {
    setExceptions(controls);
    final WaitUntilCompleteListener listener = new WaitUntilCompleteListener(1);
    QueryTestUtil.testWithListener(drillClient, QueryType.SQL,  TEST_QUERY, listener);
    final Pair<QueryState, Exception> result = listener.waitForCompletion();
    assertTrue(result.getFirst() == QueryState.FAILED);
    final Exception e = result.getSecond();
    assertExceptionInjected(e, exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 2
  public void failsWhenParsing() {
    final String exceptionDesc = "sql-parsing";
    final Class<? extends Throwable> exceptionClass = ForemanSetupException.class;
    final String controls = createSingleException(DrillSqlWorker.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 3
  public void failsWhenSendingFragments() {
    final String exceptionDesc = "send-fragments";
    final Class<? extends Throwable> exceptionClass = ForemanException.class;
    final String controls = createSingleException(Foreman.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }

  @Test // Completion TC 4
  public void failsDuringExecution() {
    final String exceptionDesc = "fragment-execution";
    final Class<? extends Throwable> exceptionClass = IOException.class;
    final String controls = createSingleException(FragmentExecutor.class, exceptionDesc, exceptionClass);
    assertFailsWithException(controls, exceptionClass, exceptionDesc);
  }
}
