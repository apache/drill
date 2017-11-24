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
package org.apache.drill.exec.work.foreman;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.drill.common.CatastrophicFailure;
import org.apache.drill.common.EventProcessor;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.ServerPreparedStatementState;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.PreparedStatementHandle;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.foreman.rm.QueryResourceManager;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Foreman manages all the fragments (local and remote) for a single query where this
 * is the driving/root node.
 *
 * The flow is as follows:
 * <ul>
 * <li>While Foreman is initialized query is in preparing state.</li>
 * <li>Foreman is submitted as a runnable.</li>
 * <li>Runnable does query planning.</li>
 * <li>Runnable submits query to be enqueued.</li>
 * <li>The Runnable's run() completes, but the Foreman stays around to listen to state changes.</li>
 * <li>Once query is enqueued, starting fragments are sent out.</li>
 * <li>Status listener are activated</li>
 * <li>Foreman listens for state change messages.</li>
 * <li>State change messages can drive the state to FAILED or CANCELED, in which case
 *   messages are sent to running fragments to terminate.</li>
 * <li>When all fragments is completed, state change messages drive the state to COMPLETED.</li>
 * </ul>
 */

public class Foreman implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);
  private static final org.slf4j.Logger queryLogger = org.slf4j.LoggerFactory.getLogger("query.logger");
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(Foreman.class);

  public enum ProfileOption { SYNC, ASYNC, NONE }

  private static final ObjectMapper MAPPER = new ObjectMapper();


  private static final Counter planningQueries = DrillMetrics.getRegistry().counter("drill.queries.planning");
  private static final Counter enqueuedQueries = DrillMetrics.getRegistry().counter("drill.queries.enqueued");
  private static final Counter runningQueries = DrillMetrics.getRegistry().counter("drill.queries.running");
  private static final Counter completedQueries = DrillMetrics.getRegistry().counter("drill.queries.completed");
  private static final Counter succeededQueries = DrillMetrics.getRegistry().counter("drill.queries.succeeded");
  private static final Counter failedQueries = DrillMetrics.getRegistry().counter("drill.queries.failed");
  private static final Counter canceledQueries = DrillMetrics.getRegistry().counter("drill.queries.canceled");

  private final QueryId queryId;
  private final String queryIdString;
  private final RunQuery queryRequest;
  private final QueryContext queryContext;
  private final QueryManager queryManager; // handles lower-level details of query execution
  private final DrillbitContext drillbitContext;
  private final UserClientConnection initiatingClient; // used to send responses
  private volatile QueryState state;
  private boolean resume = false;
  private final ProfileOption profileOption;

  private final QueryResourceManager queryRM;

  private final ResponseSendListener responseListener = new ResponseSendListener();
  private final StateSwitch stateSwitch = new StateSwitch();
  private final ForemanResult foremanResult = new ForemanResult();
  private final ConnectionClosedListener closeListener = new ConnectionClosedListener();
  private final ChannelFuture closeFuture;
  private final FragmentsRunner fragmentsRunner;

  private String queryText;

  /**
   * Constructor. Sets up the Foreman, but does not initiate any execution.
   *
   * @param bee work manager (runs fragments)
   * @param drillbitContext drillbit context
   * @param connection connection
   * @param queryId the id for the query
   * @param queryRequest the query to execute
   */
  public Foreman(final WorkerBee bee, final DrillbitContext drillbitContext,
      final UserClientConnection connection, final QueryId queryId, final RunQuery queryRequest) {
    this.queryId = queryId;
    this.queryIdString = QueryIdHelper.getQueryId(queryId);
    this.queryRequest = queryRequest;
    this.drillbitContext = drillbitContext;

    initiatingClient = connection;
    closeFuture = initiatingClient.getChannelClosureFuture();
    closeFuture.addListener(closeListener);

    queryContext = new QueryContext(connection.getSession(), drillbitContext, queryId);
    queryManager = new QueryManager(queryId, queryRequest, drillbitContext.getStoreProvider(),
        drillbitContext.getClusterCoordinator(), this);

    recordNewState(QueryState.PREPARING);

    queryRM = drillbitContext.getResourceManager().newQueryRM(this);
    fragmentsRunner = new FragmentsRunner(bee, initiatingClient, drillbitContext, this);

    profileOption = setProfileOption(queryContext.getOptions());
  }

  private ProfileOption setProfileOption(OptionManager options) {
    if (! options.getOption(ExecConstants.ENABLE_QUERY_PROFILE_VALIDATOR)) {
      return ProfileOption.NONE;
    }
    if (options.getOption(ExecConstants.QUERY_PROFILE_DEBUG_VALIDATOR)) {
      return ProfileOption.SYNC;
    } else {
      return ProfileOption.ASYNC;
    }
  }

  private class ConnectionClosedListener implements GenericFutureListener<Future<Void>> {
    @Override
    public void operationComplete(Future<Void> future) throws Exception {
      cancel();
    }
  }

  /**
   * Get the QueryContext created for the query.
   *
   * @return the QueryContext
   */
  public QueryContext getQueryContext() {
    return queryContext;
  }

  /**
   * Get the QueryManager created for the query.
   *
   * @return the QueryManager
   */
  public QueryManager getQueryManager() {
    return queryManager;
  }

  /**
   * Cancel the query. Asynchronous -- it may take some time for all remote fragments to be terminated.
   * For planning and enqueued states we cancel immediately since these states are done locally.
   *
   * Note this can be called from outside of run() on another thread, or after run() completes
   */
  public void cancel() {
    if (QueryState.PLANNING == state || QueryState.ENQUEUED == state) {
      moveToState(QueryState.CANCELLATION_REQUESTED, null);
    }
    addToEventQueue(QueryState.CANCELLATION_REQUESTED, null);
  }

  /**
   * Resume the query. Regardless of the current state, this method sends a resume signal to all fragments.
   * This method can be called multiple times.
   */
  public void resume() {
    resume = true;
    // resume all pauses through query context
    queryContext.getExecutionControls().unpauseAll();
    // resume all pauses through all fragment contexts
    queryManager.unpauseExecutingFragments(drillbitContext);
  }

  /**
   * Called by execution pool to do query setup, and kick off remote execution.
   *
   * <p>Note that completion of this function is not the end of the Foreman's role
   * in the query's lifecycle.
   */
  @Override
  public void run() {
    // rename the thread we're using for debugging purposes
    final Thread currentThread = Thread.currentThread();
    final String originalName = currentThread.getName();
    currentThread.setName(queryIdString + ":foreman");
    try {
      /*
       Check if the foreman is ONLINE. If not dont accept any new queries.
      */
      if (!drillbitContext.isForemanOnline()) {
        throw new ForemanException("Query submission failed since Foreman is shutting down.");
      }
    } catch (ForemanException e) {
      logger.debug("Failure while submitting query", e);
      addToEventQueue(QueryState.FAILED, e);
    }
    // track how long the query takes
    queryManager.markStartTime();
    runningQueries.inc();

    queryText = queryRequest.getPlan();
    recordNewState(QueryState.PLANNING);
    planningQueries.inc();

    try {
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-beginning", ForemanException.class);

      // convert a run query request into action
      switch (queryRequest.getType()) {
      case LOGICAL:
        parseAndRunLogicalPlan(queryRequest.getPlan());
        break;
      case PHYSICAL:
        parseAndRunPhysicalPlan(queryRequest.getPlan());
        break;
      case SQL:
        final String sql = queryRequest.getPlan();
        // log query id and query text before starting any real work. Also, put
        // them together such that it is easy to search based on query id
        logger.info("Query text for query id {}: {}", this.queryIdString, sql);
        runSQL(sql);
        break;
      case EXECUTION:
        runFragment(queryRequest.getFragmentsList());
        break;
      case PREPARED_STATEMENT:
        runPreparedStatement(queryRequest.getPreparedStatementHandle());
        break;
      default:
        throw new IllegalStateException();
      }
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-end", ForemanException.class);
    } catch (final OutOfMemoryException e) {
      moveToState(QueryState.FAILED, UserException.memoryError(e).build(logger));
    } catch (final ForemanException e) {
      moveToState(QueryState.FAILED, e);
    } catch (AssertionError | Exception ex) {
      moveToState(QueryState.FAILED,
          new ForemanException("Unexpected exception during fragment initialization: " + ex.getMessage(), ex));
    } catch (final OutOfMemoryError e) {
      if ("Direct buffer memory".equals(e.getMessage())) {
        moveToState(QueryState.FAILED,
            UserException.resourceError(e)
                .message("One or more nodes ran out of memory while executing the query.")
                .build(logger));
      } else {
        /*
         * FragmentExecutors use a DrillbitStatusListener to watch out for the death of their query's Foreman. So, if we
         * die here, they should get notified about that, and cancel themselves; we don't have to attempt to notify
         * them, which might not work under these conditions.
         */
        CatastrophicFailure.exit(e, "Unable to handle out of memory condition in Foreman.", -1);
      }

    } finally {
      // restore the thread's original name
      currentThread.setName(originalName);
    }

    /*
     * Note that despite the run() completing, the Foreman continues to exist, and receives
     * events (indirectly, through the QueryManager's use of stateListener), about fragment
     * completions. It won't go away until everything is completed, failed, or cancelled.
     */
  }

  public void startProcessingEvents() {
      /*
       * Begin accepting external events.
       *
       * Doing this here in the finally clause will guarantee that it occurs. Otherwise, if there
       * is an exception anywhere during setup, it wouldn't occur, and any events that are generated
       * as a result of any partial setup that was done (such as the FragmentSubmitListener,
       * the ResponseSendListener, or an external call to cancel()), will hang the thread that makes the
       * event delivery call.
       *
       * If we do throw an exception during setup, and have already moved to QueryState.FAILED, we just need to
       * make sure that we can't make things any worse as those events are delivered, but allow
       * any necessary remaining cleanup to proceed.
       *
       * Note that cancellations cannot be simulated before this point, i.e. pauses can be injected, because Foreman
       * would wait on the cancelling thread to signal a resume and the cancelling thread would wait on the Foreman
       * to accept events.
       */
    try {
      stateSwitch.start();
    } catch (Exception ex) {
      moveToState(QueryState.FAILED, ex);
    }

    // If we received the resume signal before fragments are setup, the first call does not actually resume the
    // fragments. Since setup is done, all fragments must have been delivered to remote nodes. Now we can resume.
    if(resume) {
      resume();
    }
  }

  private void parseAndRunLogicalPlan(final String json) throws ExecutionSetupException {
    LogicalPlan logicalPlan;
    try {
      logicalPlan = drillbitContext.getPlanReader().readLogicalPlan(json);
    } catch (final IOException e) {
      throw new ForemanException("Failure parsing logical plan.", e);
    }

    if (logicalPlan.getProperties().resultMode == ResultMode.LOGICAL) {
      throw new ForemanException(
          "Failure running plan.  You requested a result mode of LOGICAL and submitted a logical plan.  In this case you're output mode must be PHYSICAL or EXEC.");
    }

    log(logicalPlan);

    final PhysicalPlan physicalPlan = convert(logicalPlan);

    if (logicalPlan.getProperties().resultMode == ResultMode.PHYSICAL) {
      returnPhysical(physicalPlan);
      return;
    }

    log(physicalPlan);
    runPhysicalPlan(physicalPlan);
  }

  private void log(final LogicalPlan plan) {
    if (logger.isDebugEnabled()) {
      logger.debug("Logical {}", plan.unparse(queryContext.getLpPersistence()));
    }
  }

  private void log(final PhysicalPlan plan) {
    if (logger.isDebugEnabled()) {
      try {
        final String planText = queryContext.getLpPersistence().getMapper().writeValueAsString(plan);
        logger.debug("Physical {}", planText);
      } catch (final IOException e) {
        logger.warn("Error while attempting to log physical plan.", e);
      }
    }
  }

  private void returnPhysical(final PhysicalPlan plan) throws ExecutionSetupException {
    final String jsonPlan = plan.unparse(queryContext.getLpPersistence().getMapper().writer());
    runPhysicalPlan(DirectPlan.createDirectPlan(queryContext, new PhysicalFromLogicalExplain(jsonPlan)));
  }

  public static class PhysicalFromLogicalExplain {
    public final String json;

    public PhysicalFromLogicalExplain(final String json) {
      this.json = json;
    }
  }

  private void parseAndRunPhysicalPlan(final String json) throws ExecutionSetupException {
    try {
      final PhysicalPlan plan = drillbitContext.getPlanReader().readPhysicalPlan(json);
      runPhysicalPlan(plan);
    } catch (final IOException e) {
      throw new ForemanSetupException("Failure while parsing physical plan.", e);
    }
  }

  private void runPhysicalPlan(final PhysicalPlan plan) throws ExecutionSetupException {
    validatePlan(plan);

    queryRM.visitAbstractPlan(plan);
    final QueryWorkUnit work = getQueryWorkUnit(plan);
    queryRM.visitPhysicalPlan(work);
    queryRM.setCost(plan.totalCost());
    queryManager.setTotalCost(plan.totalCost());
    work.applyPlan(drillbitContext.getPlanReader());
    logWorkUnit(work);

    fragmentsRunner.setPlanFragments(work.getFragments());
    fragmentsRunner.setRootPlanFragment(work.getRootFragment());
    fragmentsRunner.setRootOperator(work.getRootOperator());

    admit();
  }

  private void admit() throws ForemanSetupException {
    queryManager.markPlanningEndTime();
    planningQueries.dec();
    moveToState(QueryState.ENQUEUED, null);
    queryRM.admit();
  }

  /**
   * This is a helper method to run query based on the list of PlanFragment that were planned
   * at some point of time
   * @param fragmentsList fragment list
   * @throws ExecutionSetupException
   */
  private void runFragment(List<PlanFragment> fragmentsList) throws ExecutionSetupException {
    // need to set QueryId, MinorFragment for incoming Fragments
    PlanFragment rootFragment = null;
    boolean isFirst = true;
    final List<PlanFragment> planFragments = Lists.newArrayList();
    for (PlanFragment myFragment : fragmentsList) {
      final FragmentHandle handle = myFragment.getHandle();
      // though we have new field in the FragmentHandle - parentQueryId
      // it can not be used until every piece of code that creates handle is using it, as otherwise
      // comparisons on that handle fail that causes fragment runtime failure
      final FragmentHandle newFragmentHandle = FragmentHandle.newBuilder().setMajorFragmentId(handle.getMajorFragmentId())
          .setMinorFragmentId(handle.getMinorFragmentId()).setQueryId(queryId)
          .build();
      final PlanFragment newFragment = PlanFragment.newBuilder(myFragment).setHandle(newFragmentHandle).build();
      if (isFirst) {
        rootFragment = newFragment;
        isFirst = false;
      } else {
        planFragments.add(newFragment);
      }
    }

    assert rootFragment != null;

    final FragmentRoot rootOperator;
    try {
      rootOperator = drillbitContext.getPlanReader().readFragmentRoot(rootFragment.getFragmentJson());
    } catch (IOException e) {
      throw new ExecutionSetupException(String.format("Unable to parse FragmentRoot from fragment: %s", rootFragment.getFragmentJson()));
    }
    queryRM.setCost(rootOperator.getCost());

    fragmentsRunner.setPlanFragments(planFragments);
    fragmentsRunner.setRootPlanFragment(rootFragment);
    fragmentsRunner.setRootOperator(rootOperator);

    admit();
  }

  /**
   * Helper method to execute the query in prepared statement. Current implementation takes the query from opaque
   * object of the <code>preparedStatement</code> and submits as a new query.
   *
   * @param preparedStatementHandle prepared statement handle
   * @throws ExecutionSetupException
   */
  private void runPreparedStatement(final PreparedStatementHandle preparedStatementHandle)
      throws ExecutionSetupException {
    final ServerPreparedStatementState serverState;

    try {
      serverState =
          ServerPreparedStatementState.PARSER.parseFrom(preparedStatementHandle.getServerInfo());
    } catch (final InvalidProtocolBufferException ex) {
      throw UserException.parseError(ex)
          .message("Failed to parse the prepared statement handle. " +
              "Make sure the handle is same as one returned from create prepared statement call.")
          .build(logger);
    }

    queryText = serverState.getSqlQuery();
    logger.info("Prepared statement query for QueryId {} : {}", queryId, queryText);
    runSQL(queryText);

  }

  private static void validatePlan(final PhysicalPlan plan) throws ForemanSetupException {
    if (plan.getProperties().resultMode != ResultMode.EXEC) {
      throw new ForemanSetupException(String.format(
          "Failure running plan.  You requested a result mode of %s and a physical plan can only be output as EXEC",
          plan.getProperties().resultMode));
    }
  }

  private QueryWorkUnit getQueryWorkUnit(final PhysicalPlan plan) throws ExecutionSetupException {
    final PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
    final Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext);
    return parallelizer.getFragments(
        queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(),
        queryId, queryContext.getOnlineEndpoints(), rootFragment,
        initiatingClient.getSession(), queryContext.getQueryContextInfo());
  }

  private void logWorkUnit(QueryWorkUnit queryWorkUnit) {
    if (! logger.isTraceEnabled()) {
      return;
    }
    logger.trace(String.format("PlanFragments for query %s \n%s",
        queryId, queryWorkUnit.convertFragmentToJson()));
  }

  /**
   * Manages the end-state processing for Foreman.
   *
   * End-state processing is tricky, because even if a query appears to succeed, but
   * we then encounter a problem during cleanup, we still want to mark the query as
   * failed. So we have to construct the successful result we would send, and then
   * clean up before we send that result, possibly changing that result if we encounter
   * a problem during cleanup. We only send the result when there is nothing left to
   * do, so it will account for any possible problems.
   *
   * The idea here is to make close()ing the ForemanResult do the final cleanup and
   * sending. Closing the result must be the last thing that is done by Foreman.
   */
  private class ForemanResult implements AutoCloseable {
    private QueryState resultState = null;
    private volatile Exception resultException = null;
    private boolean isClosed = false;

    /**
     * Set up the result for a COMPLETED or CANCELED state.
     *
     * <p>Note that before sending this result, we execute cleanup steps that could
     * result in this result still being changed to a FAILED state.
     *
     * @param queryState one of COMPLETED or CANCELED
     */
    public void setCompleted(final QueryState queryState) {
      Preconditions.checkArgument((queryState == QueryState.COMPLETED) || (queryState == QueryState.CANCELED));
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);

      resultState = queryState;
    }

    /**
     * Set up the result for a FAILED state.
     *
     * <p>Failures that occur during cleanup processing will be added as suppressed
     * exceptions.
     *
     * @param exception the exception that led to the FAILED state
     */
    public void setFailed(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState == null);

      resultState = QueryState.FAILED;
      resultException = exception;
    }

    /**
     * Ignore the current status and force the given failure as current status.
     * NOTE: Used only for testing purposes. Shouldn't be used in production.
     */
    public void setForceFailure(final Exception exception) {
      Preconditions.checkArgument(exception != null);
      Preconditions.checkState(!isClosed);

      resultState = QueryState.FAILED;
      resultException = exception;
    }

    /**
     * Add an exception to the result. All exceptions after the first become suppressed
     * exceptions hanging off the first.
     *
     * @param exception the exception to add
     */
    private void addException(final Exception exception) {
      assert exception != null;

      if (resultException == null) {
        resultException = exception;
      } else {
        resultException.addSuppressed(exception);
      }
    }

    /**
     * Expose the current exception (if it exists). This is useful for secondary reporting to the query profile.
     *
     * @return the current Foreman result exception or null.
     */
    public Exception getException() {
      return resultException;
    }

    /**
     * Close the given resource, catching and adding any caught exceptions via {@link #addException(Exception)}. If an
     * exception is caught, it will change the result state to FAILED, regardless of what its current value.
     *
     * @param autoCloseable
     *          the resource to close
     */
    private void suppressingClose(final AutoCloseable autoCloseable) {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      if (autoCloseable == null) {
        return;
      }

      try {
        autoCloseable.close();
      } catch(final Exception e) {
        /*
         * Even if the query completed successfully, we'll still report failure if we have
         * problems cleaning up.
         */
        resultState = QueryState.FAILED;
        addException(e);
      }
    }

    private void logQuerySummary() {
      try {
        LoggedQuery q = new LoggedQuery(
            queryIdString,
            queryContext.getQueryContextInfo().getDefaultSchemaName(),
            queryText,
            new Date(queryContext.getQueryContextInfo().getQueryStartTime()),
            new Date(System.currentTimeMillis()),
            state,
            queryContext.getSession().getCredentials().getUserName(),
            initiatingClient.getRemoteAddress());
        queryLogger.info(MAPPER.writeValueAsString(q));
      } catch (Exception e) {
        logger.error("Failure while recording query information to query log.", e);
      }
    }

    @SuppressWarnings("resource")
    @Override
    public void close() {
      Preconditions.checkState(!isClosed);
      Preconditions.checkState(resultState != null);

      // to track how long the query takes
      queryManager.markEndTime();

      logger.debug(queryIdString + ": cleaning up.");
      injector.injectPause(queryContext.getExecutionControls(), "foreman-cleanup", logger);

      // remove the channel disconnected listener (doesn't throw)
      closeFuture.removeListener(closeListener);

      // log the query summary
      logQuerySummary();

      // These are straight forward removals from maps, so they won't throw.
      drillbitContext.getWorkBus().removeFragmentStatusListener(queryId);
      drillbitContext.getClusterCoordinator().removeDrillbitStatusListener(queryManager.getDrillbitStatusListener());

      suppressingClose(queryContext);

      /*
       * We do our best to write the latest state, but even that could fail. If it does, we can't write
       * the (possibly newly failing) state, so we continue on anyway.
       *
       * We only need to do this if the resultState differs from the last recorded state
       */
      if (resultState != state) {
        suppressingClose(new AutoCloseable() {
          @Override
          public void close() throws Exception {
            recordNewState(resultState);
          }
        });
      }

      /*
       * Construct the response based on the latest resultState. The builder shouldn't fail.
       */
      final QueryResult.Builder resultBuilder = QueryResult.newBuilder()
          .setQueryId(queryId)
          .setQueryState(resultState);
      final UserException uex;
      if (resultException != null) {
        final boolean verbose = queryContext.getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
        uex = UserException.systemError(resultException).addIdentity(queryContext.getCurrentEndpoint()).build(logger);
        resultBuilder.addError(uex.getOrCreatePBError(verbose));
      } else {
        uex = null;
      }

      // Debug option: write query profile before sending final results so that
      // the client can be certain the profile exists.

      if (profileOption == ProfileOption.SYNC) {
        queryManager.writeFinalProfile(uex);
      }

      /*
       * If sending the result fails, we don't really have any way to modify the result we tried to send;
       * it is possible it got sent but the result came from a later part of the code path. It is also
       * possible the connection has gone away, so this is irrelevant because there's nowhere to
       * send anything to.
       */
      try {
        // send whatever result we ended up with
        initiatingClient.sendResult(responseListener, resultBuilder.build());
      } catch(final Exception e) {
        addException(e);
        logger.warn("Exception sending result to client", resultException);
      }

      // Store the final result here so we can capture any error/errorId in the
      // profile for later debugging.
      // Normal behavior is to write the query profile AFTER sending results to the user.
      // The observed
      // user behavior is a possible time-lag between query return and appearance
      // of the query profile in persistent storage. Also, the query might
      // succeed, but the profile never appear if the profile write fails. This
      // behavior is acceptable for an eventually-consistent distributed system.
      // The key benefit is that the client does not wait for the persistent
      // storage write; query completion occurs in parallel with profile
      // persistence.

      if (profileOption == ProfileOption.ASYNC) {
        queryManager.writeFinalProfile(uex);
      }

      // Remove the Foreman from the running query list.
      fragmentsRunner.getBee().retireForeman(Foreman.this);

      try {
        queryManager.close();
      } catch (final Exception e) {
        logger.warn("unable to close query manager", e);
      }

      // Incrementing QueryState counters
      switch (state) {
        case FAILED:
          failedQueries.inc();
          break;
        case CANCELED:
          canceledQueries.inc();
          break;
        case COMPLETED:
          succeededQueries.inc();
          break;
      }

      runningQueries.dec();
      completedQueries.inc();
      try {
        queryRM.exit();
      } finally {
        isClosed = true;
      }
    }
  }

  private static class StateEvent {
    final QueryState newState;
    final Exception exception;

    StateEvent(final QueryState newState, final Exception exception) {
      this.newState = newState;
      this.exception = exception;
    }
  }

  public synchronized void moveToState(final QueryState newState, final Exception exception) {
    logger.debug(queryIdString + ": State change requested {} --> {}", state, newState, exception);
    switch (state) {
      case PLANNING:
        switch (newState) {
          case ENQUEUED:
            recordNewState(newState);
            enqueuedQueries.inc();
            return;
          case CANCELLATION_REQUESTED:
            assert exception == null;
            recordNewState(newState);
            foremanResult.setCompleted(QueryState.CANCELED);
            foremanResult.close();
            return;
          case FAILED:
            assert exception != null;
            recordNewState(newState);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;
        }
        break;
      case ENQUEUED:
        enqueuedQueries.dec();
        queryManager.markQueueWaitEndTime();
        switch (newState) {
          case STARTING:
            recordNewState(newState);
            queryManager.setQueueName(queryRM.queueName());
            fragmentsRunner.submit();
            return;
          case CANCELLATION_REQUESTED:
            assert exception == null;
            recordNewState(newState);
            queryRM.cancel();
            foremanResult.setCompleted(QueryState.CANCELED);
            return;
          case FAILED:
            assert exception != null;
            recordNewState(newState);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;
        }
        break;
      case STARTING:
        switch (newState) {
          case RUNNING:
            recordNewState(QueryState.RUNNING);
            return;
          case CANCELLATION_REQUESTED:
            // since during starting state fragments are sent to the remote nodes,
            // we don't want to cancel until they all are sent out
            addToEventQueue(QueryState.CANCELLATION_REQUESTED, null);
            return;
        }

        //$FALL-THROUGH$

      case RUNNING: {
      /*
       * For cases that cancel executing fragments, we have to record the new
       * state first, because the cancellation of the local root fragment will
       * cause this to be called recursively.
       */
        switch (newState) {
          case CANCELLATION_REQUESTED: {
            assert exception == null;
            recordNewState(QueryState.CANCELLATION_REQUESTED);
            queryManager.cancelExecutingFragments(drillbitContext);
            foremanResult.setCompleted(QueryState.CANCELED);
        /*
         * We don't close the foremanResult until we've gotten
         * acknowledgments, which happens below in the case for current state
         * == CANCELLATION_REQUESTED.
         */
            return;
          }

          case COMPLETED: {
            assert exception == null;
            recordNewState(QueryState.COMPLETED);
            foremanResult.setCompleted(QueryState.COMPLETED);
            foremanResult.close();
            return;
          }

          case FAILED: {
            assert exception != null;
            recordNewState(QueryState.FAILED);
            queryManager.cancelExecutingFragments(drillbitContext);
            foremanResult.setFailed(exception);
            foremanResult.close();
            return;
          }

        }
        break;
      }

      case CANCELLATION_REQUESTED:
        if ((newState == QueryState.CANCELED) || (newState == QueryState.COMPLETED) || (newState == QueryState.FAILED)) {

          if (drillbitContext.getConfig().getBoolean(ExecConstants.RETURN_ERROR_FOR_FAILURE_IN_CANCELLED_FRAGMENTS)) {
            if (newState == QueryState.FAILED) {
              assert exception != null;
              recordNewState(QueryState.FAILED);
              foremanResult.setForceFailure(exception);
            }
          }
        /*
         * These amount to a completion of the cancellation requests' cleanup;
         * now we can clean up and send the result.
         */
          foremanResult.close();
        }
        return;

      case CANCELED:
      case COMPLETED:
      case FAILED:
        logger.warn("Dropping request to move to {} state as query is already at {} state (which is terminal).", newState, state);
        return;
    }

    throw new IllegalStateException(String.format("Failure trying to change states: %s --> %s", state.name(), newState.name()));
  }

  private class StateSwitch extends EventProcessor<StateEvent> {
    public void addEvent(final QueryState newState, final Exception exception) {
      sendEvent(new StateEvent(newState, exception));
    }

    @Override
    protected void processEvent(final StateEvent event) {
      moveToState(event.newState, event.exception);
    }
  }

  /**
   * Tells the foreman to move to a new state.<br>
   * This will be added to the end of the event queue and will be processed once the foreman is ready
   * to accept external events.
   *
   * @param newState the state to move to
   * @param exception if not null, the exception that drove this state transition (usually a failure)
   */
  public void addToEventQueue(final QueryState newState, final Exception exception) {
    stateSwitch.addEvent(newState, exception);
  }

  private void recordNewState(final QueryState newState) {
    state = newState;
    queryManager.updateEphemeralState(newState);
  }

  private void runSQL(final String sql) throws ExecutionSetupException {
    final Pointer<String> textPlan = new Pointer<>();
    final PhysicalPlan plan = DrillSqlWorker.getPlan(queryContext, sql, textPlan);
    queryManager.setPlanText(textPlan.value);
    runPhysicalPlan(plan);
  }

  private PhysicalPlan convert(final LogicalPlan plan) throws OptimizerException {
    if (logger.isDebugEnabled()) {
      logger.debug("Converting logical plan {}.", plan.toJsonStringSafe(queryContext.getLpPersistence()));
    }
    return new BasicOptimizer(queryContext, initiatingClient).optimize(
        new BasicOptimizer.BasicOptimizationContext(queryContext), plan);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public QueryState getState() {
    return state;
  }

  /**
   * @return sql query text of the query request
   */
  public String getQueryText() {
    return queryText;
  }


  /**
   * Listens for the status of the RPC response sent to the user for the query.
   */
  private class ResponseSendListener extends BaseRpcOutcomeListener<Ack> {
    @Override
    public void failed(final RpcException ex) {
      logger.info("Failure while trying communicate query result to initiating client. " +
              "This would happen if a client is disconnected before response notice can be sent.", ex);
    }

    @Override
    public void interrupted(final InterruptedException e) {
      logger.warn("Interrupted while waiting for RPC outcome of sending final query result to initiating client.");
    }
  }
}
