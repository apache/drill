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
package org.apache.drill.exec.work.foreman;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.EventProcessor;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentStatusReporter;
import org.apache.drill.exec.work.fragment.RootFragmentManager;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Foreman manages all the fragments (local and remote) for a single query where this
 * is the driving/root node.
 *
 * The flow is as follows:
 * - Foreman is submitted as a runnable.
 * - Runnable does query planning.
 * - state changes from PENDING to RUNNING
 * - Runnable sends out starting fragments
 * - Status listener are activated
 * - The Runnable's run() completes, but the Foreman stays around
 * - Foreman listens for state change messages.
 * - state change messages can drive the state to FAILED or CANCELED, in which case
 *   messages are sent to running fragments to terminate
 * - when all fragments complete, state change messages drive the state to COMPLETED
 */
public class Foreman implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);
  private static final org.slf4j.Logger queryLogger = org.slf4j.LoggerFactory.getLogger("query.logger");
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(Foreman.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final long RPC_WAIT_IN_MSECS_PER_FRAGMENT = 5000;

  private final QueryId queryId;
  private final String queryIdString;
  private final RunQuery queryRequest;
  private final QueryContext queryContext;
  private final QueryManager queryManager; // handles lower-level details of query execution
  private final WorkerBee bee; // provides an interface to submit tasks
  private final DrillbitContext drillbitContext;
  private final UserClientConnection initiatingClient; // used to send responses
  private volatile QueryState state;
  private boolean resume = false;

  private volatile DistributedLease lease; // used to limit the number of concurrent queries

  private final ExtendedLatch acceptExternalEvents = new ExtendedLatch(); // gates acceptance of external events
  private final StateListener stateListener = new StateListener(); // source of external events
  private final ResponseSendListener responseListener = new ResponseSendListener();
  private final StateSwitch stateSwitch = new StateSwitch();
  private final ForemanResult foremanResult = new ForemanResult();
  private final ConnectionClosedListener closeListener = new ConnectionClosedListener();
  private final ChannelFuture closeFuture;

  private String queryText;

  /**
   * Constructor. Sets up the Foreman, but does not initiate any execution.
   *
   * @param bee used to submit additional work
   * @param drillbitContext
   * @param connection
   * @param queryId the id for the query
   * @param queryRequest the query to execute
   */
  public Foreman(final WorkerBee bee, final DrillbitContext drillbitContext,
      final UserClientConnection connection, final QueryId queryId, final RunQuery queryRequest) {
    this.bee = bee;
    this.queryId = queryId;
    queryIdString = QueryIdHelper.getQueryId(queryId);
    this.queryRequest = queryRequest;
    this.drillbitContext = drillbitContext;

    initiatingClient = connection;
    this.closeFuture = initiatingClient.getChannel().closeFuture();
    closeFuture.addListener(closeListener);

    queryContext = new QueryContext(connection.getSession(), drillbitContext);
    queryManager = new QueryManager(queryId, queryRequest, drillbitContext.getPersistentStoreProvider(),
        stateListener, this); // TODO reference escapes before ctor is complete via stateListener, this

    recordNewState(QueryState.PENDING);
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
   * Cancel the query. Asynchronous -- it may take some time for all remote fragments to be
   * terminated.
   */
  public void cancel() {
    // Note this can be called from outside of run() on another thread, or after run() completes
    stateListener.moveToState(QueryState.CANCELLATION_REQUESTED, null);
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

    // track how long the query takes
    queryManager.markStartTime();

    try {
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-beginning", ForemanException.class);
      queryText = queryRequest.getPlan();

      // convert a run query request into action
      switch (queryRequest.getType()) {
      case LOGICAL:
        parseAndRunLogicalPlan(queryRequest.getPlan());
        break;
      case PHYSICAL:
        parseAndRunPhysicalPlan(queryRequest.getPlan());
        break;
      case SQL:
        runSQL(queryRequest.getPlan());
        break;
      default:
        throw new IllegalStateException();
      }
      injector.injectChecked(queryContext.getExecutionControls(), "run-try-end", ForemanException.class);
    } catch (final OutOfMemoryException | OutOfMemoryRuntimeException e) {
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
        System.out.println("Node ran out of Heap memory, exiting.");
        e.printStackTrace();
        System.out.flush();
        System.exit(-1);
      }

    } finally {
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
      acceptExternalEvents.countDown();

      // If we received the resume signal before fragments are setup, the first call does not actually resume the
      // fragments. Since setup is done, all fragments must have been delivered to remote nodes. Now we can resume.
      if(resume) {
        resume();
      }

      // restore the thread's original name
      currentThread.setName(originalName);
    }

    /*
     * Note that despite the run() completing, the Foreman continues to exist, and receives
     * events (indirectly, through the QueryManager's use of stateListener), about fragment
     * completions. It won't go away until everything is completed, failed, or cancelled.
     */
  }

  private void releaseLease() {
    while (lease != null) {
      try {
        lease.close();
        lease = null;
      } catch (final InterruptedException e) {
        // if we end up here, the while loop will try again
      } catch (final Exception e) {
        logger.warn("Failure while releasing lease.", e);
        break;
      }
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
    setupSortMemoryAllocations(plan);
    acquireQuerySemaphore(plan);

    final QueryWorkUnit work = getQueryWorkUnit(plan);
    final List<PlanFragment> planFragments = work.getFragments();
    final PlanFragment rootPlanFragment = work.getRootFragment();
    assert queryId == rootPlanFragment.getHandle().getQueryId();

    drillbitContext.getWorkBus().addFragmentStatusListener(queryId, queryManager.getFragmentStatusListener());
    drillbitContext.getClusterCoordinator().addDrillbitStatusListener(queryManager.getDrillbitStatusListener());

    logger.debug("Submitting fragments to run.");

    // set up the root fragment first so we'll have incoming buffers available.
    setupRootFragment(rootPlanFragment, work.getRootOperator());

    setupNonRootFragments(planFragments);
    drillbitContext.getAllocator().resetFragmentLimits(); // TODO a global effect for this query?!?

    moveToState(QueryState.RUNNING, null);
    logger.debug("Fragments running.");
  }

  private static void validatePlan(final PhysicalPlan plan) throws ForemanSetupException {
    if (plan.getProperties().resultMode != ResultMode.EXEC) {
      throw new ForemanSetupException(String.format(
          "Failure running plan.  You requested a result mode of %s and a physical plan can only be output as EXEC",
          plan.getProperties().resultMode));
    }
  }

  private void setupSortMemoryAllocations(final PhysicalPlan plan) {
    // look for external sorts
    final List<ExternalSort> sortList = new LinkedList<>();
    for (final PhysicalOperator op : plan.getSortedOperators()) {
      if (op instanceof ExternalSort) {
        sortList.add((ExternalSort) op);
      }
    }

    // if there are any sorts, compute the maximum allocation, and set it on them
    if (sortList.size() > 0) {
      final OptionManager optionManager = queryContext.getOptions();
      final long maxWidthPerNode = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = Math.min(DrillConfig.getMaxDirectMemory(),
          queryContext.getConfig().getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC));
      maxAllocPerNode = Math.min(maxAllocPerNode,
          optionManager.getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).num_val);
      final long maxSortAlloc = maxAllocPerNode / (sortList.size() * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);

      for(final ExternalSort externalSort : sortList) {
        externalSort.setMaxAllocation(maxSortAlloc);
      }
    }
  }

  /**
   * This limits the number of "small" and "large" queries that a Drill cluster will run
   * simultaneously, if queueing is enabled. If the query is unable to run, this will block
   * until it can. Beware that this is called under run(), and so will consume a Thread
   * while it waits for the required distributed semaphore.
   *
   * @param plan the query plan
   * @throws ForemanSetupException
   */
  private void acquireQuerySemaphore(final PhysicalPlan plan) throws ForemanSetupException {
    final OptionManager optionManager = queryContext.getOptions();
    final boolean queuingEnabled = optionManager.getOption(ExecConstants.ENABLE_QUEUE);
    if (queuingEnabled) {
      final long queueThreshold = optionManager.getOption(ExecConstants.QUEUE_THRESHOLD_SIZE);
      double totalCost = 0;
      for (final PhysicalOperator ops : plan.getSortedOperators()) {
        totalCost += ops.getCost();
      }

      final long queueTimeout = optionManager.getOption(ExecConstants.QUEUE_TIMEOUT);
      final String queueName;

      try {
        @SuppressWarnings("resource")
        final ClusterCoordinator clusterCoordinator = drillbitContext.getClusterCoordinator();
        final DistributedSemaphore distributedSemaphore;

        // get the appropriate semaphore
        if (totalCost > queueThreshold) {
          final int largeQueue = (int) optionManager.getOption(ExecConstants.LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.large", largeQueue);
          queueName = "large";
        } else {
          final int smallQueue = (int) optionManager.getOption(ExecConstants.SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.small", smallQueue);
          queueName = "small";
        }


        lease = distributedSemaphore.acquire(queueTimeout, TimeUnit.MILLISECONDS);
      } catch (final Exception e) {
        throw new ForemanSetupException("Unable to acquire slot for query.", e);
      }

      if (lease == null) {
        throw UserException
            .resourceError()
            .message(
                "Unable to acquire queue resources for query within timeout.  Timeout for %s queue was set at %d seconds.",
                queueName, queueTimeout / 1000)
            .build(logger);
      }

    }
  }

  Exception getCurrentException() {
    return foremanResult.getException();
  }

  private QueryWorkUnit getQueryWorkUnit(final PhysicalPlan plan) throws ExecutionSetupException {
    final PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
    final Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext);
    final QueryWorkUnit queryWorkUnit = parallelizer.getFragments(
        queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(),
        queryId, queryContext.getActiveEndpoints(), drillbitContext.getPlanReader(), rootFragment,
        initiatingClient.getSession(), queryContext.getQueryContextInfo());

    if (logger.isTraceEnabled()) {
      final StringBuilder sb = new StringBuilder();
      sb.append("PlanFragments for query ");
      sb.append(queryId);
      sb.append('\n');

      final List<PlanFragment> planFragments = queryWorkUnit.getFragments();
      final int fragmentCount = planFragments.size();
      int fragmentIndex = 0;
      for(final PlanFragment planFragment : planFragments) {
        final FragmentHandle fragmentHandle = planFragment.getHandle();
        sb.append("PlanFragment(");
        sb.append(++fragmentIndex);
        sb.append('/');
        sb.append(fragmentCount);
        sb.append(") major_fragment_id ");
        sb.append(fragmentHandle.getMajorFragmentId());
        sb.append(" minor_fragment_id ");
        sb.append(fragmentHandle.getMinorFragmentId());
        sb.append('\n');

        final DrillbitEndpoint endpointAssignment = planFragment.getAssignment();
        sb.append("  DrillbitEndpoint address ");
        sb.append(endpointAssignment.getAddress());
        sb.append('\n');

        String jsonString = "<<malformed JSON>>";
        sb.append("  fragment_json: ");
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
          final Object json = objectMapper.readValue(planFragment.getFragmentJson(), Object.class);
          jsonString = objectMapper.defaultPrettyPrintingWriter().writeValueAsString(json);
        } catch(final Exception e) {
          // we've already set jsonString to a fallback value
        }
        sb.append(jsonString);

        logger.trace(sb.toString());
      }
    }

    return queryWorkUnit;
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
      Preconditions.checkNotNull(exception);

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
            queryContext.getSession().getCredentials().getUserName());
        queryLogger.info(MAPPER.writeValueAsString(q));
      } catch (Exception e) {
        logger.error("Failure while recording query information to query log.", e);
      }
    }

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

      // we store the final result here so we can capture any error/errorId in the profile for later debugging.
      queryManager.writeFinalProfile(uex);

      /*
       * If sending the result fails, we don't really have any way to modify the result we tried to send;
       * it is possible it got sent but the result came from a later part of the code path. It is also
       * possible the connection has gone away, so this is irrelevant because there's nowhere to
       * send anything to.
       */
      try {
        // send whatever result we ended up with
        initiatingClient.sendResult(responseListener, resultBuilder.build(), true);
      } catch(final Exception e) {
        addException(e);
        logger.warn("Exception sending result to client", resultException);
      }

      // Remove the Foreman from the running query list.
      bee.retireForeman(Foreman.this);

      try {
        releaseLease();
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

  private class StateSwitch extends EventProcessor<StateEvent> {
    public void moveToState(final QueryState newState, final Exception exception) {
      sendEvent(new StateEvent(newState, exception));
    }

    @Override
    protected void processEvent(final StateEvent event) {
      final QueryState newState = event.newState;
      final Exception exception = event.exception;

      // TODO Auto-generated method stub
      logger.debug(queryIdString + ": State change requested {} --> {}", state, newState,
          exception);
      switch (state) {
      case PENDING:
        if (newState == QueryState.RUNNING) {
          recordNewState(QueryState.RUNNING);
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
           * acknowledgements, which happens below in the case for current state
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

        default:
          throw new IllegalStateException("illegal transition from RUNNING to "
              + newState);
        }
      }

      case CANCELLATION_REQUESTED:
        if ((newState == QueryState.CANCELED)
            || (newState == QueryState.COMPLETED)
            || (newState == QueryState.FAILED)) {

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
        logger
            .warn(
                "Dropping request to move to {} state as query is already at {} state (which is terminal).",
                newState, state);
        return;
      }

      throw new IllegalStateException(String.format(
          "Failure trying to change states: %s --> %s", state.name(),
          newState.name()));
    }
  }

  /**
   * Tells the foreman to move to a new state.
   *
   * @param newState the state to move to
   * @param exception if not null, the exception that drove this state transition (usually a failure)
   */
  private void moveToState(final QueryState newState, final Exception exception) {
    stateSwitch.moveToState(newState, exception);
  }

  private void recordNewState(final QueryState newState) {
    state = newState;
    queryManager.updateEphemeralState(newState);
  }

  private void runSQL(final String sql) throws ExecutionSetupException {
    final DrillSqlWorker sqlWorker = new DrillSqlWorker(queryContext);
    final Pointer<String> textPlan = new Pointer<>();
    final PhysicalPlan plan = sqlWorker.getPlan(sql, textPlan);
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

  /**
   * Set up the root fragment (which will run locally), and submit it for execution.
   *
   * @param rootFragment
   * @param rootOperator
   * @throws ExecutionSetupException
   */
  private void setupRootFragment(final PlanFragment rootFragment, final FragmentRoot rootOperator)
      throws ExecutionSetupException {
    @SuppressWarnings("resource")
    final FragmentContext rootContext = new FragmentContext(drillbitContext, rootFragment, queryContext,
        initiatingClient, drillbitContext.getFunctionImplementationRegistry());
    @SuppressWarnings("resource")
    final IncomingBuffers buffers = new IncomingBuffers(rootFragment, rootContext);
    rootContext.setBuffers(buffers);

    queryManager.addFragmentStatusTracker(rootFragment, true);

    final ControlTunnel tunnel = drillbitContext.getController().getTunnel(queryContext.getCurrentEndpoint());
    final FragmentExecutor rootRunner = new FragmentExecutor(rootContext, rootFragment,
        new FragmentStatusReporter(rootContext, tunnel),
        rootOperator);
    final RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);

    if (buffers.isDone()) {
      // if we don't have to wait for any incoming data, start the fragment runner.
      bee.addFragmentRunner(fragmentManager.getRunnable());
    } else {
      // if we do, record the fragment manager in the workBus.
      drillbitContext.getWorkBus().addFragmentManager(fragmentManager);
    }
  }

  /**
   * Set up the non-root fragments for execution. Some may be local, and some may be remote.
   * Messages are sent immediately, so they may start returning data even before we complete this.
   *
   * @param fragments the fragments
   * @throws ForemanException
   */
  private void setupNonRootFragments(final Collection<PlanFragment> fragments) throws ForemanException {
    /*
     * We will send a single message to each endpoint, regardless of how many fragments will be
     * executed there. We need to start up the intermediate fragments first so that they will be
     * ready once the leaf fragments start producing data. To satisfy both of these, we will
     * make a pass through the fragments and put them into these two maps according to their
     * leaf/intermediate state, as well as their target drillbit.
     */
    final Multimap<DrillbitEndpoint, PlanFragment> leafFragmentMap = ArrayListMultimap.create();
    final Multimap<DrillbitEndpoint, PlanFragment> intFragmentMap = ArrayListMultimap.create();

    // record all fragments for status purposes.
    for (final PlanFragment planFragment : fragments) {
      logger.trace("Tracking intermediate remote node {} with data {}",
                   planFragment.getAssignment(), planFragment.getFragmentJson());
      queryManager.addFragmentStatusTracker(planFragment, false);
      if (planFragment.getLeafFragment()) {
        leafFragmentMap.put(planFragment.getAssignment(), planFragment);
      } else {
        intFragmentMap.put(planFragment.getAssignment(), planFragment);
      }
    }

    /*
     * We need to wait for the intermediates to be sent so that they'll be set up by the time
     * the leaves start producing data. We'll use this latch to wait for the responses.
     *
     * However, in order not to hang the process if any of the RPC requests fails, we always
     * count down (see FragmentSubmitFailures), but we count the number of failures so that we'll
     * know if any submissions did fail.
     */
    final int numIntFragments = intFragmentMap.keySet().size();
    final ExtendedLatch endpointLatch = new ExtendedLatch(numIntFragments);
    final FragmentSubmitFailures fragmentSubmitFailures = new FragmentSubmitFailures();

    // send remote intermediate fragments
    for (final DrillbitEndpoint ep : intFragmentMap.keySet()) {
      sendRemoteFragments(ep, intFragmentMap.get(ep), endpointLatch, fragmentSubmitFailures);
    }

    final long timeout = RPC_WAIT_IN_MSECS_PER_FRAGMENT * numIntFragments;
    if(numIntFragments > 0 && !endpointLatch.awaitUninterruptibly(timeout)){
      long numberRemaining = endpointLatch.getCount();
      throw UserException.connectionError()
          .message(
              "Exceeded timeout (%d) while waiting send intermediate work fragments to remote nodes. " +
                  "Sent %d and only heard response back from %d nodes.",
              timeout, numIntFragments, numIntFragments - numberRemaining)
          .build(logger);
    }

    // if any of the intermediate fragment submissions failed, fail the query
    final List<FragmentSubmitFailures.SubmissionException> submissionExceptions = fragmentSubmitFailures.submissionExceptions;
    if (submissionExceptions.size() > 0) {
      Set<DrillbitEndpoint> endpoints = Sets.newHashSet();
      StringBuilder sb = new StringBuilder();
      boolean first = true;

      for (FragmentSubmitFailures.SubmissionException e : fragmentSubmitFailures.submissionExceptions) {
        DrillbitEndpoint endpoint = e.drillbitEndpoint;
        if (endpoints.add(endpoint)) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(endpoint.getAddress());
        }
      }
      throw UserException.connectionError(submissionExceptions.get(0).rpcException)
          .message("Error setting up remote intermediate fragment execution")
          .addContext("Nodes with failures", sb.toString())
          .build(logger);
    }

    injector.injectChecked(queryContext.getExecutionControls(), "send-fragments", ForemanException.class);
    /*
     * Send the remote (leaf) fragments; we don't wait for these. Any problems will come in through
     * the regular sendListener event delivery.
     */
    for (final DrillbitEndpoint ep : leafFragmentMap.keySet()) {
      sendRemoteFragments(ep, leafFragmentMap.get(ep), null, null);
    }
  }

  /**
   * Send all the remote fragments belonging to a single target drillbit in one request.
   *
   * @param assignment the drillbit assigned to these fragments
   * @param fragments the set of fragments
   * @param latch the countdown latch used to track the requests to all endpoints
   * @param fragmentSubmitFailures the submission failure counter used to track the requests to all endpoints
   */
  private void sendRemoteFragments(final DrillbitEndpoint assignment, final Collection<PlanFragment> fragments,
      final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures) {
    @SuppressWarnings("resource")
    final Controller controller = drillbitContext.getController();
    final InitializeFragments.Builder fb = InitializeFragments.newBuilder();
    for(final PlanFragment planFragment : fragments) {
      fb.addFragment(planFragment);
    }
    final InitializeFragments initFrags = fb.build();

    logger.debug("Sending remote fragments to \nNode:\n{} \n\nData:\n{}", assignment, initFrags);
    final FragmentSubmitListener listener =
        new FragmentSubmitListener(assignment, initFrags, latch, fragmentSubmitFailures);
    controller.getTunnel(assignment).sendFragments(listener, initFrags);
  }

  public QueryState getState() {
    return state;
  }

  /**
   * Used by {@link FragmentSubmitListener} to track the number of submission failures.
   */
  private static class FragmentSubmitFailures {
    static class SubmissionException {
      final DrillbitEndpoint drillbitEndpoint;
      final RpcException rpcException;

      SubmissionException(@SuppressWarnings("unused") final DrillbitEndpoint drillbitEndpoint,
          final RpcException rpcException) {
        this.drillbitEndpoint = drillbitEndpoint;
        this.rpcException = rpcException;
      }
    }

    final List<SubmissionException> submissionExceptions = new LinkedList<>();

    void addFailure(final DrillbitEndpoint drillbitEndpoint, final RpcException rpcException) {
      submissionExceptions.add(new SubmissionException(drillbitEndpoint, rpcException));
    }
  }

  private class FragmentSubmitListener extends EndpointListener<Ack, InitializeFragments> {
    private final CountDownLatch latch;
    private final FragmentSubmitFailures fragmentSubmitFailures;

    /**
     * Constructor.
     *
     * @param endpoint the endpoint for the submission
     * @param value the initialize fragments message
     * @param latch the latch to count down when the status is known; may be null
     * @param fragmentSubmitFailures the counter to use for failures; must be non-null iff latch is non-null
     */
    public FragmentSubmitListener(final DrillbitEndpoint endpoint, final InitializeFragments value,
        final CountDownLatch latch, final FragmentSubmitFailures fragmentSubmitFailures) {
      super(endpoint, value);
      Preconditions.checkState((latch == null) == (fragmentSubmitFailures == null));
      this.latch = latch;
      this.fragmentSubmitFailures = fragmentSubmitFailures;
    }

    @Override
    public void success(final Ack ack, final ByteBuf byteBuf) {
      if (latch != null) {
        latch.countDown();
      }
    }

    @Override
    public void failed(final RpcException ex) {
      if (latch != null) {
        fragmentSubmitFailures.addFailure(endpoint, ex);
        latch.countDown();
      } else {
        // since this won't be waited on, we can wait to deliver this event once the Foreman is ready
        logger.debug("Failure while sending fragment.  Stopping query.", ex);
        stateListener.moveToState(QueryState.FAILED, ex);
      }
    }

    @Override
    public void interrupted(final InterruptedException e) {
      // Foreman shouldn't get interrupted while waiting for the RPC outcome of fragment submission.
      // Consider the interrupt as failure.
      final String errMsg = "Interrupted while waiting for the RPC outcome of fragment submission.";
      logger.error(errMsg, e);
      failed(new RpcException(errMsg, e));
    }
  }

  /**
   * Provides gated access to state transitions.
   *
   * <p>The StateListener waits on a latch before delivery state transitions to the Foreman. The
   * latch will be tripped when the Foreman is sufficiently set up that it can receive and process
   * external events from other threads.
   */
  public class StateListener {
    /**
     * Move the Foreman to the specified new state.
     *
     * @param newState the state to move to
     * @param ex if moving to a failure state, the exception that led to the failure; used for reporting
     *   to the user
     */
    public void moveToState(final QueryState newState, final Exception ex) {
      acceptExternalEvents.awaitUninterruptibly();

      Foreman.this.moveToState(newState, ex);
    }
  }

  /**
   * Listens for the status of the RPC response sent to the user for the query.
   */
  private class ResponseSendListener extends BaseRpcOutcomeListener<Ack> {
    @Override
    public void failed(final RpcException ex) {
      logger.info("Failure while trying communicate query result to initiating client. " +
              "This would happen if a client is disconnected before response notice can be sent.", ex);
      stateListener.moveToState(QueryState.FAILED, ex);
    }

    @Override
    public void interrupted(final InterruptedException e) {
      logger.warn("Interrupted while waiting for RPC outcome of sending final query result to initiating client.");
      stateListener.moveToState(QueryState.FAILED, e);
    }
  }
}
