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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.ErrorHelper;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.RootFragmentManager;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Foreman manages all queries where this is the driving/root node.
 *
 * The flow is as follows:
 *   - Foreman is submitted as a runnable.
 *   - Runnable does query planning.
 *   - PENDING > RUNNING
 *   - Runnable sends out starting fragments
 *   - Status listener are activated
 *   - Foreman listens for state move messages.
 *
 */
public class Foreman implements Runnable, Closeable, Comparable<Object> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);

  private QueryId queryId;
  private RunQuery queryRequest;
  private QueryContext context;
  private QueryManager queryManager;
  private WorkerBee bee;
  private UserClientConnection initiatingClient;
  private volatile QueryState state;

  private final DistributedSemaphore smallSemaphore;
  private final DistributedSemaphore largeSemaphore;
  private final long queueThreshold;
  private final long queueTimeout;
  private volatile DistributedLease lease;
  private final boolean queuingEnabled;

  private FragmentExecutor rootRunner;
  private final CountDownLatch acceptExternalEvents = new CountDownLatch(1);
  private final StateListener stateListener = new StateListener();
  private final ResponseSendListener responseListener = new ResponseSendListener();

  public Foreman(WorkerBee bee, DrillbitContext dContext, UserClientConnection connection, QueryId queryId,
      RunQuery queryRequest) {
    this.queryId = queryId;
    this.queryRequest = queryRequest;
    this.context = new QueryContext(connection.getSession(), queryId, dContext);

    // set up queuing
    this.queuingEnabled = context.getOptions().getOption(ExecConstants.ENABLE_QUEUE_KEY).bool_val;
    if (queuingEnabled) {
      int smallQueue = context.getOptions().getOption(ExecConstants.SMALL_QUEUE_KEY).num_val.intValue();
      int largeQueue = context.getOptions().getOption(ExecConstants.LARGE_QUEUE_KEY).num_val.intValue();
      this.largeSemaphore = dContext.getClusterCoordinator().getSemaphore("query.large", largeQueue);
      this.smallSemaphore = dContext.getClusterCoordinator().getSemaphore("query.small", smallQueue);
      this.queueThreshold = context.getOptions().getOption(ExecConstants.QUEUE_THRESHOLD_KEY).num_val;
      this.queueTimeout = context.getOptions().getOption(ExecConstants.QUEUE_TIMEOUT_KEY).num_val;
    } else {
      this.largeSemaphore = null;
      this.smallSemaphore = null;
      this.queueThreshold = 0;
      this.queueTimeout = 0;
    }
    // end queuing setup.

    this.initiatingClient = connection;
    this.queryManager = new QueryManager(queryId, queryRequest, bee.getContext().getPersistentStoreProvider(),
        stateListener, this);
    this.bee = bee;

    recordNewState(QueryState.PENDING);
  }

  public QueryContext getContext() {
    return context;
  }

  public void cancel() {
    stateListener.moveToState(QueryState.CANCELED, null);
  }

  private void cleanup(QueryResult result) {
    logger.info("foreman cleaning up - status: {}", queryManager.getStatus());

    bee.retireForeman(this);
    context.getWorkBus().removeFragmentStatusListener(queryId);
    context.getClusterCoordinator().removeDrillbitStatusListener(queryManager);
    if(result != null){
      initiatingClient.sendResult(responseListener, new QueryWritableBatch(result), true);
    }
    releaseLease();
  }

  /**
   * Called by execution pool to do foreman setup. Actual query execution is a separate phase (and can be scheduled).
   */
  public void run() {

    final String originalThread = Thread.currentThread().getName();
    Thread.currentThread().setName(QueryIdHelper.getQueryId(queryId) + ":foreman");
    getStatus().markStart();
    // convert a run query request into action
    try {
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
    } catch (ForemanException e) {
      moveToState(QueryState.FAILED, e);

    } catch (AssertionError | Exception ex) {
      moveToState(QueryState.FAILED, new ForemanException("Unexpected exception during fragment initialization: " + ex.getMessage(), ex));

    } catch (OutOfMemoryError e) {
      System.out.println("Out of memory, exiting.");
      e.printStackTrace();
      System.out.flush();
      System.exit(-1);

    } finally {
      Thread.currentThread().setName(originalThread);
    }
  }

  private void releaseLease() {
    if (lease != null) {
      try {
        lease.close();
      } catch (Exception e) {
        logger.warn("Failure while releasing lease.", e);
      }
      ;
    }

  }

  private void parseAndRunLogicalPlan(String json) throws ExecutionSetupException {
    LogicalPlan logicalPlan;
    try {
      logicalPlan = context.getPlanReader().readLogicalPlan(json);
    } catch (IOException e) {
      throw new ForemanException("Failure parsing logical plan.", e);
    }

    if (logicalPlan.getProperties().resultMode == ResultMode.LOGICAL) {
      throw new ForemanException(
          "Failure running plan.  You requested a result mode of LOGICAL and submitted a logical plan.  In this case you're output mode must be PHYSICAL or EXEC.");
    }

    log(logicalPlan);

    PhysicalPlan physicalPlan = convert(logicalPlan);

    if (logicalPlan.getProperties().resultMode == ResultMode.PHYSICAL) {
      returnPhysical(physicalPlan);
      return;
    }

    log(physicalPlan);

    runPhysicalPlan(physicalPlan);
  }

  private void log(LogicalPlan plan) {
    if (logger.isDebugEnabled()) {
      logger.debug("Logical {}", plan.unparse(context.getConfig()));
    }
  }

  private void log(PhysicalPlan plan) {
    if (logger.isDebugEnabled()) {
      try {
        String planText = context.getConfig().getMapper().writeValueAsString(plan);
        logger.debug("Physical {}", planText);
      } catch (IOException e) {
        logger.warn("Error while attempting to log physical plan.", e);
      }
    }
  }

  private void returnPhysical(PhysicalPlan plan) throws ExecutionSetupException {
    String jsonPlan = plan.unparse(context.getConfig().getMapper().writer());
    runPhysicalPlan(DirectPlan.createDirectPlan(context, new PhysicalFromLogicalExplain(jsonPlan)));
  }

  public static class PhysicalFromLogicalExplain {
    public String json;

    public PhysicalFromLogicalExplain(String json) {
      super();
      this.json = json;
    }

  }

  private void parseAndRunPhysicalPlan(String json) throws ExecutionSetupException {
    try {
      PhysicalPlan plan = context.getPlanReader().readPhysicalPlan(json);
      runPhysicalPlan(plan);
    } catch (IOException e) {
      throw new ForemanSetupException("Failure while parsing physical plan.", e);
    }
  }

  private void runPhysicalPlan(PhysicalPlan plan) throws ExecutionSetupException {

    validatePlan(plan);
    setupSortMemoryAllocations(plan);
    acquireQuerySemaphore(plan);

    final QueryWorkUnit work = getQueryWorkUnit(plan);

    this.context.getWorkBus().setFragmentStatusListener(work.getRootFragment().getHandle().getQueryId(), queryManager);
    this.context.getClusterCoordinator().addDrillbitStatusListener(queryManager);

    logger.debug("Submitting fragments to run.");

    final PlanFragment rootPlanFragment = work.getRootFragment();
    assert queryId == rootPlanFragment.getHandle().getQueryId();

    queryManager.setup(rootPlanFragment.getHandle(), context.getCurrentEndpoint(), work.getFragments().size());

    // set up the root fragment first so we'll have incoming buffers available.
    setupRootFragment(rootPlanFragment, initiatingClient, work.getRootOperator());

    setupNonRootFragments(work.getFragments());
    bee.getContext().getAllocator().resetFragmentLimits();

    moveToState(QueryState.RUNNING, null);
    logger.debug("Fragments running.");

  }

  private void validatePlan(PhysicalPlan plan) throws ForemanSetupException{
    if (plan.getProperties().resultMode != ResultMode.EXEC) {
      throw new ForemanSetupException(String.format(
          "Failure running plan.  You requested a result mode of %s and a physical plan can only be output as EXEC",
          plan.getProperties().resultMode));
    }
  }

  private void setupSortMemoryAllocations(PhysicalPlan plan){
    int sortCount = 0;
    for (PhysicalOperator op : plan.getSortedOperators()) {
      if (op instanceof ExternalSort) {
        sortCount++;
      }
    }

    if (sortCount > 0) {
      long maxWidthPerNode = context.getOptions().getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = Math.min(DrillConfig.getMaxDirectMemory(),
          context.getConfig().getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC));
      maxAllocPerNode = Math.min(maxAllocPerNode,
          context.getOptions().getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).num_val);
      long maxSortAlloc = maxAllocPerNode / (sortCount * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);
      for (PhysicalOperator op : plan.getSortedOperators()) {
        if (op instanceof ExternalSort) {
          ((ExternalSort) op).setMaxAllocation(maxSortAlloc);
        }
      }
    }
  }

  private void acquireQuerySemaphore(PhysicalPlan plan) throws ForemanSetupException {

    double size = 0;
    for (PhysicalOperator ops : plan.getSortedOperators()) {
      size += ops.getCost();
    }

    if (queuingEnabled) {
      try {
        if (size > this.queueThreshold) {
          this.lease = largeSemaphore.acquire(this.queueTimeout, TimeUnit.MILLISECONDS);
        } else {
          this.lease = smallSemaphore.acquire(this.queueTimeout, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        throw new ForemanSetupException("Unable to acquire slot for query.", e);
      }
    }
  }

  private QueryWorkUnit getQueryWorkUnit(PhysicalPlan plan) throws ExecutionSetupException {
    PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
    MakeFragmentsVisitor makeFragmentsVisitor = new MakeFragmentsVisitor();
    Fragment rootFragment = rootOperator.accept(makeFragmentsVisitor, null);
    PlanningSet planningSet = StatsCollector.collectStats(rootFragment);
    SimpleParallelizer parallelizer = new SimpleParallelizer(context);


    return parallelizer.getFragments(context.getOptions().getOptionList(), context.getCurrentEndpoint(),
        queryId, context.getActiveEndpoints(), context.getPlanReader(), rootFragment, planningSet,
        initiatingClient.getSession());
  }

  /**
   * Tells the foreman to move to a new state.  Note that
   * @param state
   * @return
   */
  private synchronized boolean moveToState(QueryState newState, Exception exception){
    logger.info("State change requested.  {} --> {}", state, newState, exception);
    outside: switch(state) {

    case PENDING:
      // since we're moving out of pending, we can now start accepting other changes in state.
      // This guarantees that the first state change is driven by the original thread.
      acceptExternalEvents.countDown();

      if(newState == QueryState.RUNNING){
        recordNewState(QueryState.RUNNING);
        return true;
      }

      // fall through to running behavior.
      //
    case RUNNING: {

      switch(newState){

      case CANCELED: {
        assert exception == null;
        recordNewState(QueryState.CANCELED);
        cancelExecutingFragments();
        QueryResult result = QueryResult.newBuilder() //
            .setQueryId(queryId) //
            .setQueryState(QueryState.CANCELED) //
            .setIsLastChunk(true) //
            .build();

        // immediately notify client that cancellation is taking place, final clean-up happens when foreman reaches to
        // a terminal state(completed, failed)
        initiatingClient.sendResult(responseListener, new QueryWritableBatch(result), true);
        return true;
      }

      case COMPLETED: {
        assert exception == null;
        recordNewState(QueryState.COMPLETED);
        QueryResult result = QueryResult //
            .newBuilder() //
            .setQueryState(QueryState.COMPLETED) //
            .setQueryId(queryId) //
            .build();
        cleanup(result);
        return true;
      }


      case FAILED:
        assert exception != null;
        recordNewState(QueryState.FAILED);
        cancelExecutingFragments();
        DrillPBError error = ErrorHelper.logAndConvertError(context.getCurrentEndpoint(),
            ExceptionUtils.getRootCauseMessage(exception), exception, logger);
        QueryResult result = QueryResult //
          .newBuilder() //
          .addError(error) //
          .setIsLastChunk(true) //
          .setQueryState(QueryState.FAILED) //
          .setQueryId(queryId) //
          .build();
        cleanup(result);
        return true;
      default:
        break outside;

      }
    }

    case CANCELED:
    case COMPLETED:
    case FAILED: {
      // no op.
      logger.warn("Dropping request to move to {} state as query is already at {} state (which is terminal).", newState, state, exception);
      return false;
    }

    }

    throw new IllegalStateException(String.format("Failure trying to change states: %s --> %s", state.name(), newState.name()));
  }

  private void cancelExecutingFragments(){

    // Stop all framgents with a currently active status.
    List<FragmentData> fragments = getStatus().getFragmentData();
    Collections.sort(fragments, new Comparator<FragmentData>() {
      @Override
      public int compare(FragmentData o1, FragmentData o2) {
        return o2.getHandle().getMajorFragmentId() - o1.getHandle().getMajorFragmentId();
      }
    });
    for(FragmentData data: fragments){
      FragmentHandle handle = data.getStatus().getHandle();
      switch(data.getStatus().getProfile().getState()){
      case SENDING:
      case AWAITING_ALLOCATION:
      case RUNNING:
        if(data.isLocal()){
          if(rootRunner != null){
            rootRunner.cancel();
          }
        }else{
          bee.getContext().getController().getTunnel(data.getEndpoint()).cancelFragment(new CancelListener(data.getEndpoint(), handle), handle);
        }
        break;
      default:
        break;
      }
    }

  }

  private QueryStatus getStatus(){
    return queryManager.getStatus();
  }

  private void recordNewState(QueryState newState){
    this.state = newState;
    getStatus().updateQueryStateInStore(newState);
  }

  private void runSQL(String sql) throws ExecutionSetupException {
    DrillSqlWorker sqlWorker = new DrillSqlWorker(context);
    Pointer<String> textPlan = new Pointer<>();
    PhysicalPlan plan = sqlWorker.getPlan(sql, textPlan);
    getStatus().setPlanText(textPlan.value);
    runPhysicalPlan(plan);
  }

  private PhysicalPlan convert(LogicalPlan plan) throws OptimizerException {
    if (logger.isDebugEnabled()) {
      logger.debug("Converting logical plan {}.", plan.toJsonStringSafe(context.getConfig()));
    }
    return new BasicOptimizer(DrillConfig.create(), context, initiatingClient).optimize(
        new BasicOptimizer.BasicOptimizationContext(context), plan);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public void close() throws IOException {
  }

  public QueryStatus getQueryStatus() {
    return this.queryManager.getStatus();
  }

  private void setupRootFragment(PlanFragment rootFragment, UserClientConnection rootClient, FragmentRoot rootOperator) throws ExecutionSetupException {
    FragmentContext rootContext = new FragmentContext(bee.getContext(), rootFragment, rootClient, bee.getContext()
        .getFunctionImplementationRegistry());

    IncomingBuffers buffers = new IncomingBuffers(rootOperator, rootContext);

    rootContext.setBuffers(buffers);

    // add fragment to local node.
    queryManager.addFragmentStatusTracker(rootFragment, true);

    this.rootRunner = new FragmentExecutor(rootContext, bee, rootOperator, queryManager.getRootStatusHandler(rootContext, rootFragment));
    RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);

    if (buffers.isDone()) {
      // if we don't have to wait for any incoming data, start the fragment runner.
      bee.addFragmentRunner(fragmentManager.getRunnable());
    } else {
      // if we do, record the fragment manager in the workBus.
      bee.getContext().getWorkBus().setFragmentManager(fragmentManager);
    }
  }

  private void setupNonRootFragments(Collection<PlanFragment> fragments) throws ForemanException{
    Multimap<DrillbitEndpoint, PlanFragment> leafFragmentMap = ArrayListMultimap.create();
    Multimap<DrillbitEndpoint, PlanFragment> intFragmentMap = ArrayListMultimap.create();

    // record all fragments for status purposes.
    for (PlanFragment f : fragments) {
//      logger.debug("Tracking intermediate remote node {} with data {}", f.getAssignment(), f.getFragmentJson());
      queryManager.addFragmentStatusTracker(f, false);
      if (f.getLeafFragment()) {
        leafFragmentMap.put(f.getAssignment(), f);
      } else {
        intFragmentMap.put(f.getAssignment(), f);
      }
    }

    CountDownLatch latch = new CountDownLatch(intFragmentMap.keySet().size());

    // send remote intermediate fragments
    for (DrillbitEndpoint ep : intFragmentMap.keySet()) {
      sendRemoteFragments(ep, intFragmentMap.get(ep), latch);
    }

    // wait for send complete
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new ForemanException("Interrupted while waiting to complete send of remote fragments.", e);
    }

    // send remote (leaf) fragments.
    for (DrillbitEndpoint ep : leafFragmentMap.keySet()) {
      sendRemoteFragments(ep, leafFragmentMap.get(ep), null);
    }
  }

  public RpcOutcomeListener<Ack> getSubmitListener(DrillbitEndpoint endpoint, InitializeFragments value, CountDownLatch latch){
    return new FragmentSubmitListener(endpoint, value, latch);
  }

  private void sendRemoteFragments(DrillbitEndpoint assignment, Collection<PlanFragment> fragments, CountDownLatch latch){
    Controller controller = bee.getContext().getController();
    InitializeFragments.Builder fb = InitializeFragments.newBuilder();
    for(PlanFragment f : fragments){
      fb.addFragment(f);
    }
    InitializeFragments initFrags = fb.build();

    logger.debug("Sending remote fragments to node {} with data {}", assignment, initFrags);
    FragmentSubmitListener listener = new FragmentSubmitListener(assignment, initFrags, latch);
    controller.getTunnel(assignment).sendFragments(listener, initFrags);
  }

  public QueryState getState(){
    return state;
  }

  private class FragmentSubmitListener extends EndpointListener<Ack, InitializeFragments>{

    private CountDownLatch latch;

    public FragmentSubmitListener(DrillbitEndpoint endpoint, InitializeFragments value, CountDownLatch latch) {
      super(endpoint, value);
      this.latch = latch;
    }

    @Override
    public void success(Ack ack, ByteBuf byteBuf) {
      if (latch != null) {
        latch.countDown();
      }
    }

    @Override
    public void failed(RpcException ex) {
      logger.debug("Failure while sending fragment.  Stopping query.", ex);
      moveToState(QueryState.FAILED, ex);
    }

  }


  public class StateListener {
    public boolean moveToState(QueryState newState, Exception ex){
      try {
        acceptExternalEvents.await();
      } catch(InterruptedException e){
        logger.warn("Interrupted while waiting to move state.", e);
        return false;
      }

      return Foreman.this.moveToState(newState, ex);
    }
  }


  @Override
  public int compareTo(Object o) {
    return hashCode() - o.hashCode();
  }

  private class ResponseSendListener extends BaseRpcOutcomeListener<Ack> {
    @Override
    public void failed(RpcException ex) {
      logger
          .info(
              "Failure while trying communicate query result to initating client.  This would happen if a client is disconnected before response notice can be sent.",
              ex);
      moveToState(QueryState.FAILED, ex);
    }
  }


  private class CancelListener extends EndpointListener<Ack, FragmentHandle>{

    public CancelListener(DrillbitEndpoint endpoint, FragmentHandle handle) {
      super(endpoint, handle);
    }

    @Override
    public void failed(RpcException ex) {
      logger.error("Failure while attempting to cancel fragment {} on endpoint {}.", value, endpoint, ex);
    }

    @Override
    public void success(Ack value, ByteBuf buf) {
      if(!value.getOk()){
        logger.warn("Remote node {} responded negative on cancellation request for fragment {}.", endpoint, value);
      }
      // do nothing.
    }

  }
}
