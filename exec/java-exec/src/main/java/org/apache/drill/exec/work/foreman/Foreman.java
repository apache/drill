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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.cache.DistributedCache.SerializationMode;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.coord.DistributedSemaphore.DistributedLease;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.SendingAccountor;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RequestResults;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.AtomicState;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.ErrorHelper;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.WorkManager.WorkerBee;

import com.google.common.collect.Lists;

/**
 * Foreman manages all queries where this is the driving/root node.
 */
public class Foreman implements Runnable, Closeable, Comparable<Object>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);


  public static final CacheConfig<FragmentHandle, PlanFragment> FRAGMENT_CACHE = CacheConfig //
      .newBuilder(FragmentHandle.class, PlanFragment.class) //
      .mode(SerializationMode.PROTOBUF) //
      .build();

  private QueryId queryId;
  private RunQuery queryRequest;
  private QueryContext context;
  private QueryManager fragmentManager;
  private WorkerBee bee;
  private UserClientConnection initiatingClient;
  private final AtomicState<QueryState> state;
  private final DistributedSemaphore smallSemaphore;
  private final DistributedSemaphore largeSemaphore;
  private final long queueThreshold;
  private final long queueTimeout;
  private volatile DistributedLease lease;
  private final boolean queuingEnabled;

  public Foreman(WorkerBee bee, DrillbitContext dContext, UserClientConnection connection, QueryId queryId,
      RunQuery queryRequest) {
    this.queryId = queryId;
    this.queryRequest = queryRequest;
    this.context = new QueryContext(connection.getSession(), queryId, dContext);
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

    this.initiatingClient = connection;
    this.fragmentManager = new QueryManager(queryId, queryRequest, bee.getContext().getPersistentStoreProvider(), new ForemanManagerListener(), dContext.getController(), this);
    this.bee = bee;

    this.state = new AtomicState<QueryState>(QueryState.PENDING) {
      @Override
      protected QueryState getStateFromNumber(int i) {
        return QueryState.valueOf(i);
      }
    };
  }

  public QueryContext getContext() {
    return context;
  }

  private boolean isFinished() {
    switch(state.getState()) {
    case PENDING:
    case RUNNING:
      return false;
    default:
      return true;
    }

  }

  private void fail(String message, Throwable t) {
    if(isFinished()) {
      logger.error("Received a failure message query finished of: {}", message, t);
    }
    if (!state.updateState(QueryState.RUNNING, QueryState.FAILED)) {
      if (!state.updateState(QueryState.PENDING, QueryState.FAILED)) {
        logger.warn("Tried to update query state to FAILED, but was not RUNNING");
      }
    }

    boolean verbose = getContext().getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
    DrillPBError error = ErrorHelper.logAndConvertError(context.getCurrentEndpoint(), message, t, logger, verbose);
    QueryResult result = QueryResult //
        .newBuilder() //
        .addError(error) //
        .setIsLastChunk(true) //
        .setQueryState(QueryState.FAILED) //
        .setQueryId(queryId) //
        .build();
    cleanupAndSendResult(result);
  }

  public void cancel() {
    if (isFinished()) {
      return;
    }

    // cancel remote fragments.
    fragmentManager.cancel();

    QueryResult result = QueryResult.newBuilder().setQueryState(QueryState.CANCELED).setIsLastChunk(true).setQueryId(queryId).build();
    cleanupAndSendResult(result);
  }

  void cleanupAndSendResult(QueryResult result) {
    bee.retireForeman(this);
    initiatingClient.sendResult(new ResponseSendListener(), new QueryWritableBatch(result), true);
    state.updateState(QueryState.RUNNING, QueryState.COMPLETED);
  }

  private class ResponseSendListener extends BaseRpcOutcomeListener<Ack> {
    @Override
    public void failed(RpcException ex) {
      logger.info(
              "Failure while trying communicate query result to initating client.  This would happen if a client is disconnected before response notice can be sent.",
              ex);
    }
  }

  /**
   * Called by execution pool to do foreman setup. Actual query execution is a separate phase (and can be scheduled).
   */
  public void run() {

    final String originalThread = Thread.currentThread().getName();
    Thread.currentThread().setName(QueryIdHelper.getQueryId(queryId) + ":foreman");
    fragmentManager.getStatus().setStartTime(System.currentTimeMillis());
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
        throw new UnsupportedOperationException();
      }
    } catch (AssertionError | Exception ex) {
      fail("Failure while setting up Foreman.", ex);
    } catch (OutOfMemoryError e) {
      System.out.println("Out of memory, exiting.");
      System.out.flush();
      System.exit(-1);
    } finally {
      releaseLease();
      Thread.currentThread().setName(originalThread);
    }
  }

  private void releaseLease() {
    if (lease != null) {
      try {
        lease.close();
      } catch (Exception e) {
        logger.warn("Failure while releasing lease.", e);
      };
    }

  }
  private void parseAndRunLogicalPlan(String json) {

    try {
      LogicalPlan logicalPlan = context.getPlanReader().readLogicalPlan(json);

      if (logicalPlan.getProperties().resultMode == ResultMode.LOGICAL) {
        fail("Failure running plan.  You requested a result mode of LOGICAL and submitted a logical plan.  In this case you're output mode must be PHYSICAL or EXEC.", new Exception());
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Logical {}", logicalPlan.unparse(context.getConfig()));
      }
      PhysicalPlan physicalPlan = convert(logicalPlan);

      if (logicalPlan.getProperties().resultMode == ResultMode.PHYSICAL) {
        returnPhysical(physicalPlan);
        return;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Physical {}", context.getConfig().getMapper().writeValueAsString(physicalPlan));
      }
      runPhysicalPlan(physicalPlan);
    } catch (IOException e) {
      fail("Failure while parsing logical plan.", e);
    } catch (OptimizerException e) {
      fail("Failure while converting logical plan to physical plan.", e);
    }
  }

  private void returnPhysical(PhysicalPlan plan) {
    String jsonPlan = plan.unparse(context.getConfig().getMapper().writer());
    runPhysicalPlan(DirectPlan.createDirectPlan(context, new PhysicalFromLogicalExplain(jsonPlan)));
  }

  private class PhysicalFromLogicalExplain{
    public String json;

    public PhysicalFromLogicalExplain(String json) {
      super();
      this.json = json;
    }

  }

  class SingleListener implements RpcOutcomeListener<Ack>{

    final SendingAccountor acct;

    public SingleListener() {
      acct  = new SendingAccountor();
      acct.increment();
      acct.increment();
    }

    @Override
    public void failed(RpcException ex) {
      acct.decrement();
      fail("Failure while sending single result.", ex);
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      acct.decrement();
    }

  }

  private void parseAndRunPhysicalPlan(String json) {
    try {
      PhysicalPlan plan = context.getPlanReader().readPhysicalPlan(json);
      runPhysicalPlan(plan);
    } catch (IOException e) {
      fail("Failure while parsing physical plan.", e);
    }
  }

  private void runPhysicalPlan(PhysicalPlan plan) {

    if(plan.getProperties().resultMode != ResultMode.EXEC) {
      fail(String.format("Failure running plan.  You requested a result mode of %s and a physical plan can only be output as EXEC", plan.getProperties().resultMode), new Exception());
    }
    PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();

    MakeFragmentsVisitor makeFragmentsVisitor = new MakeFragmentsVisitor();
    Fragment rootFragment;
    try {
      rootFragment = rootOperator.accept(makeFragmentsVisitor, null);
    } catch (FragmentSetupException e) {
      fail("Failure while fragmenting query.", e);
      return;
    }

    int sortCount = 0;
    for (PhysicalOperator op : plan.getSortedOperators()) {
      if (op instanceof ExternalSort) {
        sortCount++;
      }
    }

    if (sortCount > 0) {
      long maxWidthPerNode = context.getOptions().getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = Math.min(DrillConfig.getMaxDirectMemory(), context.getConfig().getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC));
      maxAllocPerNode = Math.min(maxAllocPerNode, context.getOptions().getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).num_val);
      long maxSortAlloc = maxAllocPerNode / (sortCount * maxWidthPerNode);
      logger.debug("Max sort alloc: {}", maxSortAlloc);
      for (PhysicalOperator op : plan.getSortedOperators()) {
        if (op instanceof  ExternalSort) {
          ((ExternalSort)op).setMaxAllocation(maxSortAlloc);
        }
      }
    }

    PlanningSet planningSet = StatsCollector.collectStats(rootFragment);
    SimpleParallelizer parallelizer = new SimpleParallelizer(context);

    try {

      double size = 0;
      for (PhysicalOperator ops : plan.getSortedOperators()) {
        size += ops.getCost();
      }
      if (queuingEnabled) {
        if (size > this.queueThreshold) {
          this.lease = largeSemaphore.acquire(this.queueTimeout, TimeUnit.MILLISECONDS);
        } else {
          this.lease = smallSemaphore.acquire(this.queueTimeout, TimeUnit.MILLISECONDS);
        }
      }

      QueryWorkUnit work = parallelizer.getFragments(context.getOptions().getOptionList(), context.getCurrentEndpoint(),
          queryId, context.getActiveEndpoints(), context.getPlanReader(), rootFragment, planningSet, initiatingClient.getSession());

      this.context.getWorkBus().setFragmentStatusListener(work.getRootFragment().getHandle().getQueryId(), fragmentManager);
      List<PlanFragment> leafFragments = Lists.newArrayList();
      List<PlanFragment> intermediateFragments = Lists.newArrayList();

      // store fragments in distributed grid.
      logger.debug("Storing fragments");
      List<Future<PlanFragment>> queue = new LinkedList<>();
      for (PlanFragment f : work.getFragments()) {
        // store all fragments in grid since they are part of handshake.

        queue.add(context.getCache().getMap(FRAGMENT_CACHE).put(f.getHandle(), f));
        if (f.getLeafFragment()) {
          leafFragments.add(f);
        } else {
          intermediateFragments.add(f);
        }
      }

      for (Future<PlanFragment> f : queue) {
        try {
          f.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          throw new ExecutionSetupException("failure while storing plan fragments", e);
        }
      }

      int totalFragments = 1 + intermediateFragments.size() + leafFragments.size();
      fragmentManager.getStatus().setTotalFragments(totalFragments);
      fragmentManager.getStatus().updateCache();
      logger.debug("Fragments stored.");

      logger.debug("Submitting fragments to run.");
      fragmentManager.runFragments(bee, work.getRootFragment(), work.getRootOperator(), initiatingClient, leafFragments, intermediateFragments);

      logger.debug("Fragments running.");
      state.updateState(QueryState.PENDING, QueryState.RUNNING);

    } catch (Exception e) {
      fail("Failure while setting up query.", e);
    }

  }

  private void runSQL(String sql) {
    try{
      DrillSqlWorker sqlWorker = new DrillSqlWorker(context);
      Pointer<String> textPlan = new Pointer<>();
      PhysicalPlan plan = sqlWorker.getPlan(sql, textPlan);
      fragmentManager.getStatus().setPlanText(textPlan.value);
      runPhysicalPlan(plan);
    } catch(Exception e) {
      fail("Failure while parsing sql.", e);
    }
  }

  private PhysicalPlan convert(LogicalPlan plan) throws OptimizerException {
    if (logger.isDebugEnabled()) {
      logger.debug("Converting logical plan {}.", plan.toJsonStringSafe(context.getConfig()));
    }
    return new BasicOptimizer(DrillConfig.create(), context, initiatingClient).optimize(new BasicOptimizer.BasicOptimizationContext(context), plan);
  }

  public QueryResult getResult(UserClientConnection connection, RequestResults req) {
    throw new UnsupportedOperationException();
  }


  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public void close() throws IOException {
  }

  public QueryState getQueryState() {
    return this.state.getState();
  }

  public QueryStatus getQueryStatus() {
    return this.fragmentManager.getStatus();
  }


  class ForemanManagerListener{
    void fail(String message, Throwable t) {
      ForemanManagerListener.this.fail(message, t);
    }

    void cleanupAndSendResult(QueryResult result) {
      Foreman.this.cleanupAndSendResult(result);
    }

  }

  @Override
  public int compareTo(Object o) {
    return o.hashCode() - o.hashCode();
  }

}
