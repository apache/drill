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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.exception.OptimizerException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.opt.BasicOptimizer;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.PlanningSet;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.fragment.StatsCollector;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RequestResults;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.util.AtomicState;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.WorkManager.WorkerBee;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * Foreman manages all queries where this is the driving/root node.
 */
public class Foreman implements Runnable, Closeable, Comparable<Object>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Foreman.class);

  private QueryId queryId;
  private RunQuery queryRequest;
  private QueryContext context;
  private RunningFragmentManager fragmentManager;
  private WorkerBee bee;
  private UserClientConnection initiatingClient;
  private final AtomicState<QueryState> state;
  
  
  public Foreman(WorkerBee bee, DrillbitContext dContext, UserClientConnection connection, QueryId queryId,
      RunQuery queryRequest) {
    this.queryId = queryId;
    this.queryRequest = queryRequest;
    this.context = new QueryContext(queryId, dContext);
    this.initiatingClient = connection;
    this.fragmentManager = new RunningFragmentManager(new ForemanManagerListener(), new TunnelManager(dContext.getBitCom()));
    this.bee = bee;
    
    this.state = new AtomicState<QueryState>(QueryState.PENDING) {
      protected QueryState getStateFromNumber(int i) {
        return QueryState.valueOf(i);
      }
    };
  }
  
  private boolean isFinished(){
    switch(state.getState()){
    case PENDING:
    case RUNNING:
      return false;
    default:
      return true;
    }
    
  }

  private void fail(String message, Throwable t) {
    if(isFinished()){
      logger.error("Received a failure message query finished of: {}", message, t);
    }
    DrillPBError error = ErrorHelper.logAndConvertError(context.getCurrentEndpoint(), message, t, logger);
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
    if(isFinished()){
      return;
    }
    
    // cancel remote fragments.
    fragmentManager.cancel();
    
    QueryResult result = QueryResult.newBuilder().setQueryState(QueryState.CANCELED).setIsLastChunk(true).setQueryId(queryId).build();
    cleanupAndSendResult(result);
  }
  
  void cleanupAndSendResult(QueryResult result){
    bee.retireForeman(this);
    initiatingClient.sendResult(new ResponseSendListener(), new QueryWritableBatch(result));
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
    // convert a run query request into action
    try{
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
    }catch(Exception ex){
      fail("Failure while setting up Foreman.", ex);
    }
  }

  private void parseAndRunLogicalPlan(String json) {
    
    try {
      LogicalPlan logicalPlan = context.getPlanReader().readLogicalPlan(json);
      if(logger.isDebugEnabled()) logger.debug("Logical {}", logicalPlan.unparse(context.getConfig()));
      PhysicalPlan physicalPlan = convert(logicalPlan);
      if(logger.isDebugEnabled()) logger.debug("Physical {}", context.getConfig().getMapper().writeValueAsString(physicalPlan));
      runPhysicalPlan(physicalPlan);
    } catch (IOException e) {
      fail("Failure while parsing logical plan.", e);
    } catch (OptimizerException e) {
      fail("Failure while converting logical plan to physical plan.", e);
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

    PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();
    MakeFragmentsVisitor makeFragmentsVisitor = new MakeFragmentsVisitor();
    Fragment rootFragment;
    try {
      rootFragment = rootOperator.accept(makeFragmentsVisitor, null);
    } catch (FragmentSetupException e) {
      fail("Failure while fragmenting query.", e);
      return;
    }
    
    

    
    PlanningSet planningSet = StatsCollector.collectStats(rootFragment);
    SimpleParallelizer parallelizer = new SimpleParallelizer();

    try {
      QueryWorkUnit work = parallelizer.getFragments(context.getCurrentEndpoint(), queryId, context.getActiveEndpoints(),
              context.getPlanReader(), rootFragment, planningSet, context.getConfig().getInt(ExecConstants.GLOBAL_MAX_WIDTH),
              context.getConfig().getInt(ExecConstants.MAX_WIDTH_PER_ENDPOINT));

      this.context.getBitCom().getListeners().addFragmentStatusListener(work.getRootFragment().getHandle(), fragmentManager);
      List<PlanFragment> leafFragments = Lists.newArrayList();
      List<PlanFragment> intermediateFragments = Lists.newArrayList();

      // store fragments in distributed grid.
      for (PlanFragment f : work.getFragments()) {
        if (f.getLeafFragment()) {
          leafFragments.add(f);
        } else {
          intermediateFragments.add(f);
          context.getCache().storeFragment(f);
        }
      }

      fragmentManager.runFragments(bee, work.getRootFragment(), work.getRootOperator(), initiatingClient, leafFragments, intermediateFragments);

    
    } catch (ExecutionSetupException | RpcException e) {
      fail("Failure while setting up query.", e);
    }

  }

  private void runSQL(String json) {
    throw new UnsupportedOperationException();
  }

  private PhysicalPlan convert(LogicalPlan plan) throws OptimizerException {
    return new BasicOptimizer(DrillConfig.create(), context).optimize(new BasicOptimizer.BasicOptimizationContext(), plan);
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
  
  QueryState getQueryState(){
    return this.state.getState();
  }

  
  class ForemanManagerListener{
    void fail(String message, Throwable t) {
      ForemanManagerListener.this.fail(message, t);
    }
    
    void cleanupAndSendResult(QueryResult result){
      Foreman.this.cleanupAndSendResult(result);
    }
    
  }



  @Override
  public int compareTo(Object o) {
    return o.hashCode() - o.hashCode();
  }
  
  

}
