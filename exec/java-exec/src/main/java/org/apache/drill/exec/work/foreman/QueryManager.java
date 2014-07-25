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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.foreman.Foreman.ForemanManagerListener;
import org.apache.drill.exec.work.fragment.AbstractStatusReporter;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.RootFragmentManager;

/**
 * Each Foreman holds its own fragment manager.  This manages the events associated with execution of a particular query across all fragments.
 */
public class QueryManager implements FragmentStatusListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);

  private final QueryStatus status;
  private final Controller controller;
  private ForemanManagerListener foremanManagerListener;
  private AtomicInteger remainingFragmentCount;
  private WorkEventBus workBus;
  private QueryId queryId;
  private FragmentExecutor rootRunner;
  private RunQuery query;

  public QueryManager(QueryId id, RunQuery query, PStoreProvider pStoreProvider, ForemanManagerListener foremanManagerListener, Controller controller, Foreman foreman) {
    super();
    this.foremanManagerListener = foremanManagerListener;
    this.query = query;
    this.queryId =  id;
    this.controller = controller;
    this.remainingFragmentCount = new AtomicInteger(0);
    this.status = new QueryStatus(query, id, pStoreProvider, foreman);
  }

  public QueryStatus getStatus(){
    return status;
  }

  public void addTextPlan(String textPlan){

  }

  public void runFragments(WorkerBee bee, PlanFragment rootFragment, FragmentRoot rootOperator,
                           UserClientConnection rootClient, List<PlanFragment> leafFragments,
                           List<PlanFragment> intermediateFragments) throws ExecutionSetupException{
    logger.debug("Setting up fragment runs.");
    remainingFragmentCount.set(intermediateFragments.size() + leafFragments.size() + 1);
    assert queryId == rootFragment.getHandle().getQueryId();
    workBus = bee.getContext().getWorkBus();

    // set up the root fragment first so we'll have incoming buffers available.
    {
      logger.debug("Setting up root context.");
      FragmentContext rootContext = new FragmentContext(bee.getContext(), rootFragment, rootClient, bee.getContext().getFunctionImplementationRegistry());
      logger.debug("Setting up incoming buffers");
      IncomingBuffers buffers = new IncomingBuffers(rootOperator, rootContext);
      logger.debug("Setting buffers on root context.");
      rootContext.setBuffers(buffers);
      // add fragment to local node.
      status.add(new FragmentData(rootFragment.getHandle(), null, true));
      logger.debug("Fragment added to local node.");
      rootRunner = new FragmentExecutor(rootContext, rootOperator, new RootStatusHandler(rootContext, rootFragment));
      RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);

      if(buffers.isDone()){
        // if we don't have to wait for any incoming data, start the fragment runner.
        bee.addFragmentRunner(fragmentManager.getRunnable());
      }else{
        // if we do, record the fragment manager in the workBus.
        workBus.setRootFragmentManager(fragmentManager);
      }


    }

    // keep track of intermediate fragments (not root or leaf)
    for (PlanFragment f : intermediateFragments) {
      logger.debug("Tracking intermediate remote node {} with data {}", f.getAssignment(), f.getFragmentJson());
      status.add(new FragmentData(f.getHandle(), f.getAssignment(), false));
    }

    // send remote (leaf) fragments.
    for (PlanFragment f : leafFragments) {
      sendRemoteFragment(f);
    }

    logger.debug("Fragment runs setup is complete.");
  }

  private void sendRemoteFragment(PlanFragment fragment){
    logger.debug("Sending remote fragment to node {} with data {}", fragment.getAssignment(), fragment.getFragmentJson());
    status.add(new FragmentData(fragment.getHandle(), fragment.getAssignment(), false));
    FragmentSubmitListener listener = new FragmentSubmitListener(fragment.getAssignment(), fragment);
    controller.getTunnel(fragment.getAssignment()).sendFragment(listener, fragment);
  }


  @Override
  public void statusUpdate(FragmentStatus status) {
    logger.debug("New fragment status was provided to Foreman of {}", status);
    switch(status.getProfile().getState()){
    case AWAITING_ALLOCATION:
      updateStatus(status, true);
      break;
    case CANCELLED:
      // we don't care about cancellation messages since we're the only entity that should drive cancellations.
      break;
    case FAILED:
      fail(status);
      break;
    case FINISHED:
      finished(status);
      break;
    case RUNNING:
      updateStatus(status, false);
      break;
    default:
      throw new UnsupportedOperationException(String.format("Received status of %s", status));
    }
  }

  private void updateStatus(FragmentStatus status, boolean updateCache){
    this.status.update(status, updateCache);
  }

  private void finished(FragmentStatus status){
    int remaining = remainingFragmentCount.decrementAndGet();
    if(remaining == 0){
      logger.info("Outcome status: {}", this.status);
      QueryResult result = QueryResult.newBuilder() //
              .setQueryState(QueryState.COMPLETED) //
              .setQueryId(queryId) //
              .build();
      foremanManagerListener.cleanupAndSendResult(result);
      workBus.removeFragmentStatusListener(queryId);
    }
    this.status.setEndTime(System.currentTimeMillis());
    this.status.incrementFinishedFragments();
    updateStatus(status, true);
  }

  private void fail(FragmentStatus status){
    stopQuery();
    QueryResult result = QueryResult.newBuilder().setQueryId(queryId).setQueryState(QueryState.FAILED).addError(status.getProfile().getError()).build();
    foremanManagerListener.cleanupAndSendResult(result);
    this.status.setEndTime(System.currentTimeMillis());
    updateStatus(status, true);
  }


  private void stopQuery(){
    workBus.removeFragmentStatusListener(queryId);
    // Stop all queries with a currently active status.
//    for(FragmentData data: map.values()){
//      FragmentHandle handle = data.getStatus().getHandle();
//      switch(data.getStatus().getState()){
//      case SENDING:
//      case AWAITING_ALLOCATION:
//      case RUNNING:
//        if(data.isLocal()){
//          rootRunner.cancel();
//        }else{
//          tun.get(data.getEndpoint()).cancelFragment(handle).addLightListener(new CancelListener(data.endpoint, handle));
//        }
//        break;
//      default:
//        break;
//      }
//    }
  }

  public void cancel(){
    stopQuery();
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

  };

  public RpcOutcomeListener<Ack> getSubmitListener(DrillbitEndpoint endpoint, PlanFragment value){
    return new FragmentSubmitListener(endpoint, value);
  }

  private class FragmentSubmitListener extends EndpointListener<Ack, PlanFragment>{

    public FragmentSubmitListener(DrillbitEndpoint endpoint, PlanFragment value) {
      super(endpoint, value);
    }

    @Override
    public void failed(RpcException ex) {
      logger.debug("Failure while sending fragment.  Stopping query.", ex);
      stopQuery();
    }

  }

  private class RootStatusHandler extends AbstractStatusReporter{

    private RootStatusHandler(FragmentContext context, PlanFragment fragment){
      super(context);
    }

    @Override
    protected void statusChange(FragmentHandle handle, FragmentStatus status) {
      QueryManager.this.statusUpdate(status);
    }


  }

}
