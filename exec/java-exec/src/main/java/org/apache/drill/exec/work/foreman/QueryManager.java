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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

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
  private volatile boolean running = false;
  private volatile boolean cancelled = false;
  private volatile boolean stopped = false;

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
                           UserClientConnection rootClient, List<PlanFragment> nonRootFragments) throws ExecutionSetupException{
    logger.debug("Setting up fragment runs.");
    remainingFragmentCount.set(nonRootFragments.size() + 1);
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
      status.add(new FragmentData(rootFragment.getHandle(), rootFragment.getAssignment(), true));
      logger.debug("Fragment added to local node.");
      rootRunner = new FragmentExecutor(rootContext, bee, rootOperator, new RootStatusHandler(rootContext, rootFragment));
      RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);

      if(buffers.isDone()){
        // if we don't have to wait for any incoming data, start the fragment runner.
        bee.addFragmentRunner(fragmentManager.getRunnable());
      }else{
        // if we do, record the fragment manager in the workBus.
        workBus.setFragmentManager(fragmentManager);
      }
    }

    Multimap<DrillbitEndpoint, PlanFragment> fragmentMap = ArrayListMultimap.create();

    // record all fragments for status purposes.
    for (PlanFragment f : nonRootFragments) {
      logger.debug("Tracking intermediate remote node {} with data {}", f.getAssignment(), f.getFragmentJson());
      status.add(new FragmentData(f.getHandle(), f.getAssignment(), false));
      fragmentMap.put(f.getAssignment(), f);
    }



    // send remote (leaf) fragments.
    for (DrillbitEndpoint ep : fragmentMap.keySet()) {
      sendRemoteFragments(ep, fragmentMap.get(ep));
    }

    bee.getContext().getAllocator().resetFragmentLimits();

    logger.debug("Fragment runs setup is complete.");
    running = true;
    if (cancelled && !stopped) {
      stopQuery();
    }
  }

  private void sendRemoteFragments(DrillbitEndpoint assignment, Collection<PlanFragment> fragments){
    InitializeFragments.Builder fb = InitializeFragments.newBuilder();
    for(PlanFragment f : fragments){
      fb.addFragment(f);
    }
    InitializeFragments initFrags = fb.build();

    logger.debug("Sending remote fragments to node {} with data {}", assignment, initFrags);
    FragmentSubmitListener listener = new FragmentSubmitListener(assignment, initFrags);
    controller.getTunnel(assignment).sendFragments(listener, initFrags);
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
    List<FragmentData> fragments = status.getFragmentData();
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
          rootRunner.cancel();
        }else{
          controller.getTunnel(data.getEndpoint()).cancelFragment(new CancelListener(data.getEndpoint(), handle), handle);
        }
        break;
      default:
        break;
      }
    }
  }

  public void cancel(){
    cancelled = true;
    if (running) {
      stopQuery();
      stopped = true;
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

  public RpcOutcomeListener<Ack> getSubmitListener(DrillbitEndpoint endpoint, InitializeFragments value){
    return new FragmentSubmitListener(endpoint, value);
  }

  private class FragmentSubmitListener extends EndpointListener<Ack, InitializeFragments>{

    public FragmentSubmitListener(DrillbitEndpoint endpoint, InitializeFragments value) {
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
