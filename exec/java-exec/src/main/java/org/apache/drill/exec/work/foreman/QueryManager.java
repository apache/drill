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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.FragmentStatus.FragmentState;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.QueryResult.QueryState;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.foreman.Foreman.ForemanManagerListener;
import org.apache.drill.exec.work.fragment.AbstractStatusReporter;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.RootFragmentManager;

import com.google.common.collect.Maps;

/**
 * Each Foreman holds its own fragment manager.  This manages the events associated with execution of a particular query across all fragments.  
 */
public class QueryManager implements FragmentStatusListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryManager.class);
  
  public Map<FragmentHandle, FragmentData> map = Maps.newHashMap(); // doesn't need to be thread safe as map is generated in a single thread and then accessed by multiple threads for reads only.
  private final Controller controller;
  private ForemanManagerListener foreman;
  private AtomicInteger remainingFragmentCount;
  private WorkEventBus workBus;
  private FragmentExecutor rootRunner;
  private volatile QueryId queryId;
  
  public QueryManager(ForemanManagerListener foreman, Controller controller) {
    super();
    this.foreman = foreman;
    this.controller = controller;
    this.remainingFragmentCount = new AtomicInteger(0);
    
  }

  public void runFragments(WorkerBee bee, PlanFragment rootFragment, FragmentRoot rootOperator, UserClientConnection rootClient, List<PlanFragment> leafFragments, List<PlanFragment> intermediateFragments) throws ExecutionSetupException{
    logger.debug("Setting up fragment runs.");
    remainingFragmentCount.set(leafFragments.size()+1);
    queryId = rootFragment.getHandle().getQueryId();
    workBus = bee.getContext().getWorkBus();

    // set up the root fragment first so we'll have incoming buffers available.
    {
      logger.debug("Setting up root context.");
      FragmentContext rootContext = new FragmentContext(bee.getContext(), rootFragment, rootClient, bee.getContext().getFunctionImplementationRegistry());
      logger.debug("Setting up incoming buffers");
      IncomingBuffers buffers = new IncomingBuffers(rootOperator, rootContext);
      logger.debug("Setting buffers on root context.");
      rootContext.setBuffers(buffers);
      logger.debug("Generating Exec tree");
      RootExec rootExec = ImplCreator.getExec(rootContext, rootOperator);
      logger.debug("Exec tree generated.");
      // add fragment to local node.
      map.put(rootFragment.getHandle(), new FragmentData(rootFragment.getHandle(), null, true));
      logger.debug("Fragment added to local node.");
      rootRunner = new FragmentExecutor(rootContext, rootExec, new RootStatusHandler(rootContext, rootFragment));
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
      map.put(f.getHandle(), new FragmentData(f.getHandle(), f.getAssignment(), false));
    }

    // send remote (leaf) fragments.
    for (PlanFragment f : leafFragments) {
      sendRemoteFragment(f);
    }
    
    logger.debug("Fragment runs setup is complete.");
  }
    
  private void sendRemoteFragment(PlanFragment fragment){
    logger.debug("Sending remote fragment to node {} with data {}", fragment.getAssignment(), fragment.getFragmentJson());
    map.put(fragment.getHandle(), new FragmentData(fragment.getHandle(), fragment.getAssignment(), false));
    FragmentSubmitListener listener = new FragmentSubmitListener(fragment.getAssignment(), fragment);
    controller.getTunnel(fragment.getAssignment()).sendFragment(listener, fragment);
  }
  
  
  @Override
  public void statusUpdate(FragmentStatus status) {
    logger.debug("New fragment status was provided to Foreman of {}", status);
    switch(status.getState()){
    case AWAITING_ALLOCATION:
      updateStatus(status);
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
      updateStatus(status);
      break;
    default:
      throw new UnsupportedOperationException();
    }
  }
  
  private void updateStatus(FragmentStatus status){
    map.get(status.getHandle()).setStatus(status);
  }
  
  private void finished(FragmentStatus status){
    updateStatus(status);
    int remaining = remainingFragmentCount.decrementAndGet();
    if(remaining == 0){
      QueryResult result = QueryResult.newBuilder() //
              .setQueryState(QueryState.COMPLETED) //
              .setQueryId(queryId) //
              .build();
      foreman.cleanupAndSendResult(result);
    }
  }
  
  private void fail(FragmentStatus status){
    updateStatus(status);
    stopQuery();
    QueryResult result = QueryResult.newBuilder().setQueryId(queryId).setQueryState(QueryState.FAILED).addError(status.getError()).build();
    foreman.cleanupAndSendResult(result);
  }
 
  
  private void stopQuery(){
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
  
  
  private class FragmentData{
    private final boolean isLocal;
    private volatile FragmentStatus status;
    private volatile long lastStatusUpdate = 0;
    private final DrillbitEndpoint endpoint;
    
    public FragmentData(FragmentHandle handle, DrillbitEndpoint endpoint, boolean isLocal) {
      super();
      this.status = FragmentStatus.newBuilder().setHandle(handle).setState(FragmentState.SENDING).build();
      this.endpoint = endpoint;
      this.isLocal = isLocal;
    }
    
    public void setStatus(FragmentStatus status){
      this.status = status;
      lastStatusUpdate = System.currentTimeMillis();
    }

    public FragmentStatus getStatus() {
      return status;
    }

    public boolean isLocal() {
      return isLocal;
    }

    public long getLastStatusUpdate() {
      return lastStatusUpdate;
    }

    public DrillbitEndpoint getEndpoint() {
      return endpoint;
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
