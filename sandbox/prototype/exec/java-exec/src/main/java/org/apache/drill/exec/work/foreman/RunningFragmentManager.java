/*******************************************************************************
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
 ******************************************************************************/
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
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.FragmentState;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserProtos.QueryResult;
import org.apache.drill.exec.proto.UserProtos.QueryResult.QueryState;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.work.AbstractFragmentRunnerListener;
import org.apache.drill.exec.work.EndpointListener;
import org.apache.drill.exec.work.FragmentRunner;
import org.apache.drill.exec.work.WorkManager.WorkerBee;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.foreman.Foreman.ForemanManagerListener;
import org.apache.drill.exec.work.fragment.LocalFragmentHandler;

import com.google.common.collect.Maps;

/**
 * Each Foreman holds its own fragment manager.  This manages the events associated with execution of a particular query across all fragments.  
 */
class RunningFragmentManager implements FragmentStatusListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RunningFragmentManager.class);
  
  public Map<FragmentHandle, FragmentData> map = Maps.newHashMap(); // doesn't need to be
  private final TunnelManager tun;
  private ForemanManagerListener foreman;
  private AtomicInteger remainingFragmentCount;
  private FragmentRunner rootRunner;
  private volatile QueryId queryId;
  
  public RunningFragmentManager(ForemanManagerListener foreman, TunnelManager tun) {
    super();
    this.foreman = foreman;
    this.tun = tun;
    this.remainingFragmentCount = new AtomicInteger(0);
    
  }

  public void runFragments(WorkerBee bee, PlanFragment rootFragment, FragmentRoot rootOperator, UserClientConnection rootClient, List<PlanFragment> leafFragments, List<PlanFragment> intermediateFragments) throws ExecutionSetupException{
    remainingFragmentCount.set(leafFragments.size()+1);
    queryId = rootFragment.getHandle().getQueryId();

    // set up the root fragment first so we'll have incoming buffers available.
    {
      IncomingBuffers buffers = new IncomingBuffers(rootOperator);
      
      FragmentContext rootContext = new FragmentContext(bee.getContext(), rootFragment.getHandle(), rootClient, buffers, new FunctionImplementationRegistry(bee.getContext().getConfig()));
      RootExec rootExec = ImplCreator.getExec(rootContext, rootOperator);
      // add fragment to local node.
      map.put(rootFragment.getHandle(), new FragmentData(rootFragment.getHandle(), null, true));
      rootRunner = new FragmentRunner(rootContext, rootExec, new RootFragmentManager(rootContext, rootFragment));
      LocalFragmentHandler handler = new LocalFragmentHandler(rootFragment.getHandle(), buffers, rootRunner);
      if(buffers.isDone()){
        bee.addFragmentRunner(handler.getRunnable());
      }else{
        bee.getContext().getBitCom().registerIncomingBatchHandler(handler);
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
    
  }
    
  private void sendRemoteFragment(PlanFragment fragment){
    logger.debug("Sending remote fragment to node {} with data {}", fragment.getAssignment(), fragment.getFragmentJson());
    map.put(fragment.getHandle(), new FragmentData(fragment.getHandle(), fragment.getAssignment(), false));
    FragmentSubmitListener listener = new FragmentSubmitListener(fragment.getAssignment(), fragment);
    tun.get(fragment.getAssignment()).sendFragment(listener, fragment);
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
      QueryResult result = QueryResult.newBuilder().setQueryState(QueryState.COMPLETED).build();
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

  private class RootFragmentManager extends AbstractFragmentRunnerListener{

    private RootFragmentManager(FragmentContext context, PlanFragment fragment){
      super(context);
    }

    @Override
    protected void statusChange(FragmentHandle handle, FragmentStatus status) {
      RunningFragmentManager.this.statusUpdate(status);
    }

    
  }

}
