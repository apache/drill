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
package org.apache.drill.exec.work;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.FragmentState;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;

import com.yammer.metrics.Timer;

/**
 * Responsible for running a single fragment on a single Drillbit. Listens/responds to status request and cancellation
 * messages. Two child implementation, root (driving) and child (driven) exist. 
 */
public class FragmentRunner implements Runnable, CancelableQuery, StatusProvider, Comparable<Object>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentRunner.class);

  private final AtomicInteger state = new AtomicInteger(FragmentState.AWAITING_ALLOCATION_VALUE);
  private final RootExec root;
  private final FragmentContext context;
  private final FragmentRunnerListener listener;
  
  public FragmentRunner(FragmentContext context, RootExec root, FragmentRunnerListener listener){
    this.context = context;
    this.root = root;
    this.listener = listener;
  }

  @Override
  public FragmentStatus getStatus() {
    return FragmentStatus.newBuilder() //
        .setBatchesCompleted(context.batchesCompleted.get()) //
        .setDataProcessed(context.dataProcessed.get()) //
        .setMemoryUse(context.getAllocator().getAllocatedMemory()) //
        .build();
  }

  @Override
  public void cancel() {
    updateState(FragmentState.CANCELLED);
  }

  public UserClientConnection getClient(){
    return context.getConnection();
  }
  
  @Override
  public void run() {
    logger.debug("Starting fragment runner. {}:{}", context.getHandle().getMajorFragmentId(), context.getHandle().getMinorFragmentId());
    if(!updateState(FragmentState.AWAITING_ALLOCATION, FragmentState.RUNNING, false)){
      internalFail(new RuntimeException(String.format("Run was called when fragment was in %s state.  FragmentRunnables should only be started when they are currently in awaiting allocation state.", FragmentState.valueOf(state.get()))));
      return;
    }
    
    Timer.Context t = context.fragmentTime.time();
    
    // run the query until root.next returns false.
    try{
      while(state.get() == FragmentState.RUNNING_VALUE){
        if(!root.next()){
          if(context.isFailed()){
            updateState(FragmentState.RUNNING, FragmentState.FAILED, false);  
          }else{
            updateState(FragmentState.RUNNING, FragmentState.FINISHED, false);
          }

        }
      }
      
      // If this isn't a finished stop, we'll inform other batches to finish up.
      if(state.get() != FragmentState.FINISHED_VALUE){
        root.stop();
      }
      
    }catch(Exception ex){
      logger.debug("Caught exception while running fragment: {} ", ex);
      internalFail(ex);
    }finally{
      t.stop();
    }
    logger.debug("Fragment runner complete. {}:{}", context.getHandle().getMajorFragmentId(), context.getHandle().getMinorFragmentId());
  }
  
  private void internalFail(Throwable excep){
    state.set(FragmentState.FAILED_VALUE);
    listener.fail(context.getHandle(), "Failure while running fragment.", excep);
  }
  
  private void updateState(FragmentState update){
    state.set(update.getNumber());
    listener.stateChanged(context.getHandle(), update);
  }
  
  private boolean updateState(FragmentState current, FragmentState update, boolean exceptionOnFailure) {
    boolean success = state.compareAndSet(current.getNumber(), update.getNumber());
    if (!success && exceptionOnFailure) {
      internalFail(new RuntimeException(String.format(
          "State was different than expected.  Attempting to update state from %s to %s however current state was %s.",
          current.name(), update.name(), FragmentState.valueOf(state.get()))));
      return false;
    }
    listener.stateChanged(context.getHandle(), update);
    return true;
  }

  @Override
  public int compareTo(Object o) {
    return o.hashCode() - this.hashCode();
  }

  
}
