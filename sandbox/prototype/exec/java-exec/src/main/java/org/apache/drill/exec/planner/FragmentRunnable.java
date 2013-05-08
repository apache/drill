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
package org.apache.drill.exec.planner;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.foreman.CancelableQuery;
import org.apache.drill.exec.foreman.StatusProvider;
import org.apache.drill.exec.metrics.SingleThreadNestedCounter;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentConverter;
import org.apache.drill.exec.ops.FragmentRoot;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.FragmentState;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.server.DrillbitContext;

import com.yammer.metrics.MetricRegistry;
import com.yammer.metrics.Timer;

/**
 * Responsible for running a single fragment on a single Drillbit. Listens/responds to status request and cancellation
 * messages.
 */
public class FragmentRunnable implements Runnable, CancelableQuery, StatusProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentRunnable.class);

  private final AtomicInteger state = new AtomicInteger(FragmentState.AWAITING_ALLOCATION_VALUE);
  private final FragmentRoot root;
  private final FragmentContext context;

  public FragmentRunnable(DrillbitContext dbContext, long fragmentId) throws FragmentSetupException {
    PlanFragment fragment = dbContext.getCache().getFragment(fragmentId);
    if (fragment == null) throw new FragmentSetupException(String.format("The provided fragment id [%d] was unknown.", fragmentId));
    this.context = new FragmentContext(dbContext, fragment);
    this.root = FragmentConverter.getFragment(this.context);
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
  public boolean cancel(long queryId) {
    if (context.getFragment().getQueryId() == queryId) {
      state.set(FragmentState.CANCELLED_VALUE);
      return true;
    }
    return false;
  }

  private void fail(Throwable cause){
    context.fail(cause);
    state.set(FragmentState.FAILED_VALUE);
  }
  
  @Override
  public void run() {
    if(!updateState(FragmentState.AWAITING_ALLOCATION, FragmentState.RUNNING, false)){
      fail(new RuntimeException(String.format("Run was called when fragment was in %s state.  FragmentRunnables should only be started when they are currently in awaiting allocation state.", FragmentState.valueOf(state.get()))));
      return;
    }
    
    Timer.Context t = context.fragmentTime.time();
    
    // setup the query.
    try{
      root.setup();
    }catch(FragmentSetupException e){
      
      context.fail(e);
      return;
    }
    
    // run the query.
    try{
      while(state.get() == FragmentState.RUNNING_VALUE){
        if(!root.next()){
          updateState(FragmentState.RUNNING, FragmentState.FINISHED, false);
        }
      }
      t.stop();
    }catch(Exception ex){
      fail(ex);
    }
    
  }

  private boolean updateState(FragmentState current, FragmentState update, boolean exceptionOnFailure) {
    boolean success = state.compareAndSet(current.getNumber(), update.getNumber());
    if (!success && exceptionOnFailure) {
      context.fail(new RuntimeException(String.format(
          "State was different than expected.  Attempting to update state from %s to %s however current state was %s.",
          current.name(), update.name(), FragmentState.valueOf(state.get()))));
      return false;
    }

    return true;
  }

}
