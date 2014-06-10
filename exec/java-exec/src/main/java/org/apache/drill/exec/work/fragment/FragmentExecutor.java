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
package org.apache.drill.exec.work.fragment;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.work.CancelableQuery;
import org.apache.drill.exec.work.StatusProvider;

import com.codahale.metrics.Timer;

/**
 * Responsible for running a single fragment on a single Drillbit. Listens/responds to status request and cancellation
 * messages.
 */
public class FragmentExecutor implements Runnable, CancelableQuery, StatusProvider, Comparable<Object>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutor.class);

  private final AtomicInteger state = new AtomicInteger(FragmentState.AWAITING_ALLOCATION_VALUE);
  private final FragmentRoot rootOperator;
  private RootExec root;
  private final FragmentContext context;
  private final StatusReporter listener;

  public FragmentExecutor(FragmentContext context, FragmentRoot rootOperator, StatusReporter listener){
    this.context = context;
    this.rootOperator = rootOperator;
    this.listener = listener;
  }

  @Override
  public FragmentStatus getStatus() {
    throw new UnsupportedOperationException();
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
    final String originalThread = Thread.currentThread().getName();
    String newThreadName = String.format("%s:frag:%s:%s", //
        QueryIdHelper.getQueryId(context.getHandle().getQueryId()), //
        context.getHandle().getMajorFragmentId(),
        context.getHandle().getMinorFragmentId()
        );
    Thread.currentThread().setName(newThreadName);

    boolean closed = false;
    try {
      root = ImplCreator.getExec(context, rootOperator);
    } catch (ExecutionSetupException e) {
      context.fail(e);
      logger.debug("Failure while running fragement", e);
      internalFail(e);
      return;
    }

    logger.debug("Starting fragment runner. {}:{}", context.getHandle().getMajorFragmentId(), context.getHandle().getMinorFragmentId());
    if(!updateState(FragmentState.AWAITING_ALLOCATION, FragmentState.RUNNING, false)){
      internalFail(new RuntimeException(String.format("Run was called when fragment was in %s state.  FragmentRunnables should only be started when they are currently in awaiting allocation state.", FragmentState.valueOf(state.get()))));
      return;
    }



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

      root.stop();
      if(context.isFailed()) {
        internalFail(context.getFailureCause());
      }

      closed = true;

      context.close();
    }catch(AssertionError | Exception ex){
      logger.debug("Caught exception while running fragment", ex);
      internalFail(ex);
    }finally{
      Thread.currentThread().setName(originalThread);
      if(!closed) {
        try {
          if(context.isFailed()) {
            internalFail(context.getFailureCause());
          }
          context.close();
        } catch (RuntimeException e) {
          logger.warn("Failure while closing context in failed state.", e);
        }
      }
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

  public FragmentContext getContext(){
    return context;
  }

}
