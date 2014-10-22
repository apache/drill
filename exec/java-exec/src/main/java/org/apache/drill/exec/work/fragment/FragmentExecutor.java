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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.work.CancelableQuery;
import org.apache.drill.exec.work.StatusProvider;
import org.apache.drill.exec.work.WorkManager.WorkerBee;

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
  private final WorkerBee bee;
  private final StatusReporter listener;
  private Thread executionThread;
  private AtomicBoolean closed = new AtomicBoolean(false);

  public FragmentExecutor(FragmentContext context, WorkerBee bee, FragmentRoot rootOperator, StatusReporter listener) {
    this.context = context;
    this.bee = bee;
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
    logger.debug("Cancelled Fragment {}", context.getHandle());
    context.cancel();
    if (executionThread != null) {
      executionThread.interrupt();
    }
  }

  public void receivingFragmentFinished(FragmentHandle handle) {
    root.receivingFragmentFinished(handle);
  }

  public UserClientConnection getClient() {
    return context.getConnection();
  }

  @Override
  public void run() {
    final String originalThread = Thread.currentThread().getName();
    try {
      String newThreadName = String.format("%s:frag:%s:%s", //
          QueryIdHelper.getQueryId(context.getHandle().getQueryId()), //
          context.getHandle().getMajorFragmentId(),
          context.getHandle().getMinorFragmentId()
          );
      Thread.currentThread().setName(newThreadName);
      executionThread = Thread.currentThread();

      root = ImplCreator.getExec(context, rootOperator);

      logger.debug("Starting fragment runner. {}:{}", context.getHandle().getMajorFragmentId(), context.getHandle().getMinorFragmentId());
      if (!updateState(FragmentState.AWAITING_ALLOCATION, FragmentState.RUNNING, false)) {
        internalFail(new RuntimeException(String.format("Run was called when fragment was in %s state.  FragmentRunnables should only be started when they are currently in awaiting allocation state.", FragmentState.valueOf(state.get()))));
        return;
      }

      // run the query until root.next returns false.
      while (state.get() == FragmentState.RUNNING_VALUE) {
        if (!root.next()) {
          if (context.isFailed()) {
            internalFail(context.getFailureCause());
            closeOutResources(false);
          } else {
            closeOutResources(true); // make sure to close out resources before we report success.
            updateState(FragmentState.RUNNING, FragmentState.FINISHED, false);
          }

          break;
        }
      }
    } catch (AssertionError | Exception e) {
      logger.warn("Error while initializing or executing fragment", e);
      context.fail(e);
      internalFail(e);
    } finally {
      bee.removeFragment(context.getHandle());

      logger.debug("Fragment runner complete. {}:{}", context.getHandle().getMajorFragmentId(), context.getHandle().getMinorFragmentId());
      Thread.currentThread().setName(originalThread);
    }
  }

  private void closeOutResources(boolean throwFailure) {
    if (closed.get()) {
      return;
    }

    try {
      root.stop();
    } catch (RuntimeException e) {
      if (throwFailure) {
        throw e;
      }
      logger.warn("Failure while closing out resources.", e);
    }

    try {
      context.close();
    } catch (RuntimeException e) {
      if (throwFailure) {
        throw e;
      }
      logger.warn("Failure while closing out resources.", e);
    }

    closed.set(true);
  }

  private void internalFail(Throwable excep) {
    state.set(FragmentState.FAILED_VALUE);
    listener.fail(context.getHandle(), "Failure while running fragment.", excep);
  }

  private void updateState(FragmentState update) {
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

  public FragmentContext getContext() {
    return context;
  }

}
