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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.rpc.RemoteConnection;
import org.apache.drill.exec.rpc.data.IncomingDataBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.foreman.ForemanException;

import com.google.common.base.Preconditions;

/**
 * This managers determines when to run a non-root fragment node.
 */
// TODO a lot of this is the same as RootFragmentManager
public class NonRootFragmentManager implements FragmentManager {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NonRootFragmentManager.class);

  private final IncomingBuffers buffers;
  private final FragmentExecutor runner;
  private final FragmentHandle handle;
  private volatile boolean cancel = false;
  private final FragmentContext context;
  private final List<RemoteConnection> connections = new CopyOnWriteArrayList<>();
  private volatile boolean runnerRetrieved = false;

  public NonRootFragmentManager(final PlanFragment fragment, final DrillbitContext context)
      throws ExecutionSetupException {
    try {
      this.handle = fragment.getHandle();
      this.context = new FragmentContext(context, fragment, context.getFunctionImplementationRegistry());
      this.buffers = new IncomingBuffers(fragment, this.context);
      final FragmentStatusReporter reporter = new FragmentStatusReporter(this.context,
          context.getController().getTunnel(fragment.getForeman()));
      this.runner = new FragmentExecutor(this.context, fragment, reporter);
      this.context.setBuffers(buffers);

    } catch (ForemanException e) {
      throw new FragmentSetupException("Failure while decoding fragment.", e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.fragment.FragmentHandler#handle(org.apache.drill.exec.rpc.RemoteConnection.ConnectionThrottle, org.apache.drill.exec.record.RawFragmentBatch)
   */
  @Override
  public boolean handle(final IncomingDataBatch batch) throws FragmentSetupException, IOException {
    return buffers.batchArrived(batch);
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.fragment.FragmentHandler#getRunnable()
   */
  @Override
  public FragmentExecutor getRunnable() {
    synchronized(this) {

      // historically, we had issues where we tried to run the same fragment multiple times. Let's check to make sure
      // this isn't happening.
      Preconditions.checkArgument(!runnerRetrieved, "Get Runnable can only be run once.");

      if (cancel) {
        return null;
      }
      runnerRetrieved = true;
      return runner;
    }
  }

  @Override
  public void receivingFragmentFinished(final FragmentHandle handle) {
    runner.receivingFragmentFinished(handle);
  }

  @Override
  public synchronized void cancel() {
    cancel = true;
    runner.cancel();
  }

  @Override
  public boolean isCancelled() {
    return cancel;
  }

  @Override
  public void unpause() {
    runner.unpause();
  }

  @Override
  public FragmentHandle getHandle() {
    return handle;
  }

  @Override
  public boolean isWaiting() {
    return !buffers.isDone() && !cancel;
  }

  @Override
  public FragmentContext getFragmentContext() {
    return context;
  }

  @Override
  public void addConnection(final RemoteConnection connection) {
    connections.add(connection);
  }

  @Override
  public void setAutoRead(final boolean autoRead) {
    for (final RemoteConnection c : connections) {
      c.setAutoRead(autoRead);
    }
  }

}
