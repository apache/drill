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
package org.apache.drill.exec.work.fragment;

import java.io.IOException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.FragmentSetupException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.FragmentLeaf;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.rpc.RemoteConnection.ConnectionThrottle;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.work.FragmentRunner;
import org.apache.drill.exec.work.FragmentRunnerListener;
import org.apache.drill.exec.work.RemotingFragmentRunnerListener;
import org.apache.drill.exec.work.batch.IncomingBuffers;

/**
 * This handler receives all incoming traffic for a particular FragmentHandle.  It will monitor the state of incoming batches
 */
public class RemoteFragmentHandler implements IncomingFragmentHandler {
  private final PlanFragment fragment;
  private FragmentLeaf root;
  private final IncomingBuffers buffers;
  private final FragmentRunnerListener runnerListener;
  private volatile FragmentRunner runner;
  private volatile boolean cancel = false;
  private final FragmentContext context;
  private final PhysicalPlanReader reader;
  
  public RemoteFragmentHandler(PlanFragment fragment, DrillbitContext context, BitTunnel foremanTunnel) throws FragmentSetupException{
    try{
      this.fragment = fragment;
      this.root = context.getPlanReader().readFragmentOperator(fragment.getFragmentJson());
      this.buffers = new IncomingBuffers(root);
      this.context = new FragmentContext(context, fragment.getHandle(), null, buffers, new FunctionImplementationRegistry(context.getConfig()));
      this.runnerListener = new RemotingFragmentRunnerListener(this.context, foremanTunnel);
      this.reader = context.getPlanReader();
      
    }catch(IOException e){
      throw new FragmentSetupException("Failure while decoding fragment.", e);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.fragment.FragmentHandler#handle(org.apache.drill.exec.rpc.RemoteConnection.ConnectionThrottle, org.apache.drill.exec.record.RawFragmentBatch)
   */
  @Override
  public boolean handle(ConnectionThrottle throttle, RawFragmentBatch batch) throws FragmentSetupException {
    return buffers.batchArrived(throttle, batch);
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.fragment.FragmentHandler#getRunnable()
   */
  @Override
  public FragmentRunner getRunnable(){
    synchronized(this){
      if(runner != null) throw new IllegalStateException("Get Runnable can only be run once.");
      if(cancel) return null;
      try {
        FragmentRoot fragRoot = reader.readFragmentOperator(fragment.getFragmentJson());
        RootExec exec = ImplCreator.getExec(context, fragRoot);
        runner = new FragmentRunner(context, exec, runnerListener);
        return this.runner;
      } catch (IOException | ExecutionSetupException e) {
        runnerListener.fail(fragment.getHandle(), "Failure while setting up remote fragment.", e);
        return null;
      }
    }
    
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.work.fragment.FragmentHandler#cancel()
   */
  @Override
  public void cancel(){
    synchronized(this){
      cancel = true;
      if(runner != null){
        runner.cancel();
      }
    }
  }

  @Override
  public FragmentHandle getHandle() {
    return fragment.getHandle();
  }

  @Override
  public boolean isWaiting() {
    return !buffers.isDone() && !cancel;
  }
  
  

  
}