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

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentStatus.FragmentState;
import org.apache.drill.exec.work.foreman.ErrorHelper;

public class AbstractFragmentRunnerListener implements FragmentRunnerListener{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractFragmentRunnerListener.class);
  
  private FragmentContext context;
  private volatile long startNanos;
  
  public AbstractFragmentRunnerListener(FragmentContext context) {
    super();
    this.context = context;
  }
  
  private  FragmentStatus.Builder getBuilder(FragmentState state){
    FragmentStatus.Builder status = FragmentStatus.newBuilder();
    context.addMetricsToStatus(status);
    status.setState(state);
    status.setRunningTime(System.nanoTime() - startNanos);
    status.setHandle(context.getHandle());
    status.setMemoryUse(context.getAllocator().getAllocatedMemory());
    return status;
  }
  
  @Override
  public void stateChanged(FragmentHandle handle, FragmentState newState) {
    FragmentStatus.Builder status = getBuilder(newState);

    switch(newState){
    case AWAITING_ALLOCATION:
      awaitingAllocation(handle, status);
      break;
    case CANCELLED:
      cancelled(handle, status);
      break;
    case FAILED:
      // no op since fail should have also been called.
      break;
    case FINISHED:
      finished(handle, status);
      break;
    case RUNNING:
      this.startNanos = System.nanoTime();
      running(handle, status);
      break;
    case SENDING:
      // no op.
      break;
    default:
      break;
    
    }
  }
  
  protected void awaitingAllocation(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
  }
  
  protected void running(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
  }

  protected void cancelled(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
  }

  protected void finished(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
  }
  
  protected void statusChange(FragmentHandle handle, FragmentStatus status){
    
  }
  
  @Override
  public final void fail(FragmentHandle handle, String message, Throwable excep) {
    FragmentStatus.Builder status = getBuilder(FragmentState.FAILED);
    status.setError(ErrorHelper.logAndConvertError(context.getIdentity(), message, excep, logger));
    fail(handle, status);
  }

  protected void fail(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
    // TODO: ensure the foreman handles the exception
  }


}
