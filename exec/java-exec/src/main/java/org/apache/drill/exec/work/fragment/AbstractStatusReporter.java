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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.work.ErrorHelper;

public abstract class AbstractStatusReporter implements StatusReporter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStatusReporter.class);

  private FragmentContext context;
  private volatile long startNanos;

  public AbstractStatusReporter(FragmentContext context) {
    super();
    this.context = context;
  }

  private  FragmentStatus.Builder getBuilder(FragmentState state){
    return getBuilder(context, state, null, null);
  }

  public static FragmentStatus.Builder getBuilder(FragmentContext context, FragmentState state, String message, Throwable t){
    FragmentStatus.Builder status = FragmentStatus.newBuilder();
    MinorFragmentProfile.Builder b = MinorFragmentProfile.newBuilder();
    context.getStats().addMetricsToStatus(b);
    b.setState(state);
    if(t != null){
      boolean verbose = context.getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
      b.setError(ErrorHelper.logAndConvertMessageError(context.getIdentity(), message, t, logger, verbose));
    }
    status.setHandle(context.getHandle());
    b.setMemoryUsed(context.getAllocator().getAllocatedMemory());
    b.setMinorFragmentId(context.getHandle().getMinorFragmentId());
    status.setProfile(b);
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

  protected abstract void statusChange(FragmentHandle handle, FragmentStatus status);

  @Override
  public final void fail(FragmentHandle handle, String message, Throwable excep) {
    FragmentStatus.Builder status = getBuilder(context, FragmentState.FAILED, message, excep);
    fail(handle, status);
  }

  protected void fail(FragmentHandle handle, FragmentStatus.Builder statusBuilder){
    statusChange(handle, statusBuilder.build());
  }


}
