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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;

public abstract class AbstractStatusReporter implements StatusReporter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStatusReporter.class);

  private final FragmentContext context;

  public AbstractStatusReporter(final FragmentContext context) {
    super();
    this.context = context;
  }

  private  FragmentStatus.Builder getBuilder(final FragmentState state){
    return getBuilder(context, state, null);
  }

  public static FragmentStatus.Builder getBuilder(final FragmentContext context, final FragmentState state, final UserException ex){
    final FragmentStatus.Builder status = FragmentStatus.newBuilder();
    final MinorFragmentProfile.Builder b = MinorFragmentProfile.newBuilder();
    context.getStats().addMetricsToStatus(b);
    b.setState(state);
    if(ex != null){
      final boolean verbose = context.getOptions().getOption(ExecConstants.ENABLE_VERBOSE_ERRORS_KEY).bool_val;
      b.setError(ex.getOrCreatePBError(verbose));
    }
    status.setHandle(context.getHandle());
    b.setMemoryUsed(context.getAllocator().getAllocatedMemory());
    b.setMinorFragmentId(context.getHandle().getMinorFragmentId());
    status.setProfile(b);
    return status;
  }

  @Override
  public void stateChanged(final FragmentHandle handle, final FragmentState newState) {
    final FragmentStatus.Builder status = getBuilder(newState);
    logger.info("State changed for {}. New state: {}", QueryIdHelper.getQueryIdentifier(handle), newState);
    switch(newState){
    case AWAITING_ALLOCATION:
    case CANCELLATION_REQUESTED:
    case CANCELLED:
    case FINISHED:
    case RUNNING:
      statusChange(handle, status.build());
      break;
    case SENDING:
      // no op.
      break;
    case FAILED:
      // shouldn't get here since fail() should be called.
    default:
      throw new IllegalStateException(String.format("Received state changed event for unexpected state of %s.", newState));
    }
  }

  protected abstract void statusChange(FragmentHandle handle, FragmentStatus status);

  @Override
  public final void fail(final FragmentHandle handle, final UserException excep) {
    final FragmentStatus.Builder status = getBuilder(context, FragmentState.FAILED, excep);
    fail(handle, status);
  }

  private void fail(final FragmentHandle handle, final FragmentStatus.Builder statusBuilder) {
    statusChange(handle, statusBuilder.build());
  }


}
