/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.exec.work.fragment;

import org.junit.Before;
import org.junit.Test;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentStats;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.rpc.control.ControlTunnel;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.apache.drill.exec.proto.UserBitShared.FragmentState.CANCELLATION_REQUESTED;
import static org.apache.drill.exec.proto.UserBitShared.FragmentState.FAILED;
import static org.apache.drill.exec.proto.UserBitShared.FragmentState.RUNNING;

public class FragmentStatusReporterTest {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStatusReporterTest.class);

  private FragmentStatusReporter statusReporter;
  private ControlTunnel tunnel;

  @Before
  public void setUp() throws Exception {
    FragmentContext context = mock(FragmentContext.class);
    when(context.getStats()).thenReturn(mock(FragmentStats.class));
    when(context.getHandle()).thenReturn(FragmentHandle.getDefaultInstance());
    when(context.getAllocator()).thenReturn(mock(BufferAllocator.class));
    tunnel = mock(ControlTunnel.class);
    statusReporter = new FragmentStatusReporter(context, tunnel);
  }

  @Test
  public void testStateChanged() throws Exception {
    for (FragmentState state : FragmentState.values()) {
      try {
        statusReporter.stateChanged(state);
        if (state == FAILED) {
          fail("Expected exception: " + IllegalStateException.class.getName());
        }
      } catch (IllegalStateException e) {
        if (state != FAILED) {
          fail("Unexpected exception: " + e.toString());
        }
      }
    }
    verify(tunnel, times(FragmentState.values().length - 2)) /* exclude SENDING and FAILED */
        .sendFragmentStatus(any(FragmentStatus.class));
  }

  @Test
  public void testFail() throws Exception {
    statusReporter.fail(null);
    verify(tunnel).sendFragmentStatus(any(FragmentStatus.class));
  }

  @Test
  public void testClose() throws Exception {
    statusReporter.close();
    verifyZeroInteractions(tunnel);
  }

  @Test
  public void testCloseClosed() throws Exception {
    statusReporter.close();
    statusReporter.close();
    verifyZeroInteractions(tunnel);
  }

  @Test
  public void testStateChangedAfterClose() throws Exception {
    statusReporter.stateChanged(RUNNING);
    verify(tunnel).sendFragmentStatus(any(FragmentStatus.class));
    statusReporter.close();
    statusReporter.stateChanged(CANCELLATION_REQUESTED);
    verify(tunnel).sendFragmentStatus(any(FragmentStatus.class));
  }
}
