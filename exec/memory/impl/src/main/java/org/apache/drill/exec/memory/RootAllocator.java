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
package org.apache.drill.exec.memory;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorL;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.testing.ExecutionControls;

import com.google.common.annotations.VisibleForTesting;

/**
 * The root allocator for using direct memory inside a Drillbit. Supports creating a
 * tree of descendant child allocators.
 */
public class RootAllocator extends BaseAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RootAllocator.class);

  // TODO these statics, and others in BaseAllocator, may be a problem for multiple in-process Drillbits
  private static final PooledByteBufAllocatorL innerAllocator = PooledByteBufAllocatorL.DEFAULT;
  private static long maxDirect;

  public static AllocationPolicy getAllocationPolicy() {
    final String policyName = System.getProperty(ExecConstants.ALLOCATION_POLICY,
        BaseAllocator.POLICY_LOCAL_MAX_NAME); // TODO try with PER_FRAGMENT_NAME

    switch(policyName) {
    case POLICY_PER_FRAGMENT_NAME:
      return POLICY_PER_FRAGMENT;
    case POLICY_LOCAL_MAX_NAME:
      return POLICY_LOCAL_MAX;
    default:
      throw new IllegalArgumentException("Unrecognized allocation policy name \"" + policyName + "\"");
    }
  }

  public RootAllocator(final DrillConfig drillConfig) {
    this(getAllocationPolicy(), 0, Math.min(
        DrillConfig.getMaxDirectMemory(), drillConfig.getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC)), 0);
  }

  public static long getMaxDirect() {
    return maxDirect;
  }

  /**
   * Provide statistics via JMX for each RootAllocator.
   */
  private class AllocatorsStats implements AllocatorsStatsMXBean {
    @Override
    public long getMaxDirectMemory() {
      return maxDirect;
    }
  }

  private static class RootAllocatorOwner implements AllocatorOwner {
    @Override
    public ExecutionControls getExecutionControls() {
      return null;
    }

    @Override
    public FragmentContext getFragmentContext() {
      return null;
    }
  }

  @VisibleForTesting
  public RootAllocator(final AllocationPolicy allocationPolicy,
      final long initAllocation, final long maxReservation, final int flags) {
    super(null, new RootAllocatorOwner(), allocationPolicy, initAllocation, maxDirect = maxReservation, flags);
    assert (flags & F_LIMITING_ROOT) == 0 : "the RootAllocator shouldn't be a limiting root";

    try {
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName objectName = new ObjectName("org.apache.drill.exec.memory:Allocators=" + id);
      final AllocatorsStats mbean = new AllocatorsStats();
      mbs.registerMBean(mbean, objectName);
    } catch(Exception e) {
      logger.info("Exception setting up AllocatorsStatsMBean", e);
    }
  }

  @Override
  public ByteBufAllocator getUnderlyingAllocator() {
    return innerAllocator;
  }

  @Override
  protected boolean canIncreaseOwned(final long nBytes, final int flags) {
    // the end total has already been checked against maxAllocation, so we can just return true
    return true;
  }

  /**
   * Verify the accounting state of the allocation system.
   */
  @VisibleForTesting
  public void verify() {
    verifyAllocator();
  }
}
