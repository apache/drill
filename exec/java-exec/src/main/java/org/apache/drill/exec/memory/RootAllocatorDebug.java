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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;

/**
 * The root allocator for using direct memory inside a Drillbit. Supports creating a
 * tree of descendant child allocators.
 */
class RootAllocatorDebug extends BaseAllocatorDebug {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RootAllocator.class);

  RootAllocatorDebug(final DrillConfig drillConfig) {
    this(BaseAllocator.getAllocationPolicy(), 0, Math.min(
        DrillConfig.getMaxDirectMemory(), drillConfig.getLong(ExecConstants.TOP_LEVEL_MAX_ALLOC)), 0);
  }

  RootAllocatorDebug(final AllocationPolicy allocationPolicy,
      final long initAllocation, final long maxReservation, final int flags) {
    super(null, new RootAllocatorOwner(), allocationPolicy, initAllocation,
        BaseAllocator.maxDirect = maxReservation, flags);
    assert (flags & F_LIMITING_ROOT) == 0 : "the RootAllocator shouldn't be a limiting root";
  }

  @Override
  protected boolean canIncreaseOwned(final long nBytes, final int flags) {
    // the end total has already been checked against maxAllocation, so we can just return true
    return true;
  }

  @Override
  public void close() throws Exception {
    super.close();
    logger.debug("RootAllocatorDebug.close(): " + getRetries());
  }
}
