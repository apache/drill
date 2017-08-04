/*
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
package org.apache.drill.exec.util;

import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.options.OptionManager;

public class MemoryAllocationUtilities {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryAllocationUtilities.class);

  /**
   * Helper method to setup SortMemoryAllocations
   * since this method can be used in multiple places adding it in this class
   * rather than keeping it in Foreman
   * @param plan
   * @param queryContext
   */
  public static void setupBufferedOpsMemoryAllocations(final PhysicalPlan plan, final QueryContext queryContext) {

    // Test plans may already have a pre-defined memory plan.
    // Otherwise, determine memory allocation.

    if (plan.getProperties().hasResourcePlan) {
      return;
    }
    // look for external sorts
    final List<PhysicalOperator> bufferedOpList = new LinkedList<>();
    for (final PhysicalOperator op : plan.getSortedOperators()) {
      if ( op.isBufferedOperator() ) {
        bufferedOpList.add(op);
      }
    }

    // if there are any sorts, compute the maximum allocation, and set it on them
    if (bufferedOpList.size() > 0) {
      final OptionManager optionManager = queryContext.getOptions();
      final long maxWidthPerNode = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val;
      long maxAllocPerNode = Math.min(DrillConfig.getMaxDirectMemory(),
          queryContext.getConfig().getLong(RootAllocatorFactory.TOP_LEVEL_MAX_ALLOC));
      maxAllocPerNode = Math.min(maxAllocPerNode,
          optionManager.getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE_KEY).num_val);
      final long maxOperatorAlloc = maxAllocPerNode / (bufferedOpList.size() * maxWidthPerNode);
      logger.debug("Max buffered operator alloc: {}", maxOperatorAlloc);

      // User configurable option to allow forcing minimum memory.
      // Ensure that the buffered ops receive the minimum memory needed to make progress.
      // Without this, the math might work out to allocate too little memory.
      final long opMinMem = queryContext.getOptions().getOption(ExecConstants.MIN_MEMORY_PER_BUFFERED_OP_KEY).num_val;

      for(final PhysicalOperator op : bufferedOpList) {

        long alloc = Math.max(maxOperatorAlloc, op.getInitialAllocation());
        alloc = Math.max(alloc, opMinMem);
        op.setMaxAllocation(alloc);
      }
    }
    plan.getProperties().hasResourcePlan = true;
  }
}
