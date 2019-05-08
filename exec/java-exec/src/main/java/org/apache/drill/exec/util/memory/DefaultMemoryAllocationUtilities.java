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
package org.apache.drill.exec.util.memory;

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.options.OptionManager;

public class DefaultMemoryAllocationUtilities extends MemoryAllocationUtilities {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultMemoryAllocationUtilities.class);


  /**
   * Helper method to setup Memory Allocations
   * <p>
   * Plan the memory for buffered operators (the only ones that can spill in this release)
   * based on assumptions. These assumptions are the amount of memory per node to give
   * to each query and the number of sort operators per node.
   * <p>
   * The reason the total
   * memory is an assumption is that we have know knowledge of the number of queries
   * that can run, so we need the user to tell use that information by configuring the
   * amount of memory to be assumed available to each query.
   * <p>
   * The number of sorts per node could be calculated, but we instead simply take
   * the worst case: the maximum per-query, per-node parallization and assume that
   * all sorts appear in all fragments &mdash; a gross oversimplification, but one
   * that Drill has long made.
   * <p>
   * since this method can be used in multiple places adding it in this class
   * rather than keeping it in Foreman
   * @param planHasMemory defines the memory planning needs to be done or not.
   *                             generally skipped when the plan contains memory allocation.
   * @param bufferedOperators list of buffered operators in the plan.
   * @param queryContext context of the query.
   */
  public static void setupBufferedOpsMemoryAllocations(boolean planHasMemory,
    List<PhysicalOperator> bufferedOperators, final QueryContext queryContext) {

    // Test plans may already have a pre-defined memory plan.
    // Otherwise, determine memory allocation.

    if (planHasMemory || bufferedOperators.isEmpty()) {
      return;
    }

    // Setup options, etc.

    final OptionManager optionManager = queryContext.getOptions();
    final long directMemory = DrillConfig.getMaxDirectMemory();

    // Compute per-node, per-query memory.

    final long maxAllocPerNode = computeQueryMemory(queryContext.getConfig(), optionManager, directMemory);
    logger.debug("Memory per query per node: {}", maxAllocPerNode);

    // Now divide up the memory by slices and operators.

    final long opMinMem = computeOperatorMemory(optionManager, maxAllocPerNode, bufferedOperators.size());

    for(final PhysicalOperator op : bufferedOperators) {
      final long alloc = Math.max(opMinMem, op.getInitialAllocation());
      op.setMaxAllocation(alloc);
    }
  }


}
