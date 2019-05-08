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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

import java.util.List;

public class MemoryAllocationUtilities {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryAllocationUtilities.class);
  /**
   * Per-node memory calculations based on a number of constraints.
   * <p>
   * Factored out into a separate method to allow unit testing.
   * @param config Drill config
   * @param optionManager system options
   * @param directMemory amount of direct memory
   * @return memory per query per node
   */

  @VisibleForTesting
  public static long computeQueryMemory(DrillConfig config, OptionSet optionManager, long directMemory) {

    // Memory computed as a percent of total memory.

    long perQueryMemory = Math.round(directMemory *
      optionManager.getOption(ExecConstants.PERCENT_MEMORY_PER_QUERY));

    // But, must allow at least the amount given explicitly for
    // backward compatibility.

    perQueryMemory = Math.max(perQueryMemory,
      optionManager.getOption(ExecConstants.MAX_QUERY_MEMORY_PER_NODE));

    // Compute again as either the total direct memory, or the
    // configured maximum top-level allocation (10 GB).

    long maxAllocPerNode = Math.min(directMemory,
      config.getLong(RootAllocatorFactory.TOP_LEVEL_MAX_ALLOC));

    // Final amount per node per query is the minimum of these two.

    maxAllocPerNode = Math.min(maxAllocPerNode, perQueryMemory);
    return maxAllocPerNode;
  }

  public static class BufferedOpFinder extends AbstractPhysicalVisitor<Void, List<PhysicalOperator>, RuntimeException> {
    private final QueryContext context;

    public BufferedOpFinder(QueryContext queryContext) {
      this.context = queryContext;
    }

    @Override
    public Void visitOp(PhysicalOperator op, List<PhysicalOperator> value)
      throws RuntimeException {
      if (op.isBufferedOperator(context)) {
        value.add(op);
      }
      visitChildren(op, value);
      return null;
    }
  }


  /**
   * Compute per-operator memory based on the computed per-node memory, the
   * number of operators, and the computed number of fragments (which house
   * the operators.) Enforces a floor on the amount of memory per operator.
   *
   * @param optionManager system option manager
   * @param maxAllocPerNode computed query memory per node
   * @param opCount number of buffering operators in this query
   * @return the per-operator memory
   */

  public static long computeOperatorMemory(OptionSet optionManager, long maxAllocPerNode, int opCount) {
    final long maxWidth = optionManager.getOption(ExecConstants.MAX_WIDTH_PER_NODE);
    final double cpuLoadAverage = optionManager.getOption(ExecConstants.CPU_LOAD_AVERAGE);
    final long maxWidthPerNode = ExecConstants.MAX_WIDTH_PER_NODE.computeMaxWidth(cpuLoadAverage, maxWidth);
    final long maxOperatorAlloc = maxAllocPerNode / (opCount * maxWidthPerNode);
    logger.debug("Max buffered operator alloc: {}", maxOperatorAlloc);

    // User configurable option to allow forcing minimum memory.
    // Ensure that the buffered ops receive the minimum memory needed to make progress.
    // Without this, the math might work out to allocate too little memory.

    return Math.max(maxOperatorAlloc,
      optionManager.getOption(ExecConstants.MIN_MEMORY_PER_BUFFERED_OP));
  }
}
