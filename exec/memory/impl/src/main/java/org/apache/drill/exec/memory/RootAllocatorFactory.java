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

import com.google.common.annotations.VisibleForTesting;

public class RootAllocatorFactory {
  /**
   * Constructor to prevent instantiation of this static utility class.
   */
  private RootAllocatorFactory() {}

  /**
   * Factory method.
   *
   * @param drillConfig the DrillConfig
   * @return a new root allocator
   */
  public static BufferAllocator newRoot(final DrillConfig drillConfig) {
/* TODO(DRILL-1942)
    if (BaseAllocator.DEBUG) {
      return new TopLevelAllocator(drillConfig);
    }
*/

    return new TopLevelAllocator(drillConfig);
  }

  /**
   * Factory method for testing, where a DrillConfig may not be available.
   *
   * @param allocationPolicy the allocation policy
   * @param initAllocation initial allocation from parent
   * @param maxReservation maximum amount of memory this can hand out
   * @param flags one of BufferAllocator's F_* flags
   * @return a new root allocator
   */
/* TODO(DRILL-1942)
  @VisibleForTesting
  public static BufferAllocator newRoot(
      final AllocationPolicy allocationPolicy,
      final long initAllocation, final long maxReservation, final int flags) {
    if (BaseAllocator.DEBUG) {
      return new RootAllocatorDebug(allocationPolicy, initAllocation, maxReservation, flags);
    }

    return new RootAllocator(allocationPolicy, initAllocation, maxReservation, flags);
  }
*/
}
