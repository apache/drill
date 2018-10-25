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
package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class HashAggMemoryCalculatorImpl implements HashAggMemoryCalculator {
  private boolean initialized = false;

  private final BufferAllocator allocator;
  private final long estOutgoingAllocSize;
  private final long estValuesBatchSize;

  private long reserveOutgoingMemory;
  private long reserveValueBatchMemory;

  public HashAggMemoryCalculatorImpl(
                         final BufferAllocator allocator,
                         final long estOutgoingAllocSize,
                         final long estValuesBatchSize,
                         final long reserveOutgoingMemory,
                         final long reserveValueBatchMemory) {
    Preconditions.checkState(!initialized);
    initialized = true;

    this.allocator = Preconditions.checkNotNull(allocator);
    this.estOutgoingAllocSize = estOutgoingAllocSize;
    this.estValuesBatchSize = estValuesBatchSize;
    this.reserveOutgoingMemory = reserveOutgoingMemory;
    this.reserveValueBatchMemory = reserveValueBatchMemory;
  }

  @Override
  public void useReservedValuesMemory() {
    // try to preempt an OOM by using the reserved memory
    long reservedMemory = reserveValueBatchMemory;
    if ( reservedMemory > 0 ) { allocator.setLimit(allocator.getLimit() + reservedMemory); }

    reserveValueBatchMemory = 0;
  }

  @Override
  public void useReservedOutgoingMemory() {
    Preconditions.checkState(initialized);

    // try to preempt an OOM by using the reserved memory
    long reservedMemory = reserveOutgoingMemory;
    if ( reservedMemory > 0 ) { allocator.setLimit(allocator.getLimit() + reservedMemory); }

    reserveOutgoingMemory = 0;
  }

  @Override
  public void restoreReservedMemory() {
    Preconditions.checkState(initialized);

    if ( 0 == reserveOutgoingMemory ) { // always restore OutputValues first (needed for spilling)
      long memAvail = allocator.getLimit() - allocator.getAllocatedMemory();
      if ( memAvail > estOutgoingAllocSize) {
        allocator.setLimit(allocator.getLimit() - estOutgoingAllocSize);
        reserveOutgoingMemory = estOutgoingAllocSize;
      }
    }
    if ( 0 == reserveValueBatchMemory ) {
      long memAvail = allocator.getLimit() - allocator.getAllocatedMemory();
      if ( memAvail > estValuesBatchSize) {
        allocator.setLimit(allocator.getLimit() - estValuesBatchSize);
        reserveValueBatchMemory = estValuesBatchSize;
      }
    }
  }
}
