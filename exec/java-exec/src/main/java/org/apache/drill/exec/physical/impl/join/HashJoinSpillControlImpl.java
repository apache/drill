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

package org.apache.drill.exec.physical.impl.join;

  import org.apache.drill.exec.memory.BufferAllocator;
  import org.apache.drill.exec.record.RecordBatchSizer;
  import org.apache.drill.exec.record.VectorContainer;
  // import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
  // import java.util.concurrent.TimeUnit;

/**
 * This class is currently used only for Semi-Hash-Join that avoids duplicates by the use of a hash table
 * The method {@link HashJoinMemoryCalculator.HashJoinSpillControl#shouldSpill(VectorContainer)} returns true if the memory available now to the allocator if not enough
 * to hold (a multiple of, for safety) a new allocated batch
 */
public class HashJoinSpillControlImpl implements HashJoinMemoryCalculator.HashJoinSpillControl {
  private BufferAllocator allocator;
  private int recordsPerBatch;

  HashJoinSpillControlImpl(BufferAllocator allocator, int recordsPerBatch) {
    this.allocator = allocator;
    this.recordsPerBatch = recordsPerBatch;
  }

  @Override
  public boolean shouldSpill(VectorContainer currentVectorContainer) {
    // Stopwatch watch = Stopwatch.createStarted();
    assert currentVectorContainer.hasRecordCount();
    assert currentVectorContainer.getRecordCount() == recordsPerBatch;
    // Expected new batch size like the current, plus the Hash Value vector (4 bytes per HV)
    long batchSize = new RecordBatchSizer(currentVectorContainer).getActualSize() + 4 * recordsPerBatch;
    long memoryAvailableNow = allocator.getLimit() - allocator.getAllocatedMemory();
    boolean needsSpill = 3 * batchSize > memoryAvailableNow; // go spill if too little memory is available
    // long total = watch.elapsed(TimeUnit.MICROSECONDS);
    return needsSpill;
  }

}
