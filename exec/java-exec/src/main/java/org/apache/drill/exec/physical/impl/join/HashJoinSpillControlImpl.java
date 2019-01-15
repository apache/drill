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

  import org.apache.drill.exec.ExecConstants;
  import org.apache.drill.exec.memory.BufferAllocator;
  import org.apache.drill.exec.ops.FragmentContext;
  import org.apache.drill.exec.record.RecordBatch;
  import org.apache.drill.exec.record.RecordBatchMemoryManager;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

  import javax.annotation.Nullable;
  import java.util.Set;

  import static org.apache.drill.exec.record.JoinBatchMemoryManager.LEFT_INDEX;
  import static org.apache.drill.exec.record.JoinBatchMemoryManager.RIGHT_INDEX;

/**
 * This class is currently used only for Semi-Hash-Join that avoids duplicates by the use of a hash table
 * The method {@link HashJoinMemoryCalculator.HashJoinSpillControl#shouldSpill()} returns true if the memory available now to the allocator if not enough
 * to hold (a multiple of, for safety) a new allocated batch
 */
public class HashJoinSpillControlImpl implements HashJoinMemoryCalculator.BuildSidePartitioning {
  private static final Logger logger = LoggerFactory.getLogger(HashJoinSpillControlImpl.class);

  private BufferAllocator allocator;
  private int recordsPerBatch;
  private int minBatchesInAvailableMemory;
  private RecordBatchMemoryManager batchMemoryManager;
  private int initialPartitions;
  private int numPartitions;
  private int recordsPerPartitionBatchProbe;
  private FragmentContext context;
  HashJoinMemoryCalculator.PartitionStatSet partitionStatSet;

  HashJoinSpillControlImpl(BufferAllocator allocator, int recordsPerBatch, int minBatchesInAvailableMemory, RecordBatchMemoryManager batchMemoryManager, FragmentContext context) {
    this.allocator = allocator;
    this.recordsPerBatch = recordsPerBatch;
    this.minBatchesInAvailableMemory = minBatchesInAvailableMemory;
    this.batchMemoryManager = batchMemoryManager;
    this.context = context;
  }

  @Override
  public boolean shouldSpill() {
    // Expected new batch size like the current, plus the Hash Values vector (4 bytes per HV)
    long batchSize = ( batchMemoryManager.getRecordBatchSizer(RIGHT_INDEX).getRowAllocWidth() + 4 ) * recordsPerBatch;
    long reserveForOutgoing = batchMemoryManager.getOutputBatchSize();
    long memoryAvailableNow = allocator.getLimit() - allocator.getAllocatedMemory() - reserveForOutgoing;
    boolean needsSpill = minBatchesInAvailableMemory * batchSize > memoryAvailableNow;
    if ( needsSpill ) {
      logger.debug("should spill now - batch size {}, mem avail {}, reserved for outgoing {}", batchSize, memoryAvailableNow, reserveForOutgoing);
    }
    return needsSpill;   // go spill if too little memory is available
  }

  @Override
  public void initialize(boolean firstCycle,
                         boolean reserveHash,
                         RecordBatch buildSideBatch,
                         RecordBatch probeSideBatch,
                         Set<String> joinColumns,
                         boolean probeEmpty,
                         long memoryAvailable,
                         int initialPartitions,
                         int recordsPerPartitionBatchBuild,
                         int recordsPerPartitionBatchProbe,
                         int maxBatchNumRecordsBuild,
                         int maxBatchNumRecordsProbe,
                         int outputBatchSize,
                         double loadFactor) {
    this.initialPartitions = initialPartitions;
    this.recordsPerPartitionBatchProbe = recordsPerPartitionBatchProbe;

    calculateMemoryUsage();
  }

  @Override
  public void setPartitionStatSet(HashJoinMemoryCalculator.PartitionStatSet partitionStatSet) {
    this.partitionStatSet = partitionStatSet;
  }

  @Override
  public int getNumPartitions() {
    return numPartitions;
  }

  /**
   * Calculate the number of partitions possible for the given available memory
   * start at initialPartitions and adjust down (in powers of 2) as needed
   */
  private void calculateMemoryUsage() {
    long reserveForOutgoing = batchMemoryManager.getOutputBatchSize();
    long memoryAvailableNow = allocator.getLimit() - allocator.getAllocatedMemory() - reserveForOutgoing;

    // Expected new batch size like the current, plus the Hash Values vector (4 bytes per HV)
    int buildBatchSize = ( batchMemoryManager.getRecordBatchSizer(RIGHT_INDEX).getRowAllocWidth() + 4 ) * recordsPerBatch;
    int hashTableSize = buildBatchSize /* the keys in the HT */ +
      4 * (int)context.getOptions().getLong(ExecConstants.MIN_HASH_TABLE_SIZE_KEY) /* the initial hash table buckets */ +
      (2 + 2) * recordsPerBatch; /* the hash-values and the links */
    int probeBatchSize = ( batchMemoryManager.getRecordBatchSizer(LEFT_INDEX).getRowAllocWidth() + 4 ) * recordsPerBatch;

    long memoryNeededPerPartition = Integer.max(buildBatchSize + hashTableSize, probeBatchSize);

    for ( numPartitions = initialPartitions; numPartitions > 2; numPartitions /= 2 ) { // need at least 2
      // each partition needs at least one internal build batch, and a minimum hash-table
      // ( or an internal probe batch, for a spilled partition during probe phase )
      // adding "min batches" to create some safety slack
      if ( memoryAvailableNow >
             ( numPartitions + minBatchesInAvailableMemory ) * memoryNeededPerPartition ) {
        break; // got enough memory
      }
    }
    logger.debug("Spill control chosen to use {} partitions", numPartitions);
  }

  @Override
  public long getBuildReservedMemory() {
    return 0;
  }

  @Override
  public long getMaxReservedMemory() {
    return 0;
  }

  @Override
  public String makeDebugString() {
    return "Spill Control " + HashJoinMemoryCalculatorImpl.HashJoinSpillControl.class.getCanonicalName();
  }

  @Nullable
  @Override
  public HashJoinMemoryCalculator.PostBuildCalculations next() {
    return new SpillControlPostBuildCalculationsImpl(recordsPerPartitionBatchProbe,
      allocator, recordsPerBatch, minBatchesInAvailableMemory, batchMemoryManager, partitionStatSet);
  }

  @Override
  public HashJoinState getState() {
    return HashJoinState.BUILD_SIDE_PARTITIONING;
  }


  /**
   *   The purpose of this class is to provide the method {@link #shouldSpill} that ensures that enough memory is available to
   *   hold all the probe incoming batches for those partitions that spilled (else need to spill more of them, for more memory).
   */
  public static class SpillControlPostBuildCalculationsImpl implements HashJoinMemoryCalculator.PostBuildCalculations {
    private static final Logger logger = LoggerFactory.getLogger(SpillControlPostBuildCalculationsImpl.class);

    private final int recordsPerPartitionBatchProbe;
    private BufferAllocator allocator;
    private int recordsPerBatch;
    private int minBatchesInAvailableMemory;
    private RecordBatchMemoryManager batchMemoryManager;
    private boolean probeEmpty;
    private final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet;


    public SpillControlPostBuildCalculationsImpl(final int recordsPerPartitionBatchProbe,
                                                 BufferAllocator allocator, int recordsPerBatch, int minBatchesInAvailableMemory,
                                                 RecordBatchMemoryManager batchMemoryManager,
                                                 final HashJoinMemoryCalculator.PartitionStatSet buildPartitionStatSet) {
      this.allocator = allocator;
      this.recordsPerBatch = recordsPerBatch;
      this.minBatchesInAvailableMemory = minBatchesInAvailableMemory;
      this.batchMemoryManager = batchMemoryManager;
      this.recordsPerPartitionBatchProbe = recordsPerPartitionBatchProbe;
      this.buildPartitionStatSet = buildPartitionStatSet;
    }

    @Override
    public void initialize(boolean hasProbeData) {
      this.probeEmpty = hasProbeData;
    }


    @Override
    public int getProbeRecordsPerBatch() {
      return recordsPerPartitionBatchProbe;
    }

    @Override
    public boolean shouldSpill() {
      int numPartitionsSpilled = buildPartitionStatSet.getNumSpilledPartitions();
      if ( numPartitionsSpilled == 0 ) { return false; } // no extra memory is needed if all the build side is in memory
      if ( probeEmpty ) { return false; } // no probe side data
      // Expected new batch size like the current, plus the Hash Values vector (4 bytes per HV)
      long batchSize = ( batchMemoryManager.getRecordBatchSizer(LEFT_INDEX).getRowAllocWidth() + 4 ) * recordsPerBatch;
      long reserveForOutgoing = batchMemoryManager.getOutputBatchSize();
      long memoryAvailableNow = allocator.getLimit() - allocator.getAllocatedMemory() - reserveForOutgoing;
      boolean needsSpill = (numPartitionsSpilled + minBatchesInAvailableMemory ) * batchSize > memoryAvailableNow;
      if ( needsSpill ) {
        logger.debug("Post build should spill now - batch size {}, mem avail {}, reserved for outgoing {}, num partn spilled {}", batchSize,
          memoryAvailableNow, reserveForOutgoing, numPartitionsSpilled);
      }
      return needsSpill;   // go spill if too little memory is available
    }

    @Nullable
    @Override
    public HashJoinMemoryCalculator next() {
      return null;
    }

    @Override
    public HashJoinState getState() {
      return HashJoinState.POST_BUILD_CALCULATIONS;
    }

    @Override
    public String makeDebugString() {
      return "Spill Control " + HashJoinMemoryCalculatorImpl.HashJoinSpillControl.class.getCanonicalName() + " calculator.";
    }
  }



}

