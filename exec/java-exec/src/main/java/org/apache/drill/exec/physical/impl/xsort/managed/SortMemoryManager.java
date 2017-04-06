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
package org.apache.drill.exec.physical.impl.xsort.managed;

import com.google.common.annotations.VisibleForTesting;

public class SortMemoryManager {

  /**
   * Maximum memory this operator may use. Usually comes from the
   * operator definition, but may be overridden by a configuration
   * parameter for unit testing.
   */

  private final long memoryLimit;

  /**
   * Estimated size of the records for this query, updated on each
   * new batch received from upstream.
   */

  private int estimatedRowWidth;

  /**
   * Size of the merge batches that this operator produces. Generally
   * the same as the merge batch size, unless low memory forces a smaller
   * value.
   */

  private int expectedMergeBatchSize;

  /**
   * Estimate of the input batch size based on the largest batch seen
   * thus far.
   */
  private int estimatedInputBatchSize;

  /**
   * Maximum memory level before spilling occurs. That is, we can buffer input
   * batches in memory until we reach the level given by the buffer memory pool.
   */

  private long bufferMemoryLimit;

  /**
   * Maximum memory that can hold batches during the merge
   * phase.
   */

  private long mergeMemoryLimit;

  /**
   * The target size for merge batches sent downstream.
   */

  private int preferredMergeBatchSize;

  /**
   * The configured size for each spill batch.
   */
  private int preferredSpillBatchSize;

  /**
   * Estimated number of rows that fit into a single spill batch.
   */

  private int spillBatchRowCount;

  /**
   * The estimated actual spill batch size which depends on the
   * details of the data rows for any particular query.
   */

  private int expectedSpillBatchSize;

  /**
   * The number of records to add to each output batch sent to the
   * downstream operator or spilled to disk.
   */

  private int mergeBatchRowCount;

  private SortConfig config;

  private int estimatedInputSize;

  private boolean potentialOverflow;

  public SortMemoryManager(SortConfig config, long memoryLimit) {
    this.config = config;

    // The maximum memory this operator can use as set by the
    // operator definition (propagated to the allocator.)

    if (config.maxMemory() > 0) {
      this.memoryLimit = Math.min(memoryLimit, config.maxMemory());
    } else {
      this.memoryLimit = memoryLimit;
    }

    preferredSpillBatchSize = config.spillBatchSize();;
    preferredMergeBatchSize = config.mergeBatchSize();
  }

  /**
   * Update the data-driven memory use numbers including:
   * <ul>
   * <li>The average size of incoming records.</li>
   * <li>The estimated spill and output batch size.</li>
   * <li>The estimated number of average-size records per
   * spill and output batch.</li>
   * <li>The amount of memory set aside to hold the incoming
   * batches before spilling starts.</li>
   * </ul>
   * <p>
   * Under normal circumstances, the amount of memory available is much
   * larger than the input, spill or merge batch sizes. The primary question
   * is to determine how many input batches we can buffer during the load
   * phase, and how many spill batches we can merge during the merge
   * phase.
   *
   * @param batchSize the overall size of the current batch received from
   * upstream
   * @param batchRowWidth the average width in bytes (including overhead) of
   * rows in the current input batch
   * @param batchRowCount the number of actual (not filtered) records in
   * that upstream batch
   */

  public void updateEstimates(int batchSize, int batchRowWidth, int batchRowCount) {

    // The record count should never be zero, but better safe than sorry...

    if (batchRowCount == 0) {
      return; }


    // Update input batch estimates.
    // Go no further if nothing changed.

    if (! updateInputEstimates(batchSize, batchRowWidth, batchRowCount)) {
      return;
    }

    updateSpillSettings();
    updateMergeSettings();
    adjustForLowMemory();
    logSettings(batchRowCount);
  }

  private boolean updateInputEstimates(int batchSize, int batchRowWidth, int batchRowCount) {

    // The row width may end up as zero if all fields are nulls or some
    // other unusual situation. In this case, assume a width of 10 just
    // to avoid lots of special case code.

    if (batchRowWidth == 0) {
      batchRowWidth = 10;
    }

    // We know the batch size and number of records. Use that to estimate
    // the average record size. Since a typical batch has many records,
    // the average size is a fairly good estimator. Note that the batch
    // size includes not just the actual vector data, but any unused space
    // resulting from power-of-two allocation. This means that we don't
    // have to do size adjustments for input batches as we will do below
    // when estimating the size of other objects.

    // Record sizes may vary across batches. To be conservative, use
    // the largest size observed from incoming batches.

    int origRowEstimate = estimatedRowWidth;
    estimatedRowWidth = Math.max(estimatedRowWidth, batchRowWidth);

    // Maintain an estimate of the incoming batch size: the largest
    // batch yet seen. Used to reserve memory for the next incoming
    // batch. Because we are using the actual observed batch size,
    // the size already includes overhead due to power-of-two rounding.

    long origInputBatchSize = estimatedInputBatchSize;
    estimatedInputBatchSize = Math.max(estimatedInputBatchSize, batchSize);

    // Estimate the total size of each incoming batch plus sv2. Note that, due
    // to power-of-two rounding, the allocated sv2 size might be twice the data size.

    estimatedInputSize = estimatedInputBatchSize + 4 * batchRowCount;

    // Return whether anything changed.

    return estimatedRowWidth != origRowEstimate || estimatedInputBatchSize != origInputBatchSize;
  }

  /**
   * Determine the number of records to spill per spill batch. The goal is to
   * spill batches of either 64K records, or as many records as fit into the
   * amount of memory dedicated to each spill batch, whichever is less.
   */

  private void updateSpillSettings() {

    spillBatchRowCount = rowsPerBatch(preferredSpillBatchSize);

    // Compute the actual spill batch size which may be larger or smaller
    // than the preferred size depending on the row width. Double the estimated
    // memory needs to allow for power-of-two rounding.

    expectedSpillBatchSize = batchForRows(spillBatchRowCount);

    // Determine the minimum memory needed for spilling. Spilling is done just
    // before accepting a spill batch, so we must spill if we don't have room for a
    // (worst case) input batch. To spill, we need room for the spill batch created
    // by merging the batches already in memory.

    bufferMemoryLimit = memoryLimit - expectedSpillBatchSize;
  }

  /**
   * Determine the number of records per batch per merge step. The goal is to
   * merge batches of either 64K records, or as many records as fit into the
   * amount of memory dedicated to each merge batch, whichever is less.
   */

  private void updateMergeSettings() {

    mergeBatchRowCount = rowsPerBatch(preferredMergeBatchSize);
    expectedMergeBatchSize = batchForRows(mergeBatchRowCount);

    // The merge memory pool assumes we can spill all input batches. The memory
    // available to hold spill batches for merging is total memory minus the
    // expected output batch size.

    mergeMemoryLimit = memoryLimit - expectedMergeBatchSize;
  }

  /**
   * In a low-memory situation we have to approach the memory assignment
   * problem from a different angle. Memory is low enough that we can't
   * fit the incoming batches (of a size decided by the upstream operator)
   * and our usual spill or merge batch sizes. Instead, we have to
   * determine the largest spill and merge batch sizes possible given
   * the available memory, input batch size and row width. We shrink the
   * sizes of the batches we control to try to make things fit into limited
   * memory. At some point, however, if we cannot fit even two input
   * batches and even the smallest merge match, then we will run into an
   * out-of-memory condition and we log a warning.
   * <p>
   * Note that these calculations are a bit crazy: it is Drill that
   * decided to allocate the small memory, it is Drill that created the
   * large incoming batches, and so it is Drill that created the low
   * memory situation. Over time, a better fix for this condition is to
   * control memory usage at the query level so that the sort is guaranteed
   * to have sufficient memory. But, since we don't yet have the luxury
   * of making such changes, we just live with the situation as we find
   * it.
   */

  private void adjustForLowMemory() {

    long loadHeadroom = bufferMemoryLimit - 2 * estimatedInputSize;
    long mergeHeadroom = mergeMemoryLimit - 2 * expectedSpillBatchSize;
    if (loadHeadroom >= 0  &&  mergeHeadroom >= 0) {
      return;
    }

    lowMemorySpillBatchSize();
    lowMemoryMergeBatchSize();

    // Sanity check: if we've been given too little memory to make progress,
    // issue a warning but proceed anyway. Should only occur if something is
    // configured terribly wrong.

    long minNeeds = 2 * estimatedInputSize + expectedSpillBatchSize;
    if (minNeeds > memoryLimit) {
      ExternalSortBatch.logger.warn("Potential memory overflow during load phase! " +
          "Minimum needed = {} bytes, actual available = {} bytes",
          minNeeds, memoryLimit);
      bufferMemoryLimit = 0;
      potentialOverflow = true;
    }

    // Sanity check

    minNeeds = 2 * expectedSpillBatchSize + expectedMergeBatchSize;
    if (minNeeds > memoryLimit) {
      ExternalSortBatch.logger.warn("Potential memory overflow during merge phase! " +
          "Minimum needed = {} bytes, actual available = {} bytes",
          minNeeds, memoryLimit);
      mergeMemoryLimit = 0;
      potentialOverflow = true;
    }
  }

  /**
   * If we are in a low-memory condition, then we might not have room for the
   * default spill batch size. In that case, pick a smaller size based on
   * the observation that we need two input batches and
   * one spill batch to make progress.
   */

  private void lowMemorySpillBatchSize() {

    // The "expected" size is with power-of-two rounding in some vectors.
    // We later work backwards to the row count assuming average internal
    // fragmentation.

    // Must hold two input batches. Use (most of) the rest for the spill batch.

    expectedSpillBatchSize = (int) (memoryLimit - 2 * estimatedInputSize);

    // But, in the merge phase, we need two spill batches and one output batch.
    // (Assume that the spill and merge are equal sizes.)
    // Use 3/4 of memory for each batch (to allow power-of-two rounding:

    expectedSpillBatchSize = (int) Math.min(expectedSpillBatchSize, memoryLimit/3);

    // Never going to happen, but let's ensure we don't somehow create large batches.

    expectedSpillBatchSize = Math.max(expectedSpillBatchSize, SortConfig.MIN_SPILL_BATCH_SIZE);

    // Must hold at least one row to spill. That is, we can make progress if we
    // create spill files that consist of single-record batches.

    expectedSpillBatchSize = Math.max(expectedSpillBatchSize, estimatedRowWidth);

    // Work out the spill batch count needed by the spill code. Allow room for
    // power-of-two rounding.

    spillBatchRowCount = rowsPerBatch(expectedSpillBatchSize);

    // Finally, figure out when we must spill.

    bufferMemoryLimit = memoryLimit - expectedSpillBatchSize;
  }

  /**
   * For merge batch, we must hold at least two spill batches and
   * one output batch.
   */

  private void lowMemoryMergeBatchSize() {
    expectedMergeBatchSize = (int) (memoryLimit - 2 * expectedSpillBatchSize);
    expectedMergeBatchSize = Math.max(expectedMergeBatchSize, SortConfig.MIN_MERGE_BATCH_SIZE);
    expectedMergeBatchSize = Math.max(expectedMergeBatchSize, estimatedRowWidth);
    mergeBatchRowCount = rowsPerBatch(expectedMergeBatchSize);
    mergeMemoryLimit = memoryLimit - expectedMergeBatchSize;
  }

  /**
   * Log the calculated values. Turn this on if things seem amiss.
   * Message will appear only when the values change.
   */

  private void logSettings(int actualRecordCount) {

    ExternalSortBatch.logger.debug("Input Batch Estimates: record size = {} bytes; input batch = {} bytes, {} records",
                 estimatedRowWidth, estimatedInputBatchSize, actualRecordCount);
    ExternalSortBatch.logger.debug("Merge batch size = {} bytes, {} records; spill file size: {} bytes",
                 expectedSpillBatchSize, spillBatchRowCount, config.spillFileSize());
    ExternalSortBatch.logger.debug("Output batch size = {} bytes, {} records",
                 expectedMergeBatchSize, mergeBatchRowCount);
    ExternalSortBatch.logger.debug("Available memory: {}, buffer memory = {}, merge memory = {}",
                 memoryLimit, bufferMemoryLimit, mergeMemoryLimit);
  }

  public enum MergeAction { SPILL, MERGE, NONE }

  public static class MergeTask {
    public MergeAction action;
    public int count;

    public MergeTask(MergeAction action, int count) {
      this.action = action;
      this.count = count;
    }
  }

  public MergeTask consolidateBatches(long allocMemory, int inMemCount, int spilledRunsCount) {

    // Determine additional memory needed to hold one batch from each
    // spilled run.

    // If the on-disk batches and in-memory batches need more memory than
    // is available, spill some in-memory batches.

    if (inMemCount > 0) {
      long mergeSize = spilledRunsCount * expectedSpillBatchSize;
      if (allocMemory + mergeSize > mergeMemoryLimit) {
        return new MergeTask(MergeAction.SPILL, 0);
      }
    }

    // Maximum batches that fit into available memory.

    int mergeLimit = (int) ((mergeMemoryLimit - allocMemory) / expectedSpillBatchSize);

    // Can't merge more than the merge limit.

    mergeLimit = Math.min(mergeLimit, config.mergeLimit());

    // How many batches to merge?

    int mergeCount = spilledRunsCount - mergeLimit;
    if (mergeCount <= 0) {
      return new MergeTask(MergeAction.NONE, 0);
    }

    // We will merge. This will create yet another spilled
    // run. Account for that.

    mergeCount += 1;

    // Must merge at least 2 batches to make progress.
    // This is the the (at least one) excess plus the allowance
    // above for the new one.

    // Can't merge more than the limit.

    mergeCount = Math.min(mergeCount, config.mergeLimit());

    // Do the merge, then loop to try again in case not
    // all the target batches spilled in one go.

    return new MergeTask(MergeAction.MERGE, mergeCount);
  }

  /**
   * Compute the number of rows per batch assuming that the batch is
   * subject to average internal fragmentation due to power-of-two
   * rounding on vectors.
   * <p>
   * <pre>[____|__$__]</pre>
   * In the above, the brackets represent the whole vector. The
   * first half is always full. When the first half filled, the second
   * half was allocated. On average, the second half will be half full.
   *
   * @param batchSize expected batch size, including internal fragmentation
   * @return number of rows that fit into the batch
   */

  private int rowsPerBatch(int batchSize) {
    int rowCount = batchSize * 3 / 4 / estimatedRowWidth;
    return Math.max(1, Math.min(rowCount, Character.MAX_VALUE));
  }

  /**
   * Compute the expected number of rows that fit into a given size
   * batch, accounting for internal fragmentation due to power-of-two
   * rounding on vector allocations.
   *
   * @param rowCount the desired number of rows in the batch
   * @return the size of resulting batch, including power-of-two
   * rounding.
   */

  private int batchForRows(int rowCount) {
    return estimatedRowWidth * rowCount * 4 / 3;
  }

  // Must spill if we are below the spill point (the amount of memory
  // needed to do the minimal spill.)

  public boolean isSpillNeeded(long allocatedBytes, int incomingSize) {
    return allocatedBytes + incomingSize >= bufferMemoryLimit;
  }

  public boolean hasMemoryMergeCapacity(long allocatedBytes, long neededForInMemorySort) {
    return (freeMemory(allocatedBytes) >= neededForInMemorySort);
  }

  public long freeMemory(long allocatedBytes) {
    return memoryLimit - allocatedBytes;
  }

  public long getMergeMemoryLimit() { return mergeMemoryLimit; }
  public int getSpillBatchRowCount() { return spillBatchRowCount; }
  public int getMergeBatchRowCount() { return mergeBatchRowCount; }

  // Primarily for testing

  @VisibleForTesting
  public long getMemoryLimit() { return memoryLimit; }
  @VisibleForTesting
  public int getRowWidth() { return estimatedRowWidth; }
  @VisibleForTesting
  public int getInputBatchSize() { return estimatedInputBatchSize; }
  @VisibleForTesting
  public int getPreferredSpillBatchSize() { return preferredSpillBatchSize; }
  @VisibleForTesting
  public int getPreferredMergeBatchSize() { return preferredMergeBatchSize; }
  @VisibleForTesting
  public int getSpillBatchSize() { return expectedSpillBatchSize; }
  @VisibleForTesting
  public int getMergeBatchSize() { return expectedMergeBatchSize; }
  @VisibleForTesting
  public long getBufferMemoryLimit() { return bufferMemoryLimit; }
  @VisibleForTesting
  public boolean mayOverflow() { return potentialOverflow; }
}
