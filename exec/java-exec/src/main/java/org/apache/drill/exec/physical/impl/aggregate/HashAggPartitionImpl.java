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

import org.apache.drill.common.exceptions.RetryAfterSpillException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.physical.impl.common.SpilledState;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.util.record.RecordBatchStats;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HashAggPartitionImpl implements HashAggPartition {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggregator.class);

  private final int partitionIndex;
  private final RecordBatch outgoing;
  private final VectorContainer outContainer;
  private final SpilledState<HashAggSpilledPartition> spilledState;
  private final SpillSet spillSet;
  private final HashAggMemoryCalculator memoryCalculator;
  private final HashAggBatchAllocator batchAllocator;
  private final RecordBatchMemoryManager recordBatchMemoryManager;
  private final RecordBatchStats.RecordBatchStatsContext statsContext;

  private HashTable hashTable = null;
  private List<HashAggBatchHolder> batchHolders = new ArrayList<>();
  private int outBatchIndex = 0;
  private VectorSerializer.Writer writer = null;
  private int spilledBatchesCount = 0;
  private String spillFile = null;

  public HashAggPartitionImpl(final ChainedHashTable baseHashTable,
                              final TypedFieldId[] groupByOutFieldIds,
                              final int partitionIndex,
                              final RecordBatch outgoing,
                              final VectorContainer outContainer,
                              final SpilledState<HashAggSpilledPartition> spilledState,
                              final SpillSet spillSet,
                              final HashAggMemoryCalculator memoryCalculator,
                              final HashAggBatchAllocator batchAllocator,
                              final RecordBatchMemoryManager recordBatchMemoryManager,
                              final RecordBatchStats.RecordBatchStatsContext statsContext) {
    this.partitionIndex = partitionIndex;
    this.outgoing = Preconditions.checkNotNull(outgoing);
    this.outContainer = Preconditions.checkNotNull(outContainer);
    this.spilledState = Preconditions.checkNotNull(spilledState);
    this.spillSet = Preconditions.checkNotNull(spillSet);
    this.memoryCalculator = Preconditions.checkNotNull(memoryCalculator);
    this.batchAllocator = Preconditions.checkNotNull(batchAllocator);
    this.recordBatchMemoryManager = Preconditions.checkNotNull(recordBatchMemoryManager);
    this.statsContext = Preconditions.checkNotNull(statsContext);

    try {
      this.hashTable = baseHashTable.createAndSetupHashTable(groupByOutFieldIds);
    } catch (ClassTransformationException e) {
      throw UserException.unsupportedError(e)
        .message("Code generation error - likely an error in the code.")
        .build(logger);
    } catch (IOException e) {
      throw UserException.resourceError(e)
        .message("IO Error while creating a hash table.")
        .build(logger);
    } catch (SchemaChangeException sce) {
      throw new IllegalStateException("Unexpected Schema Change while creating a hash table",sce);
    }
  }

  public void setup(final RecordBatch newIncoming) {
    hashTable.updateIncoming(newIncoming.getContainer(), null);
    hashTable.reset();

    for (HashAggBatchHolder bh : batchHolders) {
      bh.clear();
    }
    batchHolders.clear();

    outBatchIndex = 0;
    writer = null;
    spilledBatchesCount = 0;
    spillFile = null;
  }

  @Override
  public boolean isSpilled() {
    return writer != null;
  }

  @Override
  public int getBatchHolderCount() {
    return batchHolders.size();
  }

  @Override
  public int getSpilledBatchesCount() {
    return spilledBatchesCount;
  }

  @Override
  public int getPartitionIndex() {
    return partitionIndex;
  }

  @Override
  public boolean doneOutputting() {
    return outBatchIndex == getBatchHolderCount();
  }

  @Override
  public int outputCurrentBatch() {
    // get the number of records in the batch holder that are pending output
    final HashAggBatchHolder currentBatchHolder = batchHolders.get(outBatchIndex);
    int numPendingOutput = currentBatchHolder.getNumPendingOutput();

    batchAllocator.allocateOutgoing(numPendingOutput);

    currentBatchHolder.outputValues();
    int numOutputRecords = numPendingOutput;
    this.hashTable.outputKeys(outBatchIndex, this.outContainer, numPendingOutput);

    // set the value count for outgoing batch value vectors
    for (VectorWrapper<?> v : outgoing) {
      v.getValueVector().getMutator().setValueCount(numOutputRecords);
    }

    recordBatchMemoryManager.updateOutgoingStats(numOutputRecords);
    RecordBatchStats.logRecordBatchStats(RecordBatchStats.RecordBatchIOType.OUTPUT, outgoing, statsContext);

    outBatchIndex++;
    return numPendingOutput;
  }

  @Override
  public void spill() {
    if (logger.isDebugEnabled()) { // TODO use marker
      logger.debug("HashAggregate: Spilling partition {} current cycle {} part size {}", partitionIndex, spilledState.getCycle(), getBatchHolderCount());
    }

    if (getBatchHolderCount() == 0) {
      // in case empty - nothing to spill
      return;
    }

    if (!isSpilled()) {
      // If this is the first spill for this partition, create an output stream
      spillFile = spillSet.getNextSpillFile(spilledState.getCycle() > 0 ? Integer.toString(spilledState.getCycle()) : null);

      try {
        writer = spillSet.writer(spillFile);
      } catch (IOException ioe) {
        throw UserException.resourceError(ioe).message("Hash Aggregation failed to open spill file: " + spillFile).build(logger);
      }
    }

    for (int holderIndex = 0; holderIndex < batchHolders.size(); holderIndex++) {
      final HashAggBatchHolder batchHolder = batchHolders.get(holderIndex);
      int numOutputRecords = batchHolder.getNumPendingOutput();

      batchAllocator.allocateOutgoing(numOutputRecords);
      batchHolder.outputValues();
      this.hashTable.outputKeys(holderIndex, this.outContainer, numOutputRecords);

      // set the value count for outgoing batch value vectors
      for (VectorWrapper<?> v : outgoing) {
        v.getValueVector().getMutator().setValueCount(numOutputRecords);
      }

      outContainer.setRecordCount(numOutputRecords);
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(numOutputRecords, outContainer, false);
      try {
        writer.write(batch, null);
      } catch (IOException ioe) {
        throw UserException.dataWriteError(ioe).message("Hash Aggregation failed to write to output file: " + spillFile).build(logger);
      } finally {
        batch.clear();
      }
      outContainer.zeroVectors();
      logger.trace("HASH AGG: Took {} us to spill {} records", writer.time(TimeUnit.MICROSECONDS), numOutputRecords);
    }

    spilledBatchesCount += getBatchHolderCount(); // update count of spilled batches
  }

  @Override
  public HashAggSpilledPartition finishSpilling(final int originalPartition) {
    spill();

    HashAggSpilledPartition sp = new HashAggSpilledPartition(
      spilledState.getCycle(),
      partitionIndex,
      originalPartition,
      getSpilledBatchesCount(),
      spillFile);

    spilledState.addPartition(sp);

    reinitPartition(); // free the memory

    try {
      spillSet.close(writer);
    } catch (IOException ioe) {
      throw UserException.resourceError(ioe)
        .message("IO Error while closing output stream")
        .build(logger);
    }

    return sp;
  }

  @Override
  public void addStats(HashTableStats aggregateStats) {
    HashTableStats newStats = new HashTableStats();
    hashTable.getStats(newStats);
    aggregateStats.addStats(newStats);
  }

  @Override
  public void cleanup() {
    if (hashTable != null) {
      hashTable.clear();
    }

    for (HashAggBatchHolder bh: batchHolders) {
      bh.clear();
    }

    batchHolders.clear();

    // delete any (still active) output spill file
    if ( writer != null && spillFile != null) {
      try {
        spillSet.close(writer);
        spillSet.delete(spillFile);
      } catch(IOException e) {
        logger.warn("Cleanup: Failed to delete spill file {}", spillFile, e);
      }
    }
  }

  // First free the memory used by the given (spilled) partition (i.e., hash table plus batches)
  // then reallocate them in pristine state to allow the partition to continue receiving rows
  public void reinitPartition() {
    hashTable.reset();

    for (HashAggBatchHolder bh : batchHolders) {
      bh.clear();
    }

    batchHolders.clear();
    outBatchIndex = 0;
    // in case the reserve memory was used, try to restore
    memoryCalculator.restoreReservedMemory();
  }

  @Override
  public boolean hasPendingRows() {
    return outBatchIndex < getBatchHolderCount() && batchHolders.get(outBatchIndex).getNumPendingOutput() > 0;
  }

  @Override
  public void addBatchHolder(HashAggBatchHolder batchHolder) {
    batchHolders.add(batchHolder);

    if (logger.isDebugEnabled()) {
      logger.debug("HashAggregate: Added new batch; num batches = {}.", batchHolders.size());
    }

    batchHolder.setup();
  }

  @Override
  public int buildHashcode(int incomingRowIdx) throws SchemaChangeException {
    return hashTable.getBuildHashCode(incomingRowIdx);
  }

  @Override
  public HashAggBatchHolder getBatchHolder(int index) {
    return batchHolders.get(index);
  }

  public HashTable.PutStatus put(int incomingRowIdx, IndexPointer htIdxHolder, int hashCode, int batchSize) throws SchemaChangeException, RetryAfterSpillException {
    return hashTable.put(incomingRowIdx, htIdxHolder, hashCode, batchSize);
  }

  @Override
  public void resetOutBatchIndex() {
    outBatchIndex = 0;
  }

  @Override
  public void updateBatches() {
    try {
      hashTable.updateBatches();
    } catch (SchemaChangeException sc) {
      throw new UnsupportedOperationException(sc);
    }
  }
}
