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

import com.carrotsearch.hppc.IntArrayList;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;

import static org.apache.drill.exec.record.JoinBatchMemoryManager.LEFT_INDEX;

public abstract class ProbeTemplate<T extends PhysicalOperator> implements Probe {

  protected VectorContainer container; // the outgoing container

  // Probe side record batch
  protected RecordBatch probeBatch;

  protected BatchSchema probeSchema;

  // Number of records to process on the probe side
  protected int recordsToProcess;

  // Number of records processed on the probe side
  protected int recordsProcessed;

  // Number of records in the output container
  protected int outputRecords;

  // Indicate if we should drain the next record from the probe side
  protected boolean getNextRecord = true;

  // Contains both batch idx and record idx of the matching record in the build side
  protected int currentCompositeIdx = -1;

  // Current state the hash join algorithm is in
  protected ProbeState probeState = ProbeState.PROBE_PROJECT;

  // For outer or right joins, this is a list of unmatched records that needs to be projected
  protected IntArrayList unmatchedBuildIndexes;

  protected HashPartition[] partitions;

  // While probing duplicates, retain current build-side partition in case need to continue
  // probing later on the same chain of duplicates
  protected HashPartition currPartition;
  protected int currRightPartition; // for returning RIGHT/FULL
  IntVector read_left_HV_vector; // HV vector that was read from the spilled batch
  protected int cycleNum; // 1-primary, 2-secondary, 3-tertiary, etc.
  protected AbstractHashBinaryRecordBatch.SpilledPartition[] spilledInners; // for the outer to find the partition
  protected boolean buildSideIsEmpty = true;
  protected int numPartitions = 1; // must be 2 to the power of bitsInMask
  protected int partitionMask; // numPartitions - 1
  protected int bitsInMask; // number of bits in the MASK
  protected int numberOfBuildSideColumns;
  protected int targetOutputRecords;
  protected AbstractHashBinaryRecordBatch<T> outgoingBatch;

  protected void setup(RecordBatch probeBatch, IterOutcome leftStartState,
    HashPartition[] partitions, int cycleNum,
    VectorContainer container, AbstractHashBinaryRecordBatch.SpilledPartition[] spilledInners,
    boolean buildSideIsEmpty, int numPartitions) throws SchemaChangeException {
    this.container = container;
    this.spilledInners = spilledInners;
    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    this.partitions = partitions;
    this.cycleNum = cycleNum;
    this.buildSideIsEmpty = buildSideIsEmpty;
    this.numPartitions = numPartitions;

    partitionMask = numPartitions - 1; // e.g. 32 --> 0x1F
    bitsInMask = Integer.bitCount(partitionMask); // e.g. 0x1F -> 5

    probeState = ProbeState.PROBE_PROJECT;
    this.recordsToProcess = 0;
    this.recordsProcessed = 0;

    // A special case - if the left was an empty file
    if (leftStartState == IterOutcome.NONE){
      changeToFinalProbeState();
    } else {
      this.recordsToProcess = probeBatch.getRecordCount();
    }

    // for those outer partitions that need spilling (cause their matching inners spilled)
    // initialize those partitions' current batches and hash-value vectors
    for (HashPartition partn : this.partitions) {
      partn.allocateNewCurrentBatchAndHV();
    }

    // Container is cleared after prefetchFirstProbeBatch in case of AggRecordBatch,
    // only the partition with batch in it will update the reference to vv in the hashtable after that.
    // Update the first partition here if not yet, it will be used when get hash code.
    if (this.cycleNum == 0 && partitions.length > 0 && partitions[0].getPartitionBatchesCount() == 0) {
      partitions[0].updateBatches();
    }

    currRightPartition = 0; // In case it's a Right/Full outer join

    // Initialize the HV vector for the first (already read) left batch
    if (this.cycleNum > 0) {
      if (read_left_HV_vector != null) { read_left_HV_vector.clear();}
      if (leftStartState != IterOutcome.NONE) { // Skip when outer spill was empty
        read_left_HV_vector = (IntVector) probeBatch.getContainer().getLast();
      }
    }
  }

  @Override
  public void setTargetOutputCount(int targetOutputRecords) {
    this.targetOutputRecords = targetOutputRecords;
  }

  @Override
  public int getOutputCount() {
    return outputRecords;
  }

  /**
   * Append the given build side row into the outgoing container
   * @param buildSrcContainer The container for the right/inner side
   * @param buildSrcIndex build side index
   */
  private void appendBuild(VectorContainer buildSrcContainer, int buildSrcIndex) {
    for (int vectorIndex = 0; vectorIndex < numberOfBuildSideColumns; vectorIndex++) {
      ValueVector destVector = container.getValueVector(vectorIndex).getValueVector();
      ValueVector srcVector = buildSrcContainer.getValueVector(vectorIndex).getValueVector();
      destVector.copyEntry(container.getRecordCount(), srcVector, buildSrcIndex);
    }
  }

  /**
   * Append the given probe side row into the outgoing container, following the build side part
   * @param probeSrcContainer The container for the left/outer side
   * @param probeSrcIndex probe side index
   */
  private void appendProbe(VectorContainer probeSrcContainer, int probeSrcIndex) {
    for (int vectorIndex = numberOfBuildSideColumns; vectorIndex < container.getNumberOfColumns(); vectorIndex++) {
      ValueVector destVector = container.getValueVector(vectorIndex).getValueVector();
      ValueVector srcVector = probeSrcContainer.getValueVector(vectorIndex - numberOfBuildSideColumns).getValueVector();
      destVector.copyEntry(container.getRecordCount(), srcVector, probeSrcIndex);
    }
  }

  /**
   *  A special version of the VectorContainer's appendRow for the HashJoin; (following a probe) it
   *  copies the build and probe sides into the outgoing container. (It uses a composite
   *  index for the build side). If any of the build/probe source containers is null, then that side
   *  is not appended (effectively outputing nulls for that side's columns).
   * @param buildSrcContainers The containers list for the right/inner side
   * @param compositeBuildSrcIndex Composite build index
   * @param probeSrcContainer The single container for the left/outer side
   * @param probeSrcIndex Index in the outer container
   * @return Number of rows in this container (after the append)
   */
  protected int outputRow(ArrayList<VectorContainer> buildSrcContainers, int compositeBuildSrcIndex,
                        VectorContainer probeSrcContainer, int probeSrcIndex) {

    if (buildSrcContainers != null) {
      int buildBatchIndex = compositeBuildSrcIndex >>> 16;
      int buildOffset = compositeBuildSrcIndex & 65535;
      appendBuild(buildSrcContainers.get(buildBatchIndex), buildOffset);
    }
    if (probeSrcContainer != null) {
      appendProbe(probeSrcContainer, probeSrcIndex);
    }
    return container.incRecordCount();
  }

  /**
   * After the "inner" probe phase, finish up a Right (of Full) Join by projecting the unmatched rows of the build side
   * @param currBuildPart Which partition
   */
  protected void executeProjectRightPhase(int currBuildPart) {
    while (outputRecords < targetOutputRecords && recordsProcessed < recordsToProcess) {
      outputRecords =
        outputRow(partitions[currBuildPart].getContainers(), unmatchedBuildIndexes.get(recordsProcessed),
          null /* no probeBatch */, 0 /* no probe index */);
      recordsProcessed++;
    }
  }

  private void executeProbePhase() throws SchemaChangeException {

    while (outputRecords < targetOutputRecords && probeState != ProbeState.DONE && probeState != ProbeState.PROJECT_RIGHT) {

      // Check if we have processed all records in this batch we need to invoke next
      if (recordsProcessed == recordsToProcess) {

        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }

        IterOutcome leftUpstream = outgoingBatch.next(HashJoinHelper.LEFT_INPUT, probeBatch);

        switch (leftUpstream) {
          case NONE:
          case NOT_YET:
            recordsProcessed = 0;
            recordsToProcess = 0;
            changeToFinalProbeState();
            // in case some outer partitions were spilled, need to spill their last batches
            for (HashPartition partn : partitions) {
              if (! partn.isSpilled()) { continue; } // skip non-spilled
              partn.completeAnOuterBatch(false);
              // update the partition's spill record with the outer side
              AbstractHashBinaryRecordBatch.SpilledPartition sp = spilledInners[partn.getPartitionNum()];
              sp.updateOuter(partn.getPartitionBatchesCount(), partn.getSpillFile());

              partn.closeWriter();
            }

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              for (HashPartition partn : partitions) { partn.updateBatches(); }

            } else {
              throw SchemaChangeException.schemaChanged(
                this.getClass().getSimpleName() + " does not support schema changes in probe side.",
                probeSchema, probeBatch.getSchema());
            }
          case OK:
            outgoingBatch.getBatchMemoryManager().update(probeBatch, LEFT_INDEX,outputRecords);
            setTargetOutputCount(outgoingBatch.getBatchMemoryManager().getCurrentOutgoingMaxRowCount()); // calculated by update()
            recordsToProcess = probeBatch.getRecordCount();
            recordsProcessed = 0;
            // If we received an empty batch do nothing
            if (recordsToProcess == 0) {
              continue;
            }
            if (cycleNum > 0) {
              read_left_HV_vector = (IntVector) probeBatch.getContainer().getLast(); // Needed ?
            }
            break;
          default:
        }
      }

      int probeIndex = -1;
      // Check if we need to drain the next row in the probe side
      if (getNextRecord) {
        if (!buildSideIsEmpty) {
          int hashCode = (cycleNum == 0) ?
            partitions[0].getProbeHashCode(recordsProcessed)
            : read_left_HV_vector.getAccessor().get(recordsProcessed);
          int currBuildPart = hashCode & partitionMask;
          hashCode >>>= bitsInMask;

          // Set and keep the current partition (may be used again on subsequent probe calls as
          // inner rows of duplicate key are processed)
          currPartition = partitions[currBuildPart]; // inner if not spilled, else outer

          // If the matching inner partition was spilled
          if (outgoingBatch.isSpilledInner(currBuildPart)) {
            // add this row to its outer partition (may cause a spill, when the batch is full)

            currPartition.appendOuterRow(hashCode, recordsProcessed);

            recordsProcessed++; // done with this outer record
            continue; // on to the next outer record
          }

          probeIndex = currPartition.probeForKey(recordsProcessed, hashCode);

        }

        handleProbeResult(probeIndex);
      } else { // match the next inner row with the same key

        currPartition.setRecordMatched(currentCompositeIdx);

        outputRecords =
          outputRow(currPartition.getContainers(), currentCompositeIdx,
            probeBatch.getContainer(), recordsProcessed);

        currentCompositeIdx = currPartition.getNextIndex(currentCompositeIdx);

        if (currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          getNextRecord = true;
          recordsProcessed++;
        }
      }
    }
  }

  /**
   *  Perform the probe, till the outgoing is full, or no more rows to probe.
   *  Performs the inner or left-outer join while there are left rows,
   *  when done, continue with right-outer, if appropriate.
   * @return Num of output records
   * @throws SchemaChangeException SchemaChangeException
   */
  @Override
  public int probeAndProject() throws SchemaChangeException {

    outputRecords = 0;

    // When handling spilled partitions, the state becomes DONE at the end of each partition
    if (probeState == ProbeState.DONE) {
      return outputRecords; // that is zero
    }

    if (probeState == ProbeState.PROBE_PROJECT) {
      executeProbePhase();
    }

    if (probeState == ProbeState.PROJECT_RIGHT) {
      // Inner probe is done; now we are here because we still have a RIGHT OUTER (or a FULL) join
      do {
        if (unmatchedBuildIndexes == null) { // first time for this partition ?
          if (buildSideIsEmpty) { return outputRecords; } // in case of an empty right
          // Get this partition's list of build indexes that didn't match any record on the probe side
          unmatchedBuildIndexes = partitions[currRightPartition].getNextUnmatchedIndex();
          recordsProcessed = 0;
          recordsToProcess = unmatchedBuildIndexes.size();
        }

        // Project the list of unmatched records on the build side
        executeProjectRightPhase(currRightPartition);

        if (recordsProcessed < recordsToProcess) { // more records in this partition?
          return outputRecords;  // outgoing is full; report and come back later
        } else {
          currRightPartition++; // on to the next right partition
          unmatchedBuildIndexes = null;
        }

      } while (currRightPartition < numPartitions);

      probeState = ProbeState.DONE; // last right partition was handled; we are done now
    }
    return outputRecords;
  }

  protected abstract void handleProbeResult(int probeIndex);
}
