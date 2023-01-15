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
package org.apache.drill.exec.physical.impl.setop;

import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.physical.impl.join.HashJoinHelper;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;

import static org.apache.drill.exec.record.JoinBatchMemoryManager.LEFT_INDEX;

public class HashSetOpProbeTemplate implements HashSetOpProbe {

  VectorContainer container; // the outgoing container

  // Probe side record batch
  private RecordBatch probeBatch;

  private BatchSchema probeSchema;

  private SqlKind opType;

  private HashSetOpRecordBatch outgoingSetOpBatch;

  // Number of records to process on the probe side
  private int recordsToProcess;

  // Number of records processed on the probe side
  private int recordsProcessed;

  // Number of records in the output container
  private int outputRecords;

  // Current state the hash set op algorithm is in
  private ProbeState probeState = ProbeState.PROBE_PROJECT;

  private  HashPartition partitions[];

  // While probing duplicates, retain current build-side partition in case need to continue
  // probing later on the same chain of duplicates
  private HashPartition currPartition;
  IntVector read_left_HV_vector; // HV vector that was read from the spilled batch
  private int cycleNum; // 1-primary, 2-secondary, 3-tertiary, etc.
  private HashSetOpRecordBatch.HashSetOpSpilledPartition spilledInners[]; // for the outer to find the partition
  private boolean buildSideIsEmpty = true;
  private int partitionMask; // numPartitions - 1
  private int bitsInMask; // number of bits in the MASK
  private int numberOfBuildSideColumns;
  private int targetOutputRecords;
  private boolean isAll;

  @Override
  public void setTargetOutputCount(int targetOutputRecords) {
    this.targetOutputRecords = targetOutputRecords;
  }

  @Override
  public int getOutputCount() {
    return outputRecords;
  }

  /**
   *  Setup the Hash Set Op Probe object
   *
   * @param probeBatch
   * @param outgoing
   * @param opType
   * @param isAll
   * @param leftStartState
   * @param partitions
   * @param cycleNum
   * @param container
   * @param spilledInners
   * @param buildSideIsEmpty
   * @param numPartitions
   * @param rightHVColPosition
   */
  @Override
  public void setupHashSetOpProbe(RecordBatch probeBatch, HashSetOpRecordBatch outgoing, SqlKind opType, boolean isAll,
                                 IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                                 VectorContainer container, HashSetOpRecordBatch.HashSetOpSpilledPartition[] spilledInners,
                                 boolean buildSideIsEmpty, int numPartitions, int rightHVColPosition) throws SchemaChangeException {
    this.container = container;
    this.spilledInners = spilledInners;
    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    this.opType = opType;
    this.outgoingSetOpBatch = outgoing;
    this.partitions = partitions;
    this.cycleNum = cycleNum;
    this.buildSideIsEmpty = buildSideIsEmpty;
    this.numberOfBuildSideColumns = 0; // position (0 based) of added column == #columns
    this.isAll = isAll;

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
      // update first probe batch as it may not available before prefetchFirstProbeBatch
      // (ex. agg record batch)
      partn.updateBatches();
    }

    // Initialize the HV vector for the first (already read) left batch
    if (this.cycleNum > 0) {
      if (read_left_HV_vector != null) { read_left_HV_vector.clear();}
      if (leftStartState != IterOutcome.NONE) { // Skip when outer spill was empty
        read_left_HV_vector = (IntVector) probeBatch.getContainer().getLast();
      }
    }
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
   *  It copies the build and probe sides into the outgoing container. (It uses a composite
   *  index for the build side). If any of the build/probe source containers is null, then that side
   *  is not appended (effectively outputing nulls for that side's columns).
   * @param buildSrcContainers The containers list for the right/inner side
   * @param compositeBuildSrcIndex Composite build index
   * @param probeSrcContainer The single container for the left/outer side
   * @param probeSrcIndex Index in the outer container
   * @return Number of rows in this container (after the append)
   */
  private int outputRow(ArrayList<VectorContainer> buildSrcContainers, int compositeBuildSrcIndex,
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

  private void executeProbePhase() throws SchemaChangeException {

    while (outputRecords < targetOutputRecords && probeState != ProbeState.DONE) {

      // Check if we have processed all records in this batch we need to invoke next
      if (recordsProcessed == recordsToProcess) {

        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }

        IterOutcome leftUpstream = outgoingSetOpBatch.next(HashJoinHelper.LEFT_INPUT, probeBatch);

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
              HashSetOpRecordBatch.HashSetOpSpilledPartition sp = spilledInners[partn.getPartitionNum()];
              sp.updateOuter(partn.getPartitionBatchesCount(), partn.getSpillFile());

              partn.closeWriter();
            }

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              for (HashPartition partn : partitions) { partn.updateBatches(); }

            } else {
              throw SchemaChangeException.schemaChanged("Hash SetOp does not support schema changes in probe side.",
                probeSchema,
                probeBatch.getSchema());
            }
          case OK:
            outgoingSetOpBatch.getBatchMemoryManager().update(probeBatch, LEFT_INDEX,outputRecords);
            setTargetOutputCount(outgoingSetOpBatch.getBatchMemoryManager().getCurrentOutgoingMaxRowCount()); // calculated by update()
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
        if (outgoingSetOpBatch.isSpilledInner(currBuildPart)) {
          // add this row to its outer partition (may cause a spill, when the batch is full)

          currPartition.appendOuterRow(hashCode, recordsProcessed);

          recordsProcessed++; // done with this outer record
          continue; // on to the next outer record
        }

        probeIndex = currPartition.probeForKey(recordsProcessed, hashCode);
      }

      switch (opType) {
        case INTERSECT:
          if (probeIndex != -1 && currPartition.getRecordNumForKey(probeIndex) > 0) {
            if (isAll) {
              currPartition.decreaseRecordNumForKey(probeIndex);
            } else {
              currPartition.setRecordNumForKey(probeIndex, 0);
            }
            outputRecords =
              outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
          }
          break;
        case EXCEPT:
          if (isAll) {
            if (probeIndex == -1 || currPartition.getRecordNumForKey(probeIndex) == 0) {
              outputRecords =
                outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
            } else {
              currPartition.decreaseRecordNumForKey(probeIndex);
            }
          } else if (probeIndex == -1) {
            outputRecords =
              outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
          }
        default:
          break;
      }
      recordsProcessed++;
    }
  }

  /**
   *  Perform the probe, till the outgoing is full, or no more rows to probe.
   * @return Num of output records
   * @throws SchemaChangeException
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

    return outputRecords;
  }

  @Override
  public void changeToFinalProbeState() {
    probeState = ProbeState.DONE;
  }

  @Override
  public String toString() {
    return "HashSetOpProbeTemplate[container=" + container
        + ", probeSchema=" + probeSchema
        + ", opType=" + opType
        + ", recordsToProcess=" + recordsToProcess
        + ", recordsProcessed=" + recordsProcessed
        + ", outputRecords=" + outputRecords
        + ", probeState=" + probeState
        + "]";
  }
}
