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

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.planner.common.JoinControl;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;

public class HashJoinProbeTemplate extends ProbeTemplate<HashJoinPOP> {
  // Join type, INNER, LEFT, RIGHT or OUTER
  private JoinRelType joinType;
  // joinControl determines how to handle INTERSECT_DISTINCT vs. INTERSECT_ALL
  private JoinControl joinControl;
  private boolean semiJoin;

  /**
   *  Setup the Hash Join Probe object
   */
  @Override
  public void setup(RecordBatch probeBatch, HashJoinBatch outgoing, JoinRelType joinRelType, boolean semiJoin,
                                 IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                                 VectorContainer container, AbstractHashBinaryRecordBatch.SpilledPartition[] spilledInners,
                                 boolean buildSideIsEmpty, int numPartitions, int rightHVColPosition) throws SchemaChangeException {
    super.setup(probeBatch, leftStartState, partitions, cycleNum, container, spilledInners,
      buildSideIsEmpty, numPartitions);
    this.outgoingBatch = outgoing;
    this.joinType = joinRelType;
    this.joinControl = new JoinControl(outgoing.getPopConfig().getJoinControl());
    this.semiJoin = semiJoin;
    this.numberOfBuildSideColumns = semiJoin ? 0 : rightHVColPosition; // position (0 based) of added column == #columns
  }

  @Override
  protected void handleProbeResult(int probeIndex) {
    if (semiJoin) {
      if (probeIndex != -1) {
        // output the probe side only
        outputRecords =
          outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
      }
      recordsProcessed++;
      return; // no build-side duplicates, go on to the next probe-side row
    }

    if (probeIndex != -1) {
      /* The current probe record has a key that matches. Get the index
       * of the first row in the build side that matches the current key
       * (and record this match in the bitmap, in case of a FULL/RIGHT join)
       */
      Pair<Integer, Boolean> matchStatus = currPartition.getStartIndex(probeIndex);

      boolean matchExists = matchStatus.getRight();

      if (joinControl.isIntersectDistinct() && matchExists) {
        // since it is intersect distinct and we already have one record matched, move to next probe row
        recordsProcessed++;
        return;
      }

      currentCompositeIdx = matchStatus.getLeft();

      outputRecords =
        outputRow(currPartition.getContainers(), currentCompositeIdx,
          probeBatch.getContainer(), recordsProcessed);

      /* Projected single row from the build side with matching key but there
       * may be more rows with the same key. Check if that's the case as long as
       * we are not doing intersect distinct since it only cares about
       * distinct values.
       */
      currentCompositeIdx = joinControl.isIntersectDistinct() ? -1 :
        currPartition.getNextIndex(currentCompositeIdx);

      if (currentCompositeIdx == -1) {
        /* We only had one row in the build side that matched the current key
         * from the probe side. Drain the next row in the probe side.
         */
        recordsProcessed++;
      } else {
        /* There is more than one row with the same key on the build side
         * don't drain more records from the probe side till we have projected
         * all the rows with this key
         */
        getNextRecord = false;
      }
    } else { // No matching key

      // If we have a left outer join, project the outer side
      if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {

        outputRecords = // output only the probe side (the build side would be all nulls)
          outputRow(null, 0, probeBatch.getContainer(), recordsProcessed);
      }
      recordsProcessed++;
    }
  }

  @Override
  public void changeToFinalProbeState() {
    // We are done with the (left) probe phase.
    // If it's a RIGHT or a FULL join then need to get the unmatched indexes from the build side
    probeState =
      (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) ? ProbeState.PROJECT_RIGHT :
        ProbeState.DONE; // else we're done
  }

  @Override
  public String toString() {
    return "HashJoinProbeTemplate[container=" + container
        + ", probeSchema=" + probeSchema
        + ", joinType=" + joinType
        + ", recordsToProcess=" + recordsToProcess
        + ", recordsProcessed=" + recordsProcessed
        + ", outputRecords=" + outputRecords
        + ", probeState=" + probeState
        + ", unmatchedBuildIndexes=" + unmatchedBuildIndexes
        + "]";
  }
}
