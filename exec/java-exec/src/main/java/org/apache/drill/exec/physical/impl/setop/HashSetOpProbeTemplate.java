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
import org.apache.drill.exec.physical.config.SetOp;
import org.apache.drill.exec.physical.impl.common.HashPartition;
import org.apache.drill.exec.physical.impl.join.AbstractHashBinaryRecordBatch;
import org.apache.drill.exec.physical.impl.join.ProbeTemplate;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;

public class HashSetOpProbeTemplate extends ProbeTemplate<SetOp> {
  private SqlKind opType;
  private boolean isAll;

  /**
   *  Setup the Hash Set Op Probe object
   */
  @Override
  public void setup(RecordBatch probeBatch, HashSetOpRecordBatch outgoing, SqlKind opType, boolean isAll,
                                 IterOutcome leftStartState, HashPartition[] partitions, int cycleNum,
                                 VectorContainer container, AbstractHashBinaryRecordBatch.SpilledPartition[] spilledInners,
                                 boolean buildSideIsEmpty, int numPartitions) throws SchemaChangeException {
    super.setup(probeBatch, leftStartState, partitions, cycleNum, container, spilledInners,
      buildSideIsEmpty, numPartitions);
    this.outgoingBatch = outgoing;
    this.opType = opType;
    this.isAll = isAll;
    this.numberOfBuildSideColumns = 0;
  }

  @Override
  protected void handleProbeResult(int probeIndex) {
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

  @Override
  public void changeToFinalProbeState() {
    probeState = ProbeState.DONE;
  }

  @Override
  public String toString() {
    return "HashSetOpProbeTemplate[container=" + container
        + ", probeSchema=" + probeSchema
        + ", opType=" + opType
        + ", isAll=" + isAll
        + ", recordsToProcess=" + recordsToProcess
        + ", recordsProcessed=" + recordsProcessed
        + ", outputRecords=" + outputRecords
        + ", probeState=" + probeState
        + "]";
  }
}
