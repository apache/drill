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
package org.apache.drill.exec.physical.impl.join;

import java.io.IOException;

import javax.inject.Named;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.calcite.rel.core.JoinRelType;

public abstract class HashJoinProbeTemplate implements HashJoinProbe {

  // Probe side record batch
  private RecordBatch probeBatch;

  private BatchSchema probeSchema;

  private VectorContainer buildBatch;

  // Join type, INNER, LEFT, RIGHT or OUTER
  private JoinRelType joinType;

  private HashJoinBatch outgoingJoinBatch = null;

  private static final int TARGET_RECORDS_PER_BATCH = 4000;

  /* Helper class
   * Maintains linked list of build side records with the same key
   * Keeps information about which build records have a corresponding
   * matching key in the probe side (for outer, right joins)
   */
  private HashJoinHelper hjHelper = null;

  // Underlying hashtable used by the hash join
  private HashTable hashTable = null;

  // Number of records in the output container
  private int outputRecords;

  private boolean unionTypeEnabled;

  // If schema changes during probe phase stop probe phase.
  private  boolean schemaChanged = false;

  private OperatorContext oContext;

  // to hold coerced left side schema and container.
  private VectorContainer coercedContainer = null;
  private BatchSchema coercedSchema = null;

  private HashJoinProbeStatus probeStatus;

  @Override
  public void setupHashJoinProbe(FragmentContext context, VectorContainer buildBatch, RecordBatch probeBatch,
                                 int probeRecordCount, HashJoinBatch outgoing, HashTable hashTable,
                                 HashJoinHelper hjHelper, JoinRelType joinRelType,
                                 HashJoinProbeStatus probeStatus,
                                 boolean unionTypeEnabled, OperatorContext oContext,
                                 BatchSchema coercedSchema, VectorContainer coercedContainer) {

    this.probeBatch = probeBatch;
    this.probeSchema = probeBatch.getSchema();
    this.buildBatch = buildBatch;
    this.joinType = joinRelType;
    this.hashTable = hashTable;
    this.hjHelper = hjHelper;
    this.outgoingJoinBatch = outgoing;
    this.unionTypeEnabled = unionTypeEnabled;
    this.coercedSchema = coercedSchema;
    this.coercedContainer = coercedContainer;
    this.oContext = oContext;
    this.probeStatus = probeStatus;
    this.probeStatus.recordsToProcess = probeRecordCount;
    this.probeStatus.recordsProcessed = 0;
    if (coercedContainer == null) {
      doSetup(context, (VectorAccessible)buildBatch, (VectorAccessible)probeBatch, outgoing);
    } else {
      doSetup(context, (VectorAccessible)buildBatch, (VectorAccessible)coercedContainer, outgoing);
    }
  }

  public void executeProjectRightPhase() {
    while (outputRecords < TARGET_RECORDS_PER_BATCH && probeStatus.recordsProcessed < probeStatus.recordsToProcess) {
      projectBuildRecord(probeStatus.unmatchedBuildIndexes.get(probeStatus.recordsProcessed), outputRecords);
      probeStatus.recordsProcessed++;
      outputRecords++;
    }
  }

  public boolean schemaChanged() {
    return schemaChanged;
  }

  public void executeProbePhase() throws SchemaChangeException {
    while (outputRecords < TARGET_RECORDS_PER_BATCH && probeStatus.probeState != ProbeState.DONE && probeStatus.probeState != ProbeState.PROJECT_RIGHT) {
      // Check if we have processed all records in this batch we need to invoke next
      if (probeStatus.recordsProcessed == probeStatus.recordsToProcess) {
        // Done processing all records in the previous batch, clean up!
        for (VectorWrapper<?> wrapper : probeBatch) {
          wrapper.getValueVector().clear();
        }
        if (coercedContainer != null) {
          coercedContainer.zeroVectors();
        }
        final IterOutcome leftUpstream = outgoingJoinBatch.next(HashJoinHelper.LEFT_INPUT, probeBatch);
        switch (leftUpstream) {
          case NONE:
          case NOT_YET:
          case STOP:
            probeStatus.recordsProcessed = 0;
            probeStatus.recordsToProcess = 0;
            probeStatus.probeState = ProbeState.DONE;

            // We are done with the probe phase. If its a RIGHT or a FULL join get the unmatched indexes from the build side
            if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
                probeStatus.probeState = ProbeState.PROJECT_RIGHT;
            }

            continue;

          case OK_NEW_SCHEMA:
            if (probeBatch.getSchema().equals(probeSchema)) {
              doSetup(outgoingJoinBatch.getContext(), buildBatch, probeBatch, outgoingJoinBatch);
              hashTable.updateBatches();
            } else {
              if (!unionTypeEnabled) {
                throw new SchemaChangeException("Hash join does not support schema changes");
              } else {
                schemaChanged = true;
                return;
              }
            }
          case OK:
            probeStatus.recordsToProcess = probeBatch.getRecordCount();
            probeStatus.recordsProcessed = 0;
            // If we received an empty batch do nothing
            if (probeStatus.recordsToProcess == 0) {
              continue;
            }
            if (coercedSchema != null) {
              coercedContainer.zeroVectors();
              final VectorContainer promotedContainer = SchemaUtil.coerceContainer(probeBatch, coercedSchema, oContext);
              promotedContainer.transferOut(coercedContainer);
            }
        }
      }
      int probeIndex = -1;
      // Check if we need to drain the next row in the probe side
      if (probeStatus.getNextRecord) {
        if (hashTable != null) {
          probeIndex = hashTable.containsKey(probeStatus.recordsProcessed, true);
        }

        if (probeIndex != -1) {
          /* The current probe record has a key that matches. Get the index
           * of the first row in the build side that matches the current key
           */
          probeStatus.currentCompositeIdx = hjHelper.getStartIndex(probeIndex);
          /* Record in the build side at probeStatus.currentCompositeIdx has a matching record in the probe
           * side. Set the bit corresponding to this index so if we are doing a FULL or RIGHT
           * join we keep track of which records we need to project at the end
           */
          hjHelper.setRecordMatched(probeStatus.currentCompositeIdx);
          projectBuildRecord(probeStatus.currentCompositeIdx, outputRecords);
          projectProbeRecord(probeStatus.recordsProcessed, outputRecords);
          outputRecords++;
            /* Projected single row from the build side with matching key but there
             * may be more rows with the same key. Check if that's the case
             */
          probeStatus.currentCompositeIdx = hjHelper.getNextIndex(probeStatus.currentCompositeIdx);
          if (probeStatus.currentCompositeIdx == -1) {
              /* We only had one row in the build side that matched the current key
               * from the probe side. Drain the next row in the probe side.
               */
            probeStatus.recordsProcessed++;
          } else {
              /* There is more than one row with the same key on the build side
               * don't drain more records from the probe side till we have projected
               * all the rows with this key
               */
            probeStatus.getNextRecord = false;
          }
        } else { // No matching key
          // If we have a left outer join, project the keys
          if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            projectProbeRecord(probeStatus.recordsProcessed, outputRecords);
            outputRecords++;
          }
          probeStatus.recordsProcessed++;
        }
      } else {
        hjHelper.setRecordMatched(probeStatus.currentCompositeIdx);
        projectBuildRecord(probeStatus.currentCompositeIdx, outputRecords);
        projectProbeRecord(probeStatus.recordsProcessed, outputRecords);
        outputRecords++;

        probeStatus.currentCompositeIdx = hjHelper.getNextIndex(probeStatus.currentCompositeIdx);
        //System.out.println("copying next idx " + probeStatus.currentCompositeIdx);
        if (probeStatus.currentCompositeIdx == -1) {
          // We don't have any more rows matching the current key on the build side, move on to the next probe row
          probeStatus.getNextRecord = true;
          probeStatus.recordsProcessed++;
        }
      }
    }
  }

  public int probeAndProject() throws SchemaChangeException, ClassTransformationException, IOException {

    outputRecords = 0;

    if (probeStatus.probeState == ProbeState.PROBE_PROJECT) {
      executeProbePhase();
    }

    if (probeStatus.probeState == ProbeState.PROJECT_RIGHT) {

      // We are here because we have a RIGHT OUTER or a FULL join
      if (probeStatus.unmatchedBuildIndexes == null) {
        // Initialize list of build indexes that didn't match a record on the probe side
        probeStatus.unmatchedBuildIndexes = hjHelper.getNextUnmatchedIndex();
        probeStatus.recordsToProcess = probeStatus.unmatchedBuildIndexes.size();
        probeStatus.recordsProcessed = 0;
      }

      // Project the list of unmatched records on the build side
      executeProjectRightPhase();
    }

    return outputRecords;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("buildBatch") VectorAccessible buildBatch,
                               @Named("probeBatch") VectorAccessible probeBatch,
                               @Named("outgoing") RecordBatch outgoing);
  public abstract void projectBuildRecord(@Named("buildIndex") int buildIndex, @Named("outIndex") int outIndex);

  public abstract void projectProbeRecord(@Named("probeIndex") int probeIndex, @Named("outIndex") int outIndex);

}
