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

import javax.inject.Named;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.sql2rel.StandardConvertletTable;

import java.io.IOException;
import java.util.List;

public abstract class HashJoinProbeTemplate implements HashJoinProbe {

    // Probe side record batch
    private RecordBatch probeBatch;

    // Join type, INNER, LEFT, RIGHT or OUTER
    private JoinRelType joinType;

    /* Helper class
     * Maintains linked list of build side records with the same key
     * Keeps information about which build records have a corresponding
     * matching key in the probe side (for outer, right joins)
     */
    private HashJoinHelper hjHelper = null;

    // Underlying hashtable used by the hash join
    private HashTable hashTable = null;

    // Number of records to process on the probe side
    private int recordsToProcess = 0;

    // Number of records processed on the probe side
    private int recordsProcessed = 0;

    // Number of records in the output container
    private int outputRecords;

    // Indicate if we should drain the next record from the probe side
    private boolean getNextRecord = true;

    // Contains both batch idx and record idx of the matching record in the build side
    private int currentCompositeIdx = -1;

    // Current state the hash join algorithm is in
    private ProbeState probeState = ProbeState.PROBE_PROJECT;

    // For outer or right joins, this is a list of unmatched records that needs to be projected
    private List<Integer> unmatchedBuildIndexes = null;

    @Override
    public void setupHashJoinProbe(FragmentContext context, VectorContainer buildBatch, RecordBatch probeBatch,
                                   RecordBatch outgoing, HashTable hashTable, HashJoinHelper hjHelper,
                                   JoinRelType joinRelType) {

        this.probeBatch = probeBatch;
        this.joinType = joinRelType;
        this.recordsToProcess = probeBatch.getRecordCount();
        this.hashTable = hashTable;
        this.hjHelper = hjHelper;

        doSetup(context, buildBatch, probeBatch, outgoing);
    }

    public void executeProjectRightPhase() {
        while (outputRecords < RecordBatch.MAX_BATCH_SIZE && recordsProcessed < recordsToProcess) {
            projectBuildRecord(unmatchedBuildIndexes.get(recordsProcessed++), outputRecords++);
        }
    }

    public void executeProbePhase() throws SchemaChangeException {
        while (outputRecords < RecordBatch.MAX_BATCH_SIZE && recordsToProcess > 0) {

            // Check if we have processed all records in this batch we need to invoke next
            if (recordsProcessed == recordsToProcess) {
                IterOutcome leftUpstream = probeBatch.next();

                switch (leftUpstream) {
                    case NONE:
                    case NOT_YET:
                    case STOP:
                        recordsProcessed = 0;
                        recordsToProcess = 0;
                        probeState = ProbeState.DONE;

                        // We are done with the probe phase. If its a RIGHT or a FULL join get the unmatched indexes from the build side
                        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
                            probeState = ProbeState.PROJECT_RIGHT;
                        }

                        continue;

                    case OK_NEW_SCHEMA:
                        throw new SchemaChangeException("Hash join does not support schema changes");
                    case OK:
                        recordsToProcess = probeBatch.getRecordCount();
                        recordsProcessed = 0;
                }
            }
            int probeIndex;

            // Check if we need to drain the next row in the probe side
            if (getNextRecord) {
                probeIndex = hashTable.containsKey(recordsProcessed, true);

                if (probeIndex != -1) {

                    /* The current probe record has a key that matches. Get the index
                     * of the first row in the build side that matches the current key
                     */
                    currentCompositeIdx = hjHelper.getStartIndex(probeIndex);

                    /* Record in the build side at currentCompositeIdx has a matching record in the probe
                     * side. Set the bit corresponding to this index so if we are doing a FULL or RIGHT
                     * join we keep track of which records we need to project at the end
                     */
                    hjHelper.setRecordMatched(currentCompositeIdx);

                    projectBuildRecord(currentCompositeIdx, outputRecords);
                    projectProbeRecord(recordsProcessed, outputRecords);
                    outputRecords++;

                    /* Projected single row from the build side with matching key but there
                     * may be more rows with the same key. Check if that's the case
                     */
                    currentCompositeIdx = hjHelper.getNextIndex(currentCompositeIdx);
                    if (currentCompositeIdx == -1) {
                        /* We only had one row in the build side that matched the current key
                         * from the probe side. Drain the next row in the probe side.
                         */
                        recordsProcessed++;
                    }
                    else {
                        /* There is more than one row with the same key on the build side
                         * don't drain more records from the probe side till we have projected
                         * all the rows with this key
                         */
                        getNextRecord = false;
                    }
                }
                else { // No matching key

                    // If we have a left outer join, project the keys
                    if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
                        projectProbeRecord(recordsProcessed, outputRecords++);
                    }
                    recordsProcessed++;
                }
            }
            else {
                hjHelper.setRecordMatched(currentCompositeIdx);
                projectBuildRecord(currentCompositeIdx, outputRecords);
                projectProbeRecord(recordsProcessed, outputRecords);
                outputRecords++;

                currentCompositeIdx = hjHelper.getNextIndex(currentCompositeIdx);

                if (currentCompositeIdx == -1) {
                    // We don't have any more rows matching the current key on the build side, move on to the next probe row
                    getNextRecord = true;
                    recordsProcessed++;
                }
            }
        }
    }

    public int probeAndProject() throws SchemaChangeException, ClassTransformationException, IOException {

        outputRecords = 0;

        if (probeState == ProbeState.PROBE_PROJECT) {
            executeProbePhase();
        }

        if (probeState == ProbeState.PROJECT_RIGHT) {

            // We are here because we have a RIGHT OUTER or a FULL join
            if (unmatchedBuildIndexes == null) {
                // Initialize list of build indexes that didn't match a record on the probe side
                unmatchedBuildIndexes = hjHelper.getNextUnmatchedIndex();
                recordsToProcess = unmatchedBuildIndexes.size();
                recordsProcessed = 0;
            }

            // Project the list of unmatched records on the build side
            executeProjectRightPhase();
        }

        return outputRecords;
    }

    public abstract void doSetup(@Named("context") FragmentContext context, @Named("buildBatch") VectorContainer buildBatch, @Named("probeBatch") RecordBatch probeBatch,
                                 @Named("outgoing") RecordBatch outgoing);
    public abstract void projectBuildRecord(@Named("buildIndex") int buildIndex, @Named("outIndex") int outIndex);
    public abstract void projectProbeRecord(@Named("probeIndex") int probeIndex, @Named("outIndex") int outIndex);
}
