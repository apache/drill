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

import org.apache.drill.exec.record.*;
import org.eigenbase.rel.JoinRelType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import com.sun.codemodel.JExpr;

import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

public class HashJoinBatch extends AbstractRecordBatch<HashJoinPOP> {
    // Probe side record batch
    private final RecordBatch left;

    // Build side record batch
    private final RecordBatch right;

    // Join type, INNER, LEFT, RIGHT or OUTER
    private final JoinRelType joinType;

    // hash table configuration, created in HashJoinPOP
    private HashTableConfig htConfig;

    // Runtime generated class implementing HashJoinProbe interface
    private HashJoinProbe hashJoinProbe = null;

    /* Helper class
     * Maintains linked list of build side records with the same key
     * Keeps information about which build records have a corresponding
     * matching key in the probe side (for outer, right joins)
     */
    private HashJoinHelper hjHelper = null;

    // Underlying hashtable used by the hash join
    private HashTable hashTable = null;

    /* Hyper container to store all build side record batches.
     * Records are retrieved from this container when there is a matching record
     * on the probe side
     */
    private ExpandableHyperContainer hyperContainer;

    // Number of records in the output container
    private int outputRecords;

    // Current batch index on the build side
    private int buildBatchIndex = 0;

    // List of vector allocators
    private List<VectorAllocator> allocators = null;

    // Schema of the build side
    private BatchSchema rightSchema = null;

    // Generator mapping for the build side
    private static final GeneratorMapping PROJECT_BUILD = GeneratorMapping.create("doSetup"/* setup method */,
                                                                                  "projectBuildRecord" /* eval method */,
                                                                                  null /* reset */, null /* cleanup */);

    // Generator mapping for the probe side
    private static final GeneratorMapping PROJECT_PROBE = GeneratorMapping.create("doSetup" /* setup method */,
                                                                                  "projectProbeRecord" /* eval method */,
                                                                                  null /* reset */, null /* cleanup */);

    // Mapping set for the build side
    private final MappingSet projectBuildMapping = new MappingSet("buildIndex" /* read index */, "outIndex" /* write index */,
                                                                  "buildBatch" /* read container */,
                                                                  "outgoing" /* write container */,
                                                                  PROJECT_BUILD, PROJECT_BUILD);

    // Mapping set for the probe side
    private final MappingSet projectProbeMapping = new MappingSet("probeIndex" /* read index */, "outIndex" /* write index */,
                                                                  "probeBatch" /* read container */,
                                                                  "outgoing" /* write container */,
                                                                  PROJECT_PROBE, PROJECT_PROBE);

    @Override
    public int getRecordCount() {
        return outputRecords;
    }


    @Override
    public IterOutcome next() {

        try {
            /* If we are here for the first time, execute the build phase of the
             * hash join and setup the run time generated class for the probe side
             */
            if (hashJoinProbe == null) {

                // Initialize the hash join helper context
                hjHelper = new HashJoinHelper(context);

                /* Build phase requires setting up the hash table. Hash table will
                 * materialize both the build and probe side expressions while
                 * creating the hash table. So we need to invoke next() on our probe batch
                 * as well, for the materialization to be successful. This batch will not be used
                 * till we complete the build phase.
                 */
                left.next();

                // Build the hash table, using the build side record batches.
                executeBuildPhase();

                // Create the run time generated code needed to probe and project
                hashJoinProbe = setupHashJoinProbe();
            }

            // Allocate the memory for the vectors in the output container
            allocateVectors();

            // Store the number of records projected
            outputRecords = hashJoinProbe.probeAndProject();

            /* We are here because of one the following
             * 1. Completed processing of all the records and we are done
             * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
             * Either case build the output container's schema and return
             */
            if (outputRecords > 0) {

                // Build the container schema and set the counts
                container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
                container.setRecordCount(outputRecords);

                for (VectorWrapper<?> v : container) {
                    v.getValueVector().getMutator().setValueCount(outputRecords);
                }

                return IterOutcome.OK_NEW_SCHEMA;
            }

            // No more output records, clean up and return
            cleanup();
            return IterOutcome.NONE;

        } catch (ClassTransformationException | SchemaChangeException | IOException e) {
            context.fail(e);
            killIncoming();
            cleanup();
            return IterOutcome.STOP;
        }
    }

    public void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {

        // Shouldn't be recreating the hash table, this should be done only once
        assert hashTable == null;

        ChainedHashTable ht  = new ChainedHashTable(htConfig, context, this.right, this.left, null);
        hashTable = ht.createAndSetupHashTable(null);

    }

    public void executeBuildPhase() throws SchemaChangeException, ClassTransformationException, IOException {

        //Setup the underlying hash table
        IterOutcome rightUpstream = right.next();

        boolean moreData = true;

        setupHashTable();

        while (moreData) {

            switch (rightUpstream) {

                case NONE:
                case NOT_YET:
                case STOP:
                    moreData = false;
                    continue;

                case OK_NEW_SCHEMA:
                    if (rightSchema == null) {
                        rightSchema = right.getSchema();
                    } else {
                        throw new SchemaChangeException("Hash join does not support schema changes");
                    }
                // Fall through
                case OK:
                    int currentRecordCount = right.getRecordCount();

                    /* For every new build batch, we store some state in the helper context
                     * Add new state to the helper context
                     */
                    hjHelper.addNewBatch(currentRecordCount);

                    // Holder contains the global index where the key is hashed into using the hash table
                    IntHolder htIndex = new IntHolder();

                    // For every record in the build batch , hash the key columns
                    for (int i = 0; i < currentRecordCount; i++) {

                        HashTable.PutStatus status = hashTable.put(i, htIndex);

                        if (status != HashTable.PutStatus.PUT_FAILED) {
                            /* Use the global index returned by the hash table, to store
                             * the current record index and batch index. This will be used
                             * later when we probe and find a match.
                             */
                            hjHelper.setCurrentIndex(htIndex.value, buildBatchIndex, i);
                        }
                    }

                    /* Completed hashing all records in this batch. Transfer the batch
                     * to the hyper vector container. Will be used when we want to retrieve
                     * records that have matching keys on the probe side.
                     */
                    RecordBatchData nextBatch = new RecordBatchData(right);
                    if (hyperContainer == null) {
                        hyperContainer = new ExpandableHyperContainer(nextBatch.getContainer());
                    } else {
                        hyperContainer.addBatch(nextBatch.getContainer());
                    }

                    // completed processing a batch, increment batch index
                    buildBatchIndex++;
                    break;
            }
            // Get the next record batch
            rightUpstream = right.next();
        }
    }

    public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {

        allocators = new ArrayList<>();

        final CodeGenerator<HashJoinProbe> cg = CodeGenerator.get(HashJoinProbe.TEMPLATE_DEFINITION, context.getFunctionRegistry());
        ClassGenerator<HashJoinProbe> g = cg.getRoot();

        // Generate the code to project build side records
        g.setMappingSet(projectBuildMapping);


        int fieldId = 0;
        JExpression buildIndex = JExpr.direct("buildIndex");
        JExpression outIndex = JExpr.direct("outIndex");
        g.rotateBlock();
        for(VectorWrapper<?> vv : hyperContainer) {

            // Add the vector to our output container
            ValueVector v = TypeHelper.getNewVector(vv.getField(), context.getAllocator());
            container.add(v);
            allocators.add(RemovingRecordBatch.getAllocator4(v));

            JVar inVV = g.declareVectorValueSetupAndMember("buildBatch", new TypedFieldId(vv.getField().getType(), fieldId, true));
            JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(vv.getField().getType(), fieldId, false));

            g.getEvalBlock().add(outVV.invoke("copyFrom")
                    .arg(buildIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
                    .arg(outIndex)
                    .arg(inVV.component(buildIndex.shrz(JExpr.lit(16)))));

            fieldId++;
        }

        // Generate the code to project probe side records
        g.setMappingSet(projectProbeMapping);

        int outputFieldId = fieldId;
        fieldId = 0;
        JExpression probeIndex = JExpr.direct("probeIndex");
        for (VectorWrapper<?> vv : left) {

            ValueVector v = TypeHelper.getNewVector(vv.getField(), context.getAllocator());
            container.add(v);
            allocators.add(RemovingRecordBatch.getAllocator4(v));

            JVar inVV = g.declareVectorValueSetupAndMember("probeBatch", new TypedFieldId(vv.getField().getType(), fieldId, false));
            JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(vv.getField().getType(), outputFieldId, false));

            g.getEvalBlock().add(outVV.invoke("copyFrom").arg(probeIndex).arg(outIndex).arg(inVV));

            fieldId++;
            outputFieldId++;
        }

        HashJoinProbe hj = context.getImplementationClass(cg);
        hj.setupHashJoinProbe(context, hyperContainer, left, this, hashTable, hjHelper, joinType);
        return hj;
    }

    private void allocateVectors(){
        for(VectorAllocator a : allocators){
            a.alloc(RecordBatch.MAX_BATCH_SIZE);
        }
    }

    public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) {
        super(popConfig, context);
        this.left = left;
        this.right = right;
        this.joinType = popConfig.getJoinType();
        this.htConfig = popConfig.getHtConfig();
    }

    @Override
    public void killIncoming() {
        this.left.kill();
        this.right.kill();
        cleanup();
    }

    @Override
    public void cleanup() {
        left.cleanup();
        right.cleanup();
        hyperContainer.clear();
        hjHelper.clear();
        container.clear();
        hashTable.clear();
        super.cleanup();
    }
}
