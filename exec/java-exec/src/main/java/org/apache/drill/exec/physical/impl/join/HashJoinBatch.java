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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.svremover.RemovingRecordBatch;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.eigenbase.rel.JoinRelType;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

public class HashJoinBatch extends AbstractRecordBatch<HashJoinPOP> {

  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

    // Probe side record batch
    private final RecordBatch left;

    // Build side record batch
    private final RecordBatch right;

    // Join type, INNER, LEFT, RIGHT or OUTER
    private final JoinRelType joinType;

    // Join conditions
    private final List<JoinCondition> conditions;

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

    // Schema of the build side
    private BatchSchema rightSchema = null;

    private boolean first = true;

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

    // indicates if we have previously returned an output batch
    boolean firstOutputBatch = true;

    IterOutcome leftUpstream = IterOutcome.NONE;

    private final HashTableStats htStats = new HashTableStats();

    public enum Metric implements MetricDef {

      NUM_BUCKETS,
      NUM_ENTRIES,
      NUM_RESIZING,
      RESIZING_TIME;
      
      // duplicate for hash ag

      @Override
      public int metricId() {
        return ordinal();
      }
    }
    
    @Override
    public int getRecordCount() {
        return outputRecords;
    }


    @Override
    public IterOutcome innerNext() {
        try {
            /* If we are here for the first time, execute the build phase of the
             * hash join and setup the run time generated class for the probe side
             */
            if (hashJoinProbe == null) {

                // Initialize the hash join helper context
                hjHelper = new HashJoinHelper(context, oContext.getAllocator());

                /* Build phase requires setting up the hash table. Hash table will
                 * materialize both the build and probe side expressions while
                 * creating the hash table. So we need to invoke next() on our probe batch
                 * as well, for the materialization to be successful. This batch will not be used
                 * till we complete the build phase.
                 */
                leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);

                // Build the hash table, using the build side record batches.
                executeBuildPhase();

                // Update the hash table related stats for the operator
                updateStats(this.hashTable);

                // Create the run time generated code needed to probe and project
                hashJoinProbe = setupHashJoinProbe();
            }

            // Store the number of records projected
            if (hashTable != null
                || joinType != JoinRelType.INNER) {

                // Allocate the memory for the vectors in the output container
                allocateVectors();

                outputRecords = hashJoinProbe.probeAndProject();

                /* We are here because of one the following
                 * 1. Completed processing of all the records and we are done
                 * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
                 * Either case build the output container's schema and return
                 */
                if (outputRecords > 0 || first) {
                  first = false;

                  // Build the container schema and set the counts
                  container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
                  container.setRecordCount(outputRecords);

                  for (VectorWrapper<?> v : container) {
                    v.getValueVector().getMutator().setValueCount(outputRecords);
                  }

                  // First output batch, return OK_NEW_SCHEMA
                  if (firstOutputBatch == true) {
                    firstOutputBatch = false;
                    return IterOutcome.OK_NEW_SCHEMA;
                  }

                  // Not the first output batch
                  return IterOutcome.OK;
                }
            } else {
                // Our build side is empty, we won't have any matches, clear the probe side
                if (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
                    for (VectorWrapper<?> wrapper : left) {
                      wrapper.getValueVector().clear();
                    }
                    leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
                    while (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
                      for (VectorWrapper<?> wrapper : left) {
                        wrapper.getValueVector().clear();
                      }
                      leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
                    }
                }
            }

            // No more output records, clean up and return
            return IterOutcome.NONE;

        } catch (ClassTransformationException | SchemaChangeException | IOException e) {
            context.fail(e);
            killIncoming();
            return IterOutcome.STOP;
        }
    }

    public void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {

        // Setup the hash table configuration object
        int conditionsSize = conditions.size();

        NamedExpression rightExpr[] = new NamedExpression[conditionsSize];
        NamedExpression leftExpr[] = new NamedExpression[conditionsSize];

        // Create named expressions from the conditions
        for (int i = 0; i < conditionsSize; i++) {
            rightExpr[i] = new NamedExpression(conditions.get(i).getRight(), new FieldReference("build_side_" + i ));
            leftExpr[i] = new NamedExpression(conditions.get(i).getLeft(), new FieldReference("probe_side_" + i));

            // Hash join only supports equality currently.
            assert conditions.get(i).getRelationship().equals("==");
        }

        // Set the left named expression to be null if the probe batch is empty.
        if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
            leftExpr = null;
        } else {
          if (left.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
            throw new SchemaChangeException("Hash join does not support probe batch with selection vectors");
          }
        }

        HashTableConfig htConfig =
            new HashTableConfig(context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE_KEY).num_val.intValue(),
            HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr);

        // Create the chained hash table
        ChainedHashTable ht  = new ChainedHashTable(htConfig, context, oContext.getAllocator(), this.right, this.left, null);
        hashTable = ht.createAndSetupHashTable(null);
    }

    public void executeBuildPhase() throws SchemaChangeException, ClassTransformationException, IOException {

        //Setup the underlying hash table
        IterOutcome rightUpstream = next(HashJoinHelper.RIGHT_INPUT, right);

        boolean moreData = true;

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

                        if (rightSchema.getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
                          throw new SchemaChangeException("Hash join does not support build batch with selection vectors");
                        }
                        setupHashTable();
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
            rightUpstream = next(HashJoinHelper.RIGHT_INPUT, right);
        }
    }

    public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {



        final CodeGenerator<HashJoinProbe> cg = CodeGenerator.get(HashJoinProbe.TEMPLATE_DEFINITION, context.getFunctionRegistry());
        ClassGenerator<HashJoinProbe> g = cg.getRoot();

        // Generate the code to project build side records
        g.setMappingSet(projectBuildMapping);


        int fieldId = 0;
        JExpression buildIndex = JExpr.direct("buildIndex");
        JExpression outIndex = JExpr.direct("outIndex");
        g.rotateBlock();

        if (hyperContainer != null) {
            for(VectorWrapper<?> vv : hyperContainer) {

                MajorType inputType = vv.getField().getType();
                MajorType outputType;
                if (joinType == JoinRelType.LEFT && inputType.getMode() == DataMode.REQUIRED) {
                  outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
                } else {
                  outputType = inputType;
                }

                // Add the vector to our output container
                ValueVector v = TypeHelper.getNewVector(MaterializedField.create(vv.getField().getPath(), outputType), context.getAllocator());
                container.add(v);

                JVar inVV = g.declareVectorValueSetupAndMember("buildBatch", new TypedFieldId(vv.getField().getType(), true, fieldId));
                JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(outputType, false, fieldId));
                g.getEvalBlock()._if(outVV.invoke("copyFromSafe")
                  .arg(buildIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
                  .arg(outIndex)
                  .arg(inVV.component(buildIndex.shrz(JExpr.lit(16)))).not())._then()._return(JExpr.FALSE);

                fieldId++;
            }
        }
        g.rotateBlock();
        g.getEvalBlock()._return(JExpr.TRUE);

        // Generate the code to project probe side records
        g.setMappingSet(projectProbeMapping);

        int outputFieldId = fieldId;
        fieldId = 0;
        JExpression probeIndex = JExpr.direct("probeIndex");
        int recordCount = 0;

        if (leftUpstream == IterOutcome.OK || leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
            for (VectorWrapper<?> vv : left) {

                MajorType inputType = vv.getField().getType();
                MajorType outputType;
                if (joinType == JoinRelType.RIGHT && inputType.getMode() == DataMode.REQUIRED) {
                  outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
                } else {
                  outputType = inputType;
                }

                ValueVector v = TypeHelper.getNewVector(MaterializedField.create(vv.getField().getPath(), outputType), oContext.getAllocator());
                container.add(v);

                JVar inVV = g.declareVectorValueSetupAndMember("probeBatch", new TypedFieldId(inputType, false, fieldId));
                JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(outputType, false, outputFieldId));

                g.getEvalBlock()._if(outVV.invoke("copyFromSafe").arg(probeIndex).arg(outIndex).arg(inVV).not())._then()._return(JExpr.FALSE);

                fieldId++;
                outputFieldId++;
            }
            recordCount = left.getRecordCount();
        }
        g.rotateBlock();
        g.getEvalBlock()._return(JExpr.TRUE);

        HashJoinProbe hj = context.getImplementationClass(cg);

        hj.setupHashJoinProbe(context, hyperContainer, left, recordCount, this, hashTable, hjHelper, joinType);
        return hj;
    }

    private void allocateVectors(){
      for(VectorWrapper<?> v : container){
        v.getValueVector().allocateNew();
      }
    }

    public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context, RecordBatch left, RecordBatch right) throws OutOfMemoryException {
        super(popConfig, context);
        this.left = left;
        this.right = right;
        this.joinType = popConfig.getJoinType();
        this.conditions = popConfig.getConditions();
    }

    private void updateStats(HashTable htable) {
      if(htable == null) return;
      htable.getStats(htStats);
      this.stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
      this.stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
      this.stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
      this.stats.setLongStat(Metric.RESIZING_TIME, htStats.resizingTime);
    }

    @Override
    public void killIncoming() {
        this.left.kill();
        this.right.kill();
    }

    @Override
    public void cleanup() {
        hjHelper.clear();

        // If we didn't receive any data, hyperContainer may be null, check before clearing
        if (hyperContainer != null) {
            hyperContainer.clear();
        }

        if (hashTable != null) {
            hashTable.clear();
        }
        super.cleanup();
        left.cleanup();
        right.cleanup();
    }
}
