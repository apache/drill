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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.ChainedHashTable;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.common.IndexPointer;
import org.apache.drill.exec.physical.impl.join.JoinUtils.JoinComparator;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.calcite.rel.core.JoinRelType;

import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

public class HashJoinBatch extends AbstractRecordBatch<HashJoinPOP> {
  public static final long ALLOCATOR_INITIAL_RESERVATION = 1 * 1024 * 1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L * 1000 * 1000 * 1000;

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


  // Generator mapping for the build side
  // Generator mapping for the build side : scalar
  private static final GeneratorMapping PROJECT_BUILD =
      GeneratorMapping.create("doSetup"/* setup method */, "projectBuildRecord" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the build side : constant
  private static final GeneratorMapping PROJECT_BUILD_CONSTANT = GeneratorMapping.create("doSetup"/* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);

  // Generator mapping for the probe side : scalar
  private static final GeneratorMapping PROJECT_PROBE =
      GeneratorMapping.create("doSetup" /* setup method */, "projectProbeRecord" /* eval method */, null /* reset */,
          null /* cleanup */);
  // Generator mapping for the probe side : constant
  private static final GeneratorMapping PROJECT_PROBE_CONSTANT = GeneratorMapping.create("doSetup" /* setup method */,
      "doSetup" /* eval method */,
      null /* reset */, null /* cleanup */);


  // Mapping set for the build side
  private final MappingSet projectBuildMapping =
      new MappingSet("buildIndex" /* read index */, "outIndex" /* write index */, "buildBatch" /* read container */,
          "outgoing" /* write container */, PROJECT_BUILD_CONSTANT, PROJECT_BUILD);

  // Mapping set for the probe side
  private final MappingSet projectProbeMapping = new MappingSet("probeIndex" /* read index */, "outIndex" /* write index */,
      "probeBatch" /* read container */,
      "outgoing" /* write container */,
      PROJECT_PROBE_CONSTANT, PROJECT_PROBE);

  // indicates if we have previously returned an output batch
  boolean firstOutputBatch = true;

  IterOutcome leftUpstream = IterOutcome.NONE;
  IterOutcome rightUpstream = IterOutcome.NONE;

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
  protected void buildSchema() throws SchemaChangeException {
    leftUpstream = next(left);
    rightUpstream = next(right);

    if (leftUpstream == IterOutcome.STOP || rightUpstream == IterOutcome.STOP) {
      state = BatchState.STOP;
      return;
    }

    if (leftUpstream == IterOutcome.OUT_OF_MEMORY || rightUpstream == IterOutcome.OUT_OF_MEMORY) {
      state = BatchState.OUT_OF_MEMORY;
      return;
    }

    // Initialize the hash join helper context
    hjHelper = new HashJoinHelper(context, oContext.getAllocator());
    try {
      rightSchema = right.getSchema();
      final VectorContainer vectors = new VectorContainer(oContext);
      for (final VectorWrapper<?> w : right) {
        vectors.addOrGet(w.getField());
      }
      vectors.buildSchema(SelectionVectorMode.NONE);
      vectors.setRecordCount(0);
      hyperContainer = new ExpandableHyperContainer(vectors);
      hjHelper.addNewBatch(0);
      buildBatchIndex++;
      setupHashTable();
      hashJoinProbe = setupHashJoinProbe();
      // Build the container schema and set the counts
      for (final VectorWrapper<?> w : container) {
        w.getValueVector().allocateNew();
      }
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
      container.setRecordCount(outputRecords);
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  public IterOutcome innerNext() {
    try {
      /* If we are here for the first time, execute the build phase of the
       * hash join and setup the run time generated class for the probe side
       */
      if (state == BatchState.FIRST) {
        // Build the hash table, using the build side record batches.
        executeBuildPhase();
        //                IterOutcome next = next(HashJoinHelper.LEFT_INPUT, left);
        hashJoinProbe.setupHashJoinProbe(context, hyperContainer, left, left.getRecordCount(), this, hashTable,
            hjHelper, joinType);

        // Update the hash table related stats for the operator
        updateStats(this.hashTable);
      }

      // Store the number of records projected
      if (!hashTable.isEmpty() || joinType != JoinRelType.INNER) {

        // Allocate the memory for the vectors in the output container
        allocateVectors();

        outputRecords = hashJoinProbe.probeAndProject();

        /* We are here because of one the following
         * 1. Completed processing of all the records and we are done
         * 2. We've filled up the outgoing batch to the maximum and we need to return upstream
         * Either case build the output container's schema and return
         */
        if (outputRecords > 0 || state == BatchState.FIRST) {
          if (state == BatchState.FIRST) {
            state = BatchState.NOT_FIRST;
          }

          for (final VectorWrapper<?> v : container) {
            v.getValueVector().getMutator().setValueCount(outputRecords);
          }

          return IterOutcome.OK;
        }
      } else {
        // Our build side is empty, we won't have any matches, clear the probe side
        if (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
          for (final VectorWrapper<?> wrapper : left) {
            wrapper.getValueVector().clear();
          }
          left.kill(true);
          leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
          while (leftUpstream == IterOutcome.OK_NEW_SCHEMA || leftUpstream == IterOutcome.OK) {
            for (final VectorWrapper<?> wrapper : left) {
              wrapper.getValueVector().clear();
            }
            leftUpstream = next(HashJoinHelper.LEFT_INPUT, left);
          }
        }
      }

      // No more output records, clean up and return
      state = BatchState.DONE;
      //            if (first) {
      //              return IterOutcome.OK_NEW_SCHEMA;
      //            }
      return IterOutcome.NONE;
    } catch (ClassTransformationException | SchemaChangeException | IOException e) {
      context.fail(e);
      killIncoming(false);
      return IterOutcome.STOP;
    }
  }

  public void setupHashTable() throws IOException, SchemaChangeException, ClassTransformationException {
    // Setup the hash table configuration object
    int conditionsSize = conditions.size();
    final List<NamedExpression> rightExpr = new ArrayList<>(conditionsSize);
    List<NamedExpression> leftExpr = new ArrayList<>(conditionsSize);

    JoinComparator comparator = JoinComparator.NONE;
    // Create named expressions from the conditions
    for (int i = 0; i < conditionsSize; i++) {
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(), new FieldReference("build_side_" + i)));
      leftExpr.add(new NamedExpression(conditions.get(i).getLeft(), new FieldReference("probe_side_" + i)));

      // Hash join only supports certain types of comparisons
      comparator = JoinUtils.checkAndSetComparison(conditions.get(i), comparator);
    }

    assert comparator != JoinComparator.NONE;
    final boolean areNullsEqual = (comparator == JoinComparator.IS_NOT_DISTINCT_FROM) ? true : false;

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA && leftUpstream != IterOutcome.OK) {
      leftExpr = null;
    } else {
      if (left.getSchema().getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        throw new SchemaChangeException("Hash join does not support probe batch with selection vectors");
      }
    }

    final HashTableConfig htConfig =
        new HashTableConfig((int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
            HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr);

    // Create the chained hash table
    final ChainedHashTable ht =
        new ChainedHashTable(htConfig, context, oContext.getAllocator(), this.right, this.left, null,
            areNullsEqual);
    hashTable = ht.createAndSetupHashTable(null);
  }

  public void executeBuildPhase() throws SchemaChangeException, ClassTransformationException, IOException {
    //Setup the underlying hash table

    // skip first batch if count is zero, as it may be an empty schema batch
    if (right.getRecordCount() == 0) {
      for (final VectorWrapper<?> w : right) {
        w.clear();
      }
      rightUpstream = next(right);
    }

    boolean moreData = true;

    while (moreData) {
      switch (rightUpstream) {
      case OUT_OF_MEMORY:
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
          if (!rightSchema.equals(right.getSchema())) {
            throw new SchemaChangeException("Hash join does not support schema changes");
          }
          hashTable.updateBatches();
        }
        // Fall through
      case OK:
        final int currentRecordCount = right.getRecordCount();

                    /* For every new build batch, we store some state in the helper context
                     * Add new state to the helper context
                     */
        hjHelper.addNewBatch(currentRecordCount);

        // Holder contains the global index where the key is hashed into using the hash table
        final IndexPointer htIndex = new IndexPointer();

        // For every record in the build batch , hash the key columns
        for (int i = 0; i < currentRecordCount; i++) {
          hashTable.put(i, htIndex, 1 /* retry count */);

                        /* Use the global index returned by the hash table, to store
                         * the current record index and batch index. This will be used
                         * later when we probe and find a match.
                         */
          hjHelper.setCurrentIndex(htIndex.value, buildBatchIndex, i);
        }

                    /* Completed hashing all records in this batch. Transfer the batch
                     * to the hyper vector container. Will be used when we want to retrieve
                     * records that have matching keys on the probe side.
                     */
        final RecordBatchData nextBatch = new RecordBatchData(right, oContext.getAllocator());
        boolean success = false;
        try {
          if (hyperContainer == null) {
            hyperContainer = new ExpandableHyperContainer(nextBatch.getContainer());
          } else {
            hyperContainer.addBatch(nextBatch.getContainer());
          }

          // completed processing a batch, increment batch index
          buildBatchIndex++;
          success = true;
        } finally {
          if (!success) {
            nextBatch.clear();
          }
        }
        break;
      }
      // Get the next record batch
      rightUpstream = next(HashJoinHelper.RIGHT_INPUT, right);
    }
  }

  public HashJoinProbe setupHashJoinProbe() throws ClassTransformationException, IOException {
    final CodeGenerator<HashJoinProbe> cg = CodeGenerator.get(HashJoinProbe.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
    final ClassGenerator<HashJoinProbe> g = cg.getRoot();

    // Generate the code to project build side records
    g.setMappingSet(projectBuildMapping);

    int fieldId = 0;
    final JExpression buildIndex = JExpr.direct("buildIndex");
    final JExpression outIndex = JExpr.direct("outIndex");
    g.rotateBlock();

    if (rightSchema != null) {
      for (final MaterializedField field : rightSchema) {
        final MajorType inputType = field.getType();
        final MajorType outputType;
        // If left or full outer join, then the output type must be nullable. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2197).
        if ((joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        // make sure to project field with children for children to show up in the schema
        final MaterializedField projected = field.withType(outputType);
        // Add the vector to our output container
        container.addOrGet(projected);

        final JVar inVV = g.declareVectorValueSetupAndMember("buildBatch", new TypedFieldId(field.getType(), true, fieldId));
        final JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(outputType, false, fieldId));
        g.getEvalBlock().add(outVV.invoke("copyFromSafe")
            .arg(buildIndex.band(JExpr.lit((int) Character.MAX_VALUE)))
            .arg(outIndex)
            .arg(inVV.component(buildIndex.shrz(JExpr.lit(16)))));

        fieldId++;
      }
    }

    // Generate the code to project probe side records
    g.setMappingSet(projectProbeMapping);

    int outputFieldId = fieldId;
    fieldId = 0;
    final JExpression probeIndex = JExpr.direct("probeIndex");

    if (leftUpstream == IterOutcome.OK || leftUpstream == IterOutcome.OK_NEW_SCHEMA) {
      for (final VectorWrapper<?> vv : left) {
        final MajorType inputType = vv.getField().getType();
        final MajorType outputType;

        // If right or full outer join then the output type should be optional. However, map types are
        // not nullable so we must exclude them from the check below (see DRILL-2771, DRILL-2197).
        if ((joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) && inputType.getMode() == DataMode.REQUIRED
            && inputType.getMinorType() != TypeProtos.MinorType.MAP) {
          outputType = Types.overrideMode(inputType, DataMode.OPTIONAL);
        } else {
          outputType = inputType;
        }

        final ValueVector v = container.addOrGet(MaterializedField.create(vv.getField().getPath(), outputType));
        if (v instanceof AbstractContainerVector) {
          vv.getValueVector().makeTransferPair(v);
          v.clear();
        }

        final JVar inVV = g.declareVectorValueSetupAndMember("probeBatch", new TypedFieldId(inputType, false, fieldId));
        final JVar outVV = g.declareVectorValueSetupAndMember("outgoing", new TypedFieldId(outputType, false, outputFieldId));

        g.getEvalBlock().add(outVV.invoke("copyFromSafe").arg(probeIndex).arg(outIndex).arg(inVV));

        fieldId++;
        outputFieldId++;
      }
    }

    final HashJoinProbe hj = context.getImplementationClass(cg);
    return hj;
  }

  private void allocateVectors() {
    for (final VectorWrapper<?> v : container) {
      v.getValueVector().allocateNew();
    }
  }

  public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context, RecordBatch left,
      RecordBatch right) throws OutOfMemoryException {
    super(popConfig, context, true);
    this.left = left;
    this.right = right;
    joinType = popConfig.getJoinType();
    conditions = popConfig.getConditions();
  }

  private void updateStats(HashTable htable) {
    if (htable == null) {
      return;
    }
    htable.getStats(htStats);
    stats.setLongStat(Metric.NUM_BUCKETS, htStats.numBuckets);
    stats.setLongStat(Metric.NUM_ENTRIES, htStats.numEntries);
    stats.setLongStat(Metric.NUM_RESIZING, htStats.numResizing);
    stats.setLongStat(Metric.RESIZING_TIME, htStats.resizingTime);
  }

  @Override
  public void killIncoming(boolean sendUpstream) {
    left.kill(sendUpstream);
    right.kill(sendUpstream);
  }

  @Override
  public void close() {
    if (hjHelper != null) {
      hjHelper.clear();
    }

    // If we didn't receive any data, hyperContainer may be null, check before clearing
    if (hyperContainer != null) {
      hyperContainer.clear();
    }

    if (hashTable != null) {
      hashTable.clear();
    }
    super.close();
  }
}
