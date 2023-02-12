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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.planner.common.JoinControl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.shaded.guava.com.google.common.collect.Iterables;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the runtime execution for the Hash-Join operator supporting INNER,
 * LEFT OUTER, RIGHT OUTER, and FULL OUTER joins
 */
public class HashJoinBatch extends AbstractHashBinaryRecordBatch<HashJoinPOP>
    implements RowKeyJoin {
  // Join type, INNER, LEFT, RIGHT or OUTER
  private final JoinRelType joinType;
  // Join conditions
  private final List<JoinCondition> conditions;
  private final JoinControl joinControl;

  /**
   * The constructor
   *
   * @param popConfig HashJoinPOP
   * @param context FragmentContext
   * @param left probe/outer side incoming input
   * @param right build/iner side incoming input
   * @throws OutOfMemoryException out of memory exception
   */
  public HashJoinBatch(HashJoinPOP popConfig, FragmentContext context,
      RecordBatch left, /* Probe side record batch */
      RecordBatch right /* Build side record batch */
  ) throws OutOfMemoryException {
    super(popConfig, context, left, right);
    joinType = popConfig.getJoinType();
    conditions = popConfig.getConditions();
    this.joinControl = new JoinControl(popConfig.getJoinControl());

    semiJoin = popConfig.isSemiJoin();
    joinIsLeftOrFull = joinType == JoinRelType.LEFT
      || joinType == JoinRelType.FULL;
    joinIsRightOrFull = joinType == JoinRelType.RIGHT
      || joinType == JoinRelType.FULL;
    this.isRowKeyJoin = popConfig.isRowKeyJoin();

    for (int i = 0; i < conditions.size(); i++) {
      SchemaPath rightPath = (SchemaPath) conditions.get(i).getRight();
      PathSegment.NameSegment nameSegment = (PathSegment.NameSegment) rightPath
        .getLastSegment();
      buildJoinColumns.add(nameSegment.getPath());
      String refName = "build_side_" + i;
      rightExpr.add(new NamedExpression(conditions.get(i).getRight(),
        new FieldReference(refName)));
    }

    runtimeFilterDef = popConfig.getRuntimeFilterDef();
    enableRuntimeFilter = context.getOptions().getOption(ExecConstants.HASHJOIN_ENABLE_RUNTIME_FILTER)
      && runtimeFilterDef != null;
  }

  @Override
  public Probe createProbe() {
    // No real code generation !!
    return new HashJoinProbeTemplate();
  }

  @Override
  public void setupProbe() throws SchemaChangeException {
    probe.setup(probeBatch, this, joinType,
      semiJoin, leftUpstream, partitions, spilledState.getCycle(),
      container, spilledInners, buildSideIsEmpty.booleanValue(),
      numPartitions, rightHVColPosition);
  }

  @Override
  protected HashTableConfig buildHashTableConfig() {
    List<Comparator> comparators = Lists
      .newArrayListWithExpectedSize(conditions.size());
    conditions.forEach(cond -> comparators
      .add(JoinUtils.checkAndReturnSupportedJoinComparator(cond)));

    // Setup the hash table configuration object
    List<NamedExpression> leftExpr = new ArrayList<>(conditions.size());

    // Create named expressions from the conditions
    for (int i = 0; i < conditions.size(); i++) {
      leftExpr.add(new NamedExpression(conditions.get(i).getLeft(),
        new FieldReference("probe_side_" + i)));
    }

    // Set the left named expression to be null if the probe batch is empty.
    if (leftUpstream != IterOutcome.OK_NEW_SCHEMA
      && leftUpstream != IterOutcome.OK) {
      leftExpr = null;
    } else {
      if (probeBatch.getSchema()
        .getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        throw UserException.internalError(null).message(
            "Hash join does not support probe batch with selection vectors.")
          .addContext("Probe batch has selection mode",
            (probeBatch.getSchema().getSelectionVectorMode()).toString())
          .build(logger);
      }
    }

    return new HashTableConfig(
      (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
      true, HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators,
      joinControl.asInt(), false);
  }

  @Override
  public void dump() {
    logger.error(
        "HashJoinBatch[container={}, left={}, right={}, leftOutcome={}, rightOutcome={}, joinType={}, hashJoinProbe={},"
            + " rightExpr={}, canSpill={}, buildSchema={}, probeSchema={}]",
        container, left, right, leftUpstream, rightUpstream, joinType,
      probe, rightExpr, canSpill, buildSchema, probeSchema);
  }

  @Override // implement RowKeyJoin interface
  public boolean hasRowKeyBatch() {
    return buildComplete;
  }

  @Override // implement RowKeyJoin interface
  public BatchState getBatchState() {
    return state;
  }

  @Override // implement RowKeyJoin interface
  public void setBatchState(BatchState newState) {
    state = newState;
  }

  @Override
  public void setRowKeyJoinState(RowKeyJoin.RowKeyJoinState newState) {
    this.rkJoinState = newState;
  }

  @Override
  public RowKeyJoin.RowKeyJoinState getRowKeyJoinState() {
    return rkJoinState;
  }

  /**
   * Get the hash table iterator that is created for the build side of the hash
   * join if this hash join was instantiated as a row-key join.
   *
   * @return hash table iterator or null if this hash join was not a row-key
   *         join or if it was a row-key join but the build has not yet
   *         completed.
   */
  @Override
  public Pair<ValueVector, Integer> nextRowKeyBatch() {
    if (buildComplete) {
      // partition 0 because Row Key Join has only a single partition - no
      // spilling
      Pair<VectorContainer, Integer> pp = partitions[0].nextBatch();
      if (pp != null) {
        VectorWrapper<?> vw = Iterables.get(pp.getLeft(), 0);
        ValueVector vv = vw.getValueVector();
        return Pair.of(vv, pp.getRight());
      }
    } else if (partitions == null && firstOutputBatch) { // if there is data
      // coming to
      // right(build) side in
      // build Schema stage,
      // use it.
      firstOutputBatch = false;
      if (right.getRecordCount() > 0) {
        VectorWrapper<?> vw = Iterables.get(right, 0);
        ValueVector vv = vw.getValueVector();
        return Pair.of(vv, right.getRecordCount() - 1);
      }
    }
    return null;
  }
}
