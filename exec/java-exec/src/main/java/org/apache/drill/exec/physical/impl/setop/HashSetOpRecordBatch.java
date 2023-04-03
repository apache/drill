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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.SetOp;
import org.apache.drill.exec.physical.impl.common.Comparator;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableConfig;
import org.apache.drill.exec.physical.impl.join.AbstractHashBinaryRecordBatch;
import org.apache.drill.exec.physical.impl.join.Probe;
import org.apache.drill.exec.planner.common.JoinControl;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Implements the runtime execution for the Hash-SetOp operator supporting EXCEPT,
 * EXCEPT ALL, INTERSECT, and INTERSECT ALL
 */
public class HashSetOpRecordBatch extends AbstractHashBinaryRecordBatch<SetOp> {
  private final SqlKind opType;
  private final boolean isAll;

  /**
   * The constructor
   *
   * @param popConfig SetOp
   * @param context FragmentContext
   * @param left probe/outer side incoming input
   * @param right build/inner side incoming input
   * @throws OutOfMemoryException out of memory exception
   */
  public HashSetOpRecordBatch(SetOp popConfig, FragmentContext context,
                              RecordBatch left, RecordBatch right)
    throws OutOfMemoryException {
    super(popConfig, context, left, right);
    this.opType = popConfig.getKind();
    this.isAll = popConfig.isAll();
    this.semiJoin = true;
    this.joinIsLeftOrFull = true;
    this.joinIsRightOrFull = false;
    this.isRowKeyJoin = false;
    this.enableRuntimeFilter = false;
  }

  private void buildCompareExpression(List<NamedExpression> leftExpr, List<Comparator> comparators) {
    Iterator<MaterializedField>  iterator = probeSchema.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      MaterializedField field = iterator.next();
      String refName = "probe_side_" + i;
      leftExpr.add(new NamedExpression(new FieldReference(field.getName()), new FieldReference(refName)));
      i++;
    }

    iterator = buildSchema.iterator();
    i = 0;
    while (iterator.hasNext()) {
      MaterializedField field = iterator.next();
      String refName = "build_side_" + i;
      rightExpr.add(new NamedExpression(new FieldReference(field.getName()), new FieldReference(refName)));
      buildJoinColumns.add(field.getName());
      i++;
    }
    leftExpr.forEach(e->comparators.add(Comparator.EQUALS));
  }

  @Override
  protected HashTableConfig buildHashTableConfig() {
    if (leftUpstream == IterOutcome.OK_NEW_SCHEMA
      || leftUpstream == IterOutcome.OK) {
      if (probeBatch.getSchema()
        .getSelectionVectorMode() != BatchSchema.SelectionVectorMode.NONE) {
        throw UserException.internalError(null).message(
            "Hash SetOp does not support probe batch with selection vectors.")
          .addContext("Probe batch has selection mode",
            (probeBatch.getSchema().getSelectionVectorMode()).toString())
          .build(logger);
      }
    }

    List<NamedExpression> leftExpr = Lists.newArrayListWithExpectedSize(probeSchema.getFieldCount());
    List<Comparator> comparators = Lists.newArrayListWithExpectedSize(probeSchema.getFieldCount());
    buildCompareExpression(leftExpr, comparators);
    return new HashTableConfig(
      (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE),
      true, HashTable.DEFAULT_LOAD_FACTOR, rightExpr, leftExpr, comparators,
      JoinControl.DEFAULT, true);
  }

  @Override
  public Probe createProbe() {
    // No real code generation !!
    return new HashSetOpProbeTemplate();
  }

  @Override
  public void setupProbe() throws SchemaChangeException {
    probe.setup(probeBatch, this, opType,
      isAll, leftUpstream, partitions, spilledState.getCycle(),
      container, spilledInners, buildSideIsEmpty.booleanValue(),
      numPartitions);
  }

  @Override
  public void dump() {
    logger.error(
      "HashSetOpRecordBatch[container={}, left={}, right={}, leftOutcome={}, rightOutcome={}, " +
        "SetOpType={}, IsAll={}, hashSetOpProbe={}, canSpill={}, buildSchema={}, probeSchema={}]",
      container, left, right, leftUpstream, rightUpstream, opType, isAll, probe, canSpill, buildSchema, probeSchema);
  }
}
