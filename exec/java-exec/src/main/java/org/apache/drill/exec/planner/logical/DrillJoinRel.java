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
package org.apache.drill.exec.planner.logical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Project;
import org.apache.drill.exec.planner.common.DrillJoinRelBase;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Pair;

import com.google.common.collect.Lists;

/**
 * Join implemented in Drill.
 */
public class DrillJoinRel extends DrillJoinRelBase implements DrillRel {

  /** Creates a DrillJoinRel. */
  public DrillJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys);
    if (!remaining.isAlwaysTrue() && (leftKeys.size() == 0 || rightKeys.size() == 0)) {
      throw new InvalidRelException("DrillJoinRel only supports equi-join");
    }
  }

  public DrillJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition,
      JoinRelType joinType, List<Integer> leftKeys, List<Integer> rightKeys, boolean checkCartesian) throws InvalidRelException {
    super(cluster, traits, left, right, condition, joinType);

    assert (leftKeys != null && rightKeys != null);

    if (checkCartesian)  {
      List<Integer> tmpLeftKeys = Lists.newArrayList();
      List<Integer> tmpRightKeys = Lists.newArrayList();
      RexNode remaining = RelOptUtil.splitJoinCondition(left, right, condition, tmpLeftKeys, tmpRightKeys);
      if (!remaining.isAlwaysTrue() && (tmpLeftKeys.size() == 0 || tmpRightKeys.size() == 0)) {
        throw new InvalidRelException("DrillJoinRel only supports equi-join");
      }
    }
    this.leftKeys = leftKeys;
    this.rightKeys = rightKeys;
  }


  @Override
  public DrillJoinRel copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      return new DrillJoinRel(getCluster(), traitSet, left, right, condition, joinType);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final List<String> fields = getRowType().getFieldNames();
    assert isUnique(fields);
    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, fields.size());

    final LogicalOperator leftOp = implementInput(implementor, 0, 0, left);
    final LogicalOperator rightOp = implementInput(implementor, 1, leftCount, right);

    Join.Builder builder = Join.builder();
    builder.type(joinType);
    builder.left(leftOp);
    builder.right(rightOp);

    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      builder.addCondition("==", new FieldReference(leftFields.get(pair.left)), new FieldReference(rightFields.get(pair.right)));
    }

    return builder.build();
  }

  /**
   * Check to make sure that the fields of the inputs are the same as the output field names.  If not, insert a project renaming them.
   * @param implementor
   * @param i
   * @param offset
   * @param input
   * @return
   */
  private LogicalOperator implementInput(DrillImplementor implementor, int i, int offset, RelNode input) {
    final LogicalOperator inputOp = implementor.visitChild(this, i, input);
    assert uniqueFieldNames(input.getRowType());
    final List<String> fields = getRowType().getFieldNames();
    final List<String> inputFields = input.getRowType().getFieldNames();
    final List<String> outputFields = fields.subList(offset, offset + inputFields.size());
    if (!outputFields.equals(inputFields)) {
      // Ensure that input field names are the same as output field names.
      // If there are duplicate field names on left and right, fields will get
      // lost.
      return rename(implementor, inputOp, inputFields, outputFields);
    } else {
      return inputOp;
    }
  }

  private LogicalOperator rename(DrillImplementor implementor, LogicalOperator inputOp, List<String> inputFields, List<String> outputFields) {
    Project.Builder builder = Project.builder();
    builder.setInput(inputOp);
    for (Pair<String, String> pair : Pair.zip(inputFields, outputFields)) {
      builder.addExpr(new FieldReference(pair.right), new FieldReference(pair.left));
    }
    return builder.build();
  }

  public static DrillJoinRel convert(Join join, ConversionContext context) throws InvalidRelException{
    RelNode left = context.toRel(join.getLeft());
    RelNode right = context.toRel(join.getRight());

    List<RexNode> joinConditions = new ArrayList<RexNode>();
    // right fields appear after the LHS fields.
    final int rightInputOffset = left.getRowType().getFieldCount();
    for (JoinCondition condition : join.getConditions()) {
      RelDataTypeField leftField = left.getRowType().getField(ExprHelper.getFieldName(condition.getLeft()), true);
      RelDataTypeField rightField = right.getRowType().getField(ExprHelper.getFieldName(condition.getRight()), true);
        joinConditions.add(
            context.getRexBuilder().makeCall(
                SqlStdOperatorTable.EQUALS,
                context.getRexBuilder().makeInputRef(leftField.getType(), leftField.getIndex()),
                context.getRexBuilder().makeInputRef(rightField.getType(), rightInputOffset + rightField.getIndex())
                )
                );
    }
    RexNode rexCondition = RexUtil.composeConjunction(context.getRexBuilder(), joinConditions, false);
    DrillJoinRel joinRel = new DrillJoinRel(context.getCluster(), context.getLogicalTraits(), left, right, rexCondition, join.getJoinType());

    return joinRel;
  }

}
