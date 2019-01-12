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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.LogicalSemiJoin;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class DrillSemiJoinRel extends SemiJoin implements DrillJoin, DrillRel {

  public DrillSemiJoinRel(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode left,
          RelNode right,
          RexNode condition,
          ImmutableIntList leftKeys,
          ImmutableIntList rightKeys) {
    super(cluster,
          traitSet,
          left,
          right,
          condition,
          leftKeys,
          rightKeys);
  }

  public static SemiJoin create(RelNode left, RelNode right, RexNode condition,
                                ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
    final RelOptCluster cluster = left.getCluster();
    return new DrillSemiJoinRel(cluster, cluster.traitSetOf(DrillRel.DRILL_LOGICAL), left,
            right, condition, leftKeys, rightKeys);
  }

  @Override
  public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
                                 RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    Preconditions.checkArgument(joinType == JoinRelType.INNER);
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    Preconditions.checkArgument(joinInfo.isEqui());
    return new DrillSemiJoinRel(getCluster(), traitSet, left, right, condition,
            joinInfo.leftKeys, joinInfo.rightKeys);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    List<String> fields = new ArrayList<>();
    fields.addAll(getInput(0).getRowType().getFieldNames());
    fields.addAll(getInput(1).getRowType().getFieldNames());
    final int leftCount = left.getRowType().getFieldCount();
    final List<String> leftFields = fields.subList(0, leftCount);
    final List<String> rightFields = fields.subList(leftCount, leftCount + right.getRowType().getFieldCount());

    final LogicalOperator leftOp = DrillJoinRel.implementInput(implementor, 0, 0, left, this, fields);
    final LogicalOperator rightOp = DrillJoinRel.implementInput(implementor, 1, leftCount, right, this, fields);

    Join.Builder builder = Join.builder();
    builder.type(joinType);
    builder.left(leftOp);
    builder.right(rightOp);
    List<JoinCondition> conditions = Lists.newArrayList();
    for (Pair<Integer, Integer> pair : Pair.zip(leftKeys, rightKeys)) {
      conditions.add(new JoinCondition(DrillJoinRel.EQUALITY_CONDITION,
              new FieldReference(leftFields.get(pair.left)), new FieldReference(rightFields.get(pair.right))));
    }

    return new LogicalSemiJoin(leftOp, rightOp, conditions, joinType);
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    if (getRowType().getFieldCount()
            != getSystemFieldList().size()
            + left.getRowType().getFieldCount()
            + right.getRowType().getFieldCount()) {
      return litmus.fail("field count mismatch");
    }
    if (condition != null) {
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
        return litmus.fail("condition must be boolean: {}",
                condition.getType());
      }
      // The input to the condition is a row type consisting of system
      // fields, left fields, and right fields. Very similar to the
      // output row type, except that fields have not yet been made due
      // due to outer joins.
      RexChecker checker =
              new RexChecker(
                      getCluster().getTypeFactory().builder()
                              .addAll(getSystemFieldList())
                              .addAll(getLeft().getRowType().getFieldList())
                              .addAll(getRight().getRowType().getFieldList())
                              .build(),
                      context, litmus);
      condition.accept(checker);
      if (checker.getFailureCount() > 0) {
        return litmus.fail(checker.getFailureCount()
                + " failures in condition " + condition);
      }
    }
    return litmus.succeed();
  }

  /*
    The rowtype returned by the DrillSemiJoinRel is different from that of calcite's semi-join.
    This is done because the semi-join implemented as the hash join doesn't remove the right side columns.
    Also the DrillSemiJoinRule converts the join--(scan, Agg) to DrillSemiJoinRel whose rowtype still has
    all the columns from both the relations.
   */
  @Override public RelDataType deriveRowType() {
    return SqlValidatorUtil.deriveJoinRowType(
            left.getRowType(),
            right.getRowType(),
            JoinRelType.INNER,
            getCluster().getTypeFactory(),
            null,
            ImmutableList.of());
  }

  @Override
  public boolean isSemiJoin() {
    return true;
  }
}
