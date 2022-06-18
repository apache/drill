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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.planner.DrillRelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.plan.RelOptUtil.collapseExpandedIsNotDistinctFromExpr;

public class DrillFilterJoinRules {
  /** Predicate that always returns true for any filter in OUTER join, and only true
   * for EQUAL or IS_DISTINCT_FROM over RexInputRef in INNER join. With this predicate,
   * the filter expression that return true will be kept in the JOIN OP.
   * Example:  INNER JOIN,   L.C1 = R.C2 and L.C3 + 100 = R.C4 + 100 will be kepted in JOIN.
   *                         L.C5 < R.C6 will be pulled up into Filter above JOIN.
   *           OUTER JOIN,   Keep any filter in JOIN.
  */
  public static final FilterJoinRule.Predicate EQUAL_IS_DISTINCT_FROM =
    (join, joinType, exp) -> {
      if (joinType != JoinRelType.INNER) {
        return true;  // In OUTER join, we could not pull-up the filter.
                      // All we can do is keep the filter with JOIN, and
                      // then decide whether the filter could be pushed down
                      // into LEFT/RIGHT.
      }

      List<RexNode> tmpLeftKeys = new ArrayList<>();
      List<RexNode> tmpRightKeys = new ArrayList<>();
      List<RelDataTypeField> sysFields = new ArrayList<>();
      List<Integer> filterNulls = new ArrayList<>();

      List<RelNode> inputs = Arrays.asList(join.getLeft(), join.getRight());
      final List<RexNode> nonEquiList = new ArrayList<>();

      splitJoinCondition(sysFields, inputs, exp, Arrays.asList(tmpLeftKeys, tmpRightKeys),
        filterNulls, null, nonEquiList);

      // Convert the remainders into a list that are AND'ed together.
      RexNode remaining = RexUtil.composeConjunction(
        inputs.get(0).getCluster().getRexBuilder(), nonEquiList);

      return remaining.isAlwaysTrue();
    };

  /** Predicate that always returns true for any filter in OUTER join, and only true
   * for strict EQUAL or IS_DISTINCT_FROM conditions (without any mathematical operations) over RexInputRef in INNER join.
   * With this predicate, the filter expression that return true will be kept in the JOIN OP.
   * Example:  INNER JOIN,   L.C1 = R.C2 will be kepted in JOIN.
   *                         L.C3 + 100 = R.C4 + 100, L.C5 < R.C6 will be pulled up into Filter above JOIN.
   *           OUTER JOIN,   Keep any filter in JOIN.
  */
  public static final FilterJoinRule.Predicate STRICT_EQUAL_IS_DISTINCT_FROM =
    (join, joinType, exp) -> {
      if (joinType != JoinRelType.INNER) {
        return true;
      }

      List<Integer> tmpLeftKeys = new ArrayList<>();
      List<Integer> tmpRightKeys = new ArrayList<>();
      List<Boolean> filterNulls = new ArrayList<>();

      RexNode remaining =
          RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), exp, tmpLeftKeys, tmpRightKeys, filterNulls);

      return remaining.isAlwaysTrue();
    };

  /** Rule that pushes predicates from a Filter into the Join below them. */
  public static final RelOptRule FILTER_INTO_JOIN =
    FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
      .withPredicate(EQUAL_IS_DISTINCT_FROM)
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  /** The same as above, but with Drill's operators. */
  public static final RelOptRule DRILL_FILTER_INTO_JOIN =
    FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
      .withPredicate(STRICT_EQUAL_IS_DISTINCT_FROM)
      .withRelBuilderFactory(
        DrillRelBuilder.proto(DrillRelFactories.DRILL_LOGICAL_PROJECT_FACTORY,
          DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY))
      .toRule();

  /** Rule that pushes predicates in a Join into the inputs to the join. */
  public static final RelOptRule JOIN_PUSH_CONDITION =
    FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT
      .withPredicate(EQUAL_IS_DISTINCT_FROM)
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  private static void splitJoinCondition(
    List<RelDataTypeField> sysFieldList,
    List<RelNode> inputs,
    RexNode condition,
    List<List<RexNode>> joinKeys,
    List<Integer> filterNulls,
    List<SqlOperator> rangeOp,
    List<RexNode> nonEquiList) {
    final int sysFieldCount = sysFieldList.size();
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    final ImmutableBitSet[] inputsRange = new ImmutableBitSet[inputs.size()];
    int totalFieldCount = 0;
    for (int i = 0; i < inputs.size(); i++) {
      final int firstField = totalFieldCount + sysFieldCount;
      totalFieldCount = firstField + inputs.get(i).getRowType().getFieldCount();
      inputsRange[i] = ImmutableBitSet.range(firstField, totalFieldCount);
    }

    // adjustment array
    int[] adjustments = new int[totalFieldCount];
    for (int i = 0; i < inputs.size(); i++) {
      final int adjustment = inputsRange[i].nextSetBit(0);
      for (int j = adjustment; j < inputsRange[i].length(); j++) {
        adjustments[j] = -adjustment;
      }
    }

    if (condition.getKind() == SqlKind.AND) {
      for (RexNode operand : ((RexCall) condition).getOperands()) {
        splitJoinCondition(
          sysFieldList,
          inputs,
          operand,
          joinKeys,
          filterNulls,
          rangeOp,
          nonEquiList);
      }
      return;
    }

    if (condition instanceof RexCall) {
      RexNode leftKey = null;
      RexNode rightKey = null;
      int leftInput = 0;
      int rightInput = 0;
      List<RelDataTypeField> leftFields = null;
      List<RelDataTypeField> rightFields = null;
      boolean reverse = false;

      final RexCall call =
        collapseExpandedIsNotDistinctFromExpr((RexCall) condition, rexBuilder);
      SqlKind kind = call.getKind();

      // Only consider range operators if we haven't already seen one
      if ((kind == SqlKind.EQUALS)
        || (filterNulls != null
        && kind == SqlKind.IS_NOT_DISTINCT_FROM)
        || (rangeOp != null
        && rangeOp.isEmpty()
        && (kind == SqlKind.GREATER_THAN
        || kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL))) {
        final List<RexNode> operands = call.getOperands();
        RexNode op0 = operands.get(0);
        RexNode op1 = operands.get(1);

        final ImmutableBitSet projRefs0 = RelOptUtil.InputFinder.bits(op0);
        final ImmutableBitSet projRefs1 = RelOptUtil.InputFinder.bits(op1);

        boolean foundBothInputs = false;
        for (int i = 0; i < inputs.size() && !foundBothInputs; i++) {
          if (projRefs0.intersects(inputsRange[i])
            && projRefs0.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op0;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op0;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              reverse = true;
              foundBothInputs = true;
            }
          } else if (projRefs1.intersects(inputsRange[i])
            && projRefs1.union(inputsRange[i]).equals(inputsRange[i])) {
            if (leftKey == null) {
              leftKey = op1;
              leftInput = i;
              leftFields = inputs.get(leftInput).getRowType().getFieldList();
            } else {
              rightKey = op1;
              rightInput = i;
              rightFields = inputs.get(rightInput).getRowType().getFieldList();
              foundBothInputs = true;
            }
          }
        }

        if ((leftKey != null) && (rightKey != null)) {
          // replace right Key input ref
          rightKey =
            rightKey.accept(
              new RelOptUtil.RexInputConverter(
                rexBuilder,
                rightFields,
                rightFields,
                adjustments));

          // left key only needs to be adjusted if there are system
          // fields, but do it for uniformity
          leftKey =
            leftKey.accept(
              new RelOptUtil.RexInputConverter(
                rexBuilder,
                leftFields,
                leftFields,
                adjustments));

          RelDataType leftKeyType = leftKey.getType();
          RelDataType rightKeyType = rightKey.getType();

          if (leftKeyType != rightKeyType) {
            // perform casting
            RelDataType targetKeyType =
              typeFactory.leastRestrictive(
                Arrays.asList(leftKeyType, rightKeyType));

            if (targetKeyType == null) {
              throw new AssertionError("Cannot find common type for join keys "
                + leftKey + " (type " + leftKeyType + ") and " + rightKey
                + " (type " + rightKeyType + ")");
            }

            if (leftKeyType != targetKeyType) {
              leftKey =
                rexBuilder.makeCast(targetKeyType, leftKey);
            }

            if (rightKeyType != targetKeyType) {
              rightKey =
                rexBuilder.makeCast(targetKeyType, rightKey);
            }
          }
        }
      }

      if ((rangeOp == null)
        && ((leftKey == null) || (rightKey == null))) {
        // no equality join keys found yet:
        // try transforming the condition to
        // equality "join" conditions, e.g.
        //     f(LHS) > 0 ===> ( f(LHS) > 0 ) = TRUE,
        // and make the RHS produce TRUE, but only if we're strictly
        // looking for equi-joins
        final ImmutableBitSet projRefs = RelOptUtil.InputFinder.bits(condition);
        leftKey = null;
        rightKey = null;

        boolean foundInput = false;
        for (int i = 0; i < inputs.size() && !foundInput; i++) {
          if (inputsRange[i].contains(projRefs)) {
            leftInput = i;
            leftFields = inputs.get(leftInput).getRowType().getFieldList();

            leftKey = condition.accept(
              new RelOptUtil.RexInputConverter(
                rexBuilder,
                leftFields,
                leftFields,
                adjustments));

            rightKey = rexBuilder.makeLiteral(true);

            // effectively performing an equality comparison
            kind = SqlKind.EQUALS;

            foundInput = true;
          }
        }
      }

      if ((leftKey != null) && (rightKey != null)) {
        // found suitable join keys
        // add them to key list, ensuring that if there is a
        // non-equi join predicate, it appears at the end of the
        // key list; also mark the null filtering property
        addJoinKey(
          joinKeys.get(leftInput),
          leftKey,
          (rangeOp != null) && !rangeOp.isEmpty());
        addJoinKey(
          joinKeys.get(rightInput),
          rightKey,
          (rangeOp != null) && !rangeOp.isEmpty());
        if (filterNulls != null
          && kind == SqlKind.EQUALS) {
          // nulls are considered not matching for equality comparison
          // add the position of the most recently inserted key
          filterNulls.add(joinKeys.get(leftInput).size() - 1);
        }
        if (rangeOp != null
          && kind != SqlKind.EQUALS
          && kind != SqlKind.IS_DISTINCT_FROM) {
          SqlOperator op = call.getOperator();
          if (reverse) {
            op = requireNonNull(op.reverse());
          }
          rangeOp.add(op);
        }
        return;
      } // else fall through and add this condition as nonEqui condition
    }

    // The operator is not of RexCall type
    // So we fail. Fall through.
    // Add this condition to the list of non-equi-join conditions.
    nonEquiList.add(condition);
  }

  private static void addJoinKey(
    List<RexNode> joinKeyList,
    RexNode key,
    boolean preserveLastElementInList) {
    if (!joinKeyList.isEmpty() && preserveLastElementInList) {
      joinKeyList.add(joinKeyList.size() - 1, key);
    } else {
      joinKeyList.add(key);
    }
  }
}
