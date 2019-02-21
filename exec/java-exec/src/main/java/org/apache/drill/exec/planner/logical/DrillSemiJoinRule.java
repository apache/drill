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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Planner rule that creates a {@code DrillSemiJoinRel} from a
 * {@link org.apache.calcite.rel.core.Join} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 */
public abstract class DrillSemiJoinRule extends RelOptRule {
  private static final Predicate<Join> IS_LEFT_OR_INNER =
          join -> {
            switch (join.getJoinType()) {
              case LEFT:
              case INNER:
                return true;
              default:
                return false;
            }
          };

  public static final DrillSemiJoinRule JOIN =
          new JoinToSemiJoinRule(Join.class, Aggregate.class,
                  DrillRelFactories.LOGICAL_BUILDER, "DrillSemiJoinRule:join");

  protected DrillSemiJoinRule(Class<Join> joinClass, Class<Aggregate> aggregateClass,
                         RelBuilderFactory relBuilderFactory, String description) {
    super(operandJ(joinClass, null, IS_LEFT_OR_INNER,
                    some(operand(RelNode.class, any()),
                            operandJ(aggregateClass, null, r -> true, any()))),
            relBuilderFactory, description);
  }

  protected void perform(RelOptRuleCall call, Project project,
                         Join join, RelNode left, Aggregate aggregate) {
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (project != null) {
      final ImmutableBitSet bits =
              RelOptUtil.InputFinder.bits(project.getProjects(), null);
      final ImmutableBitSet rightBits =
              ImmutableBitSet.range(left.getRowType().getFieldCount(),
                      join.getRowType().getFieldCount());
      if (bits.intersects(rightBits)) {
        return;
      }
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
            ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    if (!joinInfo.isEqui()) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    switch (join.getJoinType()) {
      case INNER:
        final List<Integer> newRightKeyBuilder = new ArrayList<>();
        final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
        for (int key : joinInfo.rightKeys) {
          newRightKeyBuilder.add(aggregateKeys.get(key));
        }
        final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
        relBuilder.push(aggregate.getInput());
        final RexNode newCondition =
                RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
                        joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
                        rexBuilder);
        relBuilder.semiJoin(newCondition);
        break;

      case LEFT:
        // The right-hand side produces no more than 1 row (because of the
        // Aggregate) and no fewer than 1 row (because of LEFT), and therefore
        // we can eliminate the semi-join.
        break;

      default:
        throw new AssertionError(join.getJoinType());
    }
    if (project != null) {
      relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    }
    call.transformTo(relBuilder.build());
  }

  /** DrillSemiJoinRule that matches a Join with an Aggregate (without agg functions)
   *  as its right child.
   */
  public static class JoinToSemiJoinRule extends DrillSemiJoinRule {

    /** Creates a JoinToSemiJoinRule. */
    public JoinToSemiJoinRule(
            Class<Join> joinClass, Class<Aggregate> aggregateClass,
            RelBuilderFactory relBuilderFactory, String description) {
      super(joinClass, aggregateClass, relBuilderFactory, description);
    }
  }

  /**
   * Check for the row schema if they aggregate's output rowtype is different
   * from its input rowtype then do not convert the join to a semi join.
   */
  private static boolean isRowTypeSame(RelNode join, RelNode left, RelNode rightInput) {
    return join.getRowType().getFieldCount() == left.getRowType().getFieldCount() +
                                                rightInput.getRowType().getFieldCount();
  }

  /**
   * Check if the join condition is a simple equality condition. This check
   * is needed because joininfo.isEqui treats IS_NOT_DISTINCT_FROM as equi
   * condition.
   */
  private static boolean isSimpleJoinCondition(RexNode joinCondition) {
    if (joinCondition.isAlwaysFalse() || joinCondition.isAlwaysTrue()) {
      return false;
    }

    List<RexNode> conjuncts = RelOptUtil.conjunctions(joinCondition);
    for (RexNode condition : conjuncts) {
      if (condition.getKind() != SqlKind.EQUALS) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    DrillAggregateRel agg = call.rel(2);
    if (agg.getAggCallList().size() != 0) { return false; }
    return  isSimpleJoinCondition(join.getCondition()) &&
            isRowTypeSame(join, call.rel(1), call.rel(2).getInput(0));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode left = call.rel(1);
    final Aggregate aggregate = call.rel(2);
    perform(call, null, join, left, aggregate);
  }
}
