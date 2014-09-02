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

import java.util.List;
import java.util.logging.Logger;

import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

/**
 * Rule that converts a {@link JoinRel} to a {@link DrillJoinRel}, which is implemented by Drill "join" operation.
 */
public class DrillJoinRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillJoinRule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private DrillJoinRule() {
    super(
        RelOptHelper.any(JoinRel.class, Convention.NONE),
        "DrillJoinRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final JoinRel join = (JoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();
    final RelTraitSet traits = join.getTraitSet().plus(DrillRel.DRILL_LOGICAL);

    final RelNode convertedLeft = convert(left, left.getTraitSet().plus(DrillRel.DRILL_LOGICAL));
    final RelNode convertedRight = convert(right, right.getTraitSet().plus(DrillRel.DRILL_LOGICAL));

    List<Integer> leftKeys = Lists.newArrayList();
    List<Integer> rightKeys = Lists.newArrayList();
    int numLeftFields = convertedLeft.getRowType().getFieldCount();

    boolean addFilter = false;
    RexNode origJoinCondition = join.getCondition();
    RexNode newJoinCondition = origJoinCondition;

    RexNode remaining = RelOptUtil.splitJoinCondition(convertedLeft, convertedRight, origJoinCondition, leftKeys, rightKeys);
    boolean hasEquijoins = (leftKeys.size() == rightKeys.size() && leftKeys.size() > 0) ? true : false;

    // If the join involves equijoins and non-equijoins, then we can process the non-equijoins through
    // a filter right after the join
    // DRILL-1337: We can only pull up a non-equivjoin filter for INNER join.
    // For OUTER join, pulling up a non-eqivjoin filter will lead to incorrectly discarding qualified rows.
    if (! remaining.isAlwaysTrue()) {
      if (hasEquijoins && join.getJoinType()== JoinRelType.INNER) {
        addFilter = true;
        List<RexNode> equijoinList = Lists.newArrayList();
        List<RelDataTypeField> leftTypes = convertedLeft.getRowType().getFieldList();
        List<RelDataTypeField> rightTypes = convertedRight.getRowType().getFieldList();
        RexBuilder builder = join.getCluster().getRexBuilder();

        for (int i=0; i < leftKeys.size(); i++) {
          int leftKeyOrdinal = leftKeys.get(i).intValue();
          int rightKeyOrdinal = rightKeys.get(i).intValue();

          equijoinList.add(builder.makeCall(
              SqlStdOperatorTable.EQUALS,
              builder.makeInputRef(leftTypes.get(leftKeyOrdinal).getType(), leftKeyOrdinal),
              builder.makeInputRef(rightTypes.get(rightKeyOrdinal).getType(), rightKeyOrdinal + numLeftFields)
             ) );
        }
        newJoinCondition = RexUtil.composeConjunction(builder, equijoinList, false);
      } else {
//        tracer.warning("Non-equijoins are only supported in the presence of an equijoin.");
        return;
      }
    }
    //else {
    //
    //  return;
    // }

    try {
      if (!addFilter) {
       RelNode joinRel = new DrillJoinRel(join.getCluster(), traits, convertedLeft, convertedRight, origJoinCondition,
                                         join.getJoinType(), leftKeys, rightKeys, false);
       call.transformTo(joinRel);
      } else {
        RelNode joinRel = new DrillJoinRel(join.getCluster(), traits, convertedLeft, convertedRight, newJoinCondition,
                                           join.getJoinType(), leftKeys, rightKeys, false);
        call.transformTo(new DrillFilterRel(join.getCluster(), traits, joinRel, remaining));
      }
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }
}
