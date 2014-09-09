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
package org.apache.drill.exec.planner.physical;

import java.util.List;
import java.util.logging.Logger;

import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

public class MergeJoinPrule extends JoinPruleBase {
  public static final RelOptRule INSTANCE = new MergeJoinPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private MergeJoinPrule() {
    super(
        RelOptHelper.any(DrillJoinRel.class),
        "Prel.MergeJoinPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isMergeJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();

    if (!checkPreconditions(join, left, right)) {
      return;
    }

    boolean hashSingleKey = PrelUtil.getPlannerSettings(call.getPlanner()).isHashSingleKey();

    try {
      RelCollation collationLeft = getCollation(join.getLeftKeys());
      RelCollation collationRight = getCollation(join.getRightKeys());

      createDistBothPlan(call, join, PhysicalJoinType.MERGE_JOIN, left, right, collationLeft, collationRight, hashSingleKey);

      if (checkBroadcastConditions(call.getPlanner(), join, left, right)) {
        createBroadcastPlan(call, join, PhysicalJoinType.MERGE_JOIN, left, right, collationLeft, collationRight);
      }

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

  private RelCollation getCollation(List<Integer> keys){
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int key : keys) {
      fields.add(new RelFieldCollation(key));
    }
    return RelCollationImpl.of(fields);
  }

}
