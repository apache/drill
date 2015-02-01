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

import java.util.logging.Logger;

import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.trace.EigenbaseTrace;

public class HashJoinPrule extends JoinPruleBase {
  public static final RelOptRule DIST_INSTANCE = new HashJoinPrule("Prel.HashJoinDistPrule", RelOptHelper.any(DrillJoinRel.class), true);
  public static final RelOptRule BROADCAST_INSTANCE = new HashJoinPrule("Prel.HashJoinBroadcastPrule", RelOptHelper.any(DrillJoinRel.class), false);

  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private final boolean isDist;
  private HashJoinPrule(String name, RelOptRuleOperand operand, boolean isDist) {
    super(operand, name);
    this.isDist = isDist;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    return settings.isMemoryEstimationEnabled() || settings.isHashJoinEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (!PrelUtil.getPlannerSettings(call.getPlanner()).isHashJoinEnabled()) {
      return;
    }

    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = join.getLeft();
    final RelNode right = join.getRight();

    if (!checkPreconditions(join, left, right)) {
      return;
    }

    boolean hashSingleKey = PrelUtil.getPlannerSettings(call.getPlanner()).isHashSingleKey();

    try {

      if(isDist){
        createDistBothPlan(call, join, PhysicalJoinType.HASH_JOIN, left, right, null /* left collation */, null /* right collation */, hashSingleKey);
      }else{
        if (checkBroadcastConditions(call.getPlanner(), join, left, right)) {
          createBroadcastPlan(call, join, PhysicalJoinType.HASH_JOIN, left, right, null /* left collation */, null /* right collation */);
        }
      }


    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

}
