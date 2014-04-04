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
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;

public class HashJoinPrule extends JoinPruleBase {
  public static final RelOptRule INSTANCE = new HashJoinPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private HashJoinPrule() {
    super(
        RelOptHelper.some(DrillJoinRel.class, RelOptHelper.any(RelNode.class), RelOptHelper.any(RelNode.class)),
        "Prel.HashJoinPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    final RelNode left = call.rel(1);
    final RelNode right = call.rel(2);
    
    if (!checkPreconditions(join, left, right)) {
      return;
    }
    
    try {
      // Create transform request for HashJoin plan with both children HASH distributed
      DrillDistributionTrait hashLeftPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getLeftKeys())));
      DrillDistributionTrait hashRightPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getRightKeys())));

      RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashLeftPartition);   
      RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashRightPartition);

      createTransformRequest(call, join, left, right, traitsLeft, traitsRight);

      // Create transform request for HashJoin plan with left child ANY distributed and right child BROADCAST distributed
      /// TODO: ANY distribution seems to create some problems..need to revisit
      // DrillDistributionTrait distAnyLeft = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.ANY);
      DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
      // traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distAnyLeft);
      traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distBroadcastRight);

      //temporarily not generate this plan
      //createTransformRequest(call, join, left, right, traitsLeft, traitsRight);

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

  private void createTransformRequest(RelOptRuleCall call, DrillJoinRel join, 
                                      RelNode left, RelNode right, 
                                      RelTraitSet traitsLeft, RelTraitSet traitsRight)
    throws InvalidRelException { 

    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);
      
    HashJoinPrel newJoin = new HashJoinPrel(join.getCluster(), traitsLeft, 
                                              convertedLeft, convertedRight, join.getCondition(),
                                              join.getJoinType());
    call.transformTo(newJoin) ;
  }

}
