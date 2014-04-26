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

import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillJoinRule;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class MergeJoinPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new MergeJoinPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private MergeJoinPrule() {
    super(
        RelOptHelper.some(DrillJoinRel.class, RelOptHelper.any(RelNode.class), RelOptHelper.any(RelNode.class)),
        "Prel.MergeJoinPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillJoinRel join = (DrillJoinRel) call.rel(0);
    
    try {            
      if (join.getCondition().isAlwaysTrue()) {
        throw new InvalidRelException("MergeJoinPrel does not support cartesian product join");
      }
  
      final RelNode left = call.rel(1);
      final RelNode right = call.rel(2);
  
      RelCollation collationLeft = getCollation(join.getLeftKeys());
      RelCollation collationRight = getCollation(join.getRightKeys());
      DrillDistributionTrait hashLeftPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getLeftKeys())));
      DrillDistributionTrait hashRightPartition = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(join.getRightKeys())));
      
      final RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationLeft).plus(hashLeftPartition);   
      final RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collationRight).plus(hashRightPartition);
      
      final RelNode convertedLeft = convert(left, traitsLeft);
      final RelNode convertedRight = convert(right, traitsRight);
      
    
      MergeJoinPrel newJoin = new MergeJoinPrel(join.getCluster(), traitsLeft, convertedLeft, convertedRight, join.getCondition(),
                                                join.getJoinType());
      call.transformTo(newJoin);
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

  private List<DistributionField> getDistributionField(List<Integer> keys) {
    List<DistributionField> distFields = Lists.newArrayList();

    for (int key : keys) {
      distFields.add(new DistributionField(key));
    }
     
    return distFields;
  }

}
