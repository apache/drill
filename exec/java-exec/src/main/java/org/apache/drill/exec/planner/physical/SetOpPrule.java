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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.exec.planner.logical.DrillExceptRel;
import org.apache.drill.exec.planner.logical.DrillIntersectRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

public class SetOpPrule extends Prule {
  public static final List<RelOptRule> DIST_INSTANCES = Arrays.asList(
    new SetOpPrule(RelOptHelper.any(DrillExceptRel.class), "Prel.HashExceptDistPrule", true),
    new SetOpPrule(RelOptHelper.any(DrillIntersectRel.class), "Prel.HashIntersectDistPrule", true));
  public static final List<RelOptRule> BROADCAST_INSTANCES = Arrays.asList(
    new SetOpPrule(RelOptHelper.any(DrillExceptRel.class), "Prel.HashExceptBroadcastPrule", false),
    new SetOpPrule(RelOptHelper.any(DrillIntersectRel.class), "Prel.HashIntersectBroadcastPrule", false));
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();
  private final boolean isDist;

  private SetOpPrule(RelOptRuleOperand operand, String description, boolean isDist) {
    super(operand, description);
    this.isDist = isDist;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SetOp setOp = call.rel(0);
    Preconditions.checkArgument(setOp.getInputs().size() == 2, "inputs of set op must be two items.");

    try {
      if(isDist){
        createDistBothPlan(call, setOp, setOp.getInput(0), setOp.getInput(1));
      }else{
        if (checkBroadcastConditions(setOp.getCluster(), setOp.getInput(0), setOp.getInput(1))) {
          createBroadcastPlan(call, setOp, setOp.getInput(0), setOp.getInput(1));
        }
      }
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

  private void createBroadcastPlan(final RelOptRuleCall call, final SetOp setOp,
    final RelNode left, final RelNode right) throws InvalidRelException {

    RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distBroadcastRight);

    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);
    final RelTraitSet traitSet = PrelUtil.removeCollation(convertedLeft.getTraitSet(), call);
    call.transformTo(new SetOpPrel(setOp.getCluster(), traitSet, ImmutableList.of(convertedLeft, convertedRight), setOp.kind, setOp.all));
  }

  private boolean checkBroadcastConditions(RelOptCluster cluster, RelNode left, RelNode right) {
    double estimatedRightRowCount = RelMetadataQuery.instance().getRowCount(right);
    return estimatedRightRowCount < PrelUtil.getSettings(cluster).getBroadcastThreshold()
      && !DrillDistributionTrait.SINGLETON.equals(left.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE));
  }

  private void createDistBothPlan(RelOptRuleCall call, SetOp setOp, RelNode left, RelNode right)
    throws InvalidRelException {
    int i = 0;
    List<DistributionField> distFields = Lists.newArrayList();
    while(i < left.getRowType().getFieldCount()) {
        distFields.add(new DistributionField(i));
        i++;
    }
    DrillDistributionTrait distributionTrait = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
      distFields);
    createDistBothPlan(call, setOp, left, right, distributionTrait);
  }

  private void createDistBothPlan(RelOptRuleCall call, SetOp setOp, RelNode left, RelNode right,
    DrillDistributionTrait distributionTrait)
    throws InvalidRelException {

    RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distributionTrait);
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distributionTrait);

    final RelNode convertedLeft = convert(left, traitsLeft);
    final RelNode convertedRight = convert(right, traitsRight);
    final RelTraitSet traitSet = PrelUtil.removeCollation(traitsLeft, call);

    call.transformTo(new SetOpPrel(setOp.getCluster(), traitSet, ImmutableList.of(convertedLeft, convertedRight), setOp.kind, setOp.all));
  }
}
