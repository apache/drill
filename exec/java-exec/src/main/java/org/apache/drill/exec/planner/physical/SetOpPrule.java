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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.exec.planner.logical.DrillExceptRel;
import org.apache.drill.exec.planner.logical.DrillIntersectRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.drill.exec.ExecConstants.EXCEPT_ADD_AGG_BELOW;
import static org.apache.drill.exec.planner.physical.AggPruleBase.remapGroupSet;

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
        createDistBothPlan(call);
      }else{
        if (checkBroadcastConditions(setOp.getCluster(), setOp.getInput(0), setOp.getInput(1))) {
          createBroadcastPlan(call);
        }
      }
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

  private void createDistBothPlan(RelOptRuleCall call)
    throws InvalidRelException {
    int i = 0;
    int fieldCount = call.rel(0).getInput(0).getRowType().getFieldCount();
    List<DistributionField> distFields = Lists.newArrayList();
    while(i < fieldCount) {
      distFields.add(new DistributionField(i));
      i++;
    }
    DrillDistributionTrait distributionTrait = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
      distFields);
    createPlan(call, distributionTrait);

    if (!PrelUtil.getPlannerSettings(call.getPlanner()).isHashSingleKey()) {
      return;
    }
    if (fieldCount > 1) {
      for (int j = 0; j < fieldCount; j++) {
        distributionTrait = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
          ImmutableList.of(new DistributionField(j)));
        createPlan(call, distributionTrait);
      }
    }
  }

  private boolean checkBroadcastConditions(RelOptCluster cluster, RelNode left, RelNode right) {
    double estimatedRightRowCount = RelMetadataQuery.instance().getRowCount(right);
    return estimatedRightRowCount < PrelUtil.getSettings(cluster).getBroadcastThreshold()
      && !DrillDistributionTrait.SINGLETON.equals(left.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE));
  }

  private void createBroadcastPlan(final RelOptRuleCall call) throws InvalidRelException {
    DrillDistributionTrait distBroadcastRight = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.BROADCAST_DISTRIBUTED);
    createPlan(call, distBroadcastRight);
  }

  private void createPlan(final RelOptRuleCall call, DrillDistributionTrait setOpTrait) throws InvalidRelException {
    if (needAddAgg(call.rel(0))) {
      ImmutableBitSet groupSet = ImmutableBitSet.range(0, call.rel(0).getInput(0).getRowType().getFieldList().size());

      // hashAgg: hash distribute on all grouping keys
      DrillDistributionTrait distOnAllKeys =
        new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
          ImmutableList.copyOf(getDistributionField(groupSet, true /* get all grouping keys */)));
      RelTraitSet aggTraits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnAllKeys);
      createTransformRequest(call, aggTraits, setOpTrait, null);

      // hashAgg: hash distribute on single grouping key
      DrillDistributionTrait distOnOneKey =
        new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED,
          ImmutableList.copyOf(getDistributionField(groupSet, false /* get single grouping key */)));
      aggTraits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnOneKey);
      createTransformRequest(call, aggTraits, setOpTrait, null);

      // streamAgg: hash distribute on all grouping keys
      final RelCollation collation = getCollation(groupSet);
      aggTraits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(distOnAllKeys);
      createTransformRequest(call, aggTraits, setOpTrait, collation);
    } else {
      call.transformTo(buildSetOpPrel(call, null, setOpTrait));
    }
  }

  private boolean needAddAgg(SetOp setOp) {
    if (setOp.all || !(setOp instanceof DrillExceptRel)) {
      return false;
    }
    Set<ImmutableBitSet> uniqueKeys = setOp.getCluster().getMetadataQuery().getUniqueKeys(((RelSubset)setOp.getInput(0)).getBestOrOriginal());
    if (uniqueKeys == null) {
      return true;
    }
    return uniqueKeys.size() < setOp.getRowType().getFieldCount();
  }

  private void createTransformRequest(RelOptRuleCall call, RelTraitSet aggTraits, DrillDistributionTrait setOpTrait, RelCollation collation) throws InvalidRelException {
    boolean addAggBelow = PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(EXCEPT_ADD_AGG_BELOW);
    RelNode outputRel;
    if (addAggBelow) {
      AggPrelBase newAgg = buildAggPrel(call, call.rel(0).getInput(0), aggTraits, collation);
      outputRel = buildSetOpPrel(call, newAgg, setOpTrait);
    } else {
      SetOpPrel setOpPrel = buildSetOpPrel(call, null, setOpTrait);
      outputRel = buildAggPrel(call, setOpPrel, aggTraits, collation);
    }
    call.transformTo(outputRel);
  }

  private AggPrelBase buildAggPrel(RelOptRuleCall call, RelNode input, RelTraitSet aggTraits, RelCollation collation) throws InvalidRelException {
    final DrillExceptRel drillExceptRel = call.rel(0);
    ImmutableBitSet groupSet = ImmutableBitSet.range(0, drillExceptRel.getInput(0).getRowType().getFieldList().size());
    if (collation !=  null) {
      final RelNode convertedInput = convert(input, aggTraits);
      return new StreamAggPrel(
        drillExceptRel.getCluster(),
        aggTraits,
        convertedInput,
        groupSet,
        ImmutableList.of(),
        ImmutableList.of(),
        AggPrelBase.OperatorPhase.PHASE_1of1);
    } else {
      RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, aggTraits));
      return new HashAggPrel(
        drillExceptRel.getCluster(),
        aggTraits,
        convertedInput,
        groupSet,
        ImmutableList.of(),
        ImmutableList.of(),
        AggPrelBase.OperatorPhase.PHASE_1of1);
    }
  }

  private SetOpPrel buildSetOpPrel(RelOptRuleCall call, RelNode convertedLeft, DrillDistributionTrait setOpTrait) throws InvalidRelException {
    final SetOp setOp = call.rel(0);
    final RelNode right = setOp.getInput(1);
    RelTraitSet traitsRight = right.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(setOpTrait);
    final RelNode convertedRight = convert(right, traitsRight);

    if (convertedLeft == null) {
      final RelNode left = setOp.getInput(0);
      RelTraitSet traitsLeft = left.getTraitSet().plus(Prel.DRILL_PHYSICAL);
      if (DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED.equals(setOpTrait.getType())) {
        traitsLeft.plus(setOpTrait);
      }
      convertedLeft = convert(left, traitsLeft);
    }
    final RelTraitSet traitSet = PrelUtil.removeCollation(convertedLeft.getTraitSet(), call);
    return new SetOpPrel(convertedLeft.getCluster(), traitSet, ImmutableList.of(convertedLeft, convertedRight), setOp.kind, setOp.all);
  }

  private List<DistributionField> getDistributionField(ImmutableBitSet groupSet, boolean allFields) {
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int group : remapGroupSet(groupSet)) {
      DistributionField field = new DistributionField(group);
      groupByFields.add(field);

      if (!allFields && groupByFields.size() == 1) {
        // TODO: if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

  private RelCollation getCollation(ImmutableBitSet groupSet) {

    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group : BitSets.toIter(groupSet)) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollations.of(fields);
  }
}
