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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.slf4j.Logger;

import static org.apache.drill.exec.ExecConstants.EXCEPT_ADD_AGG_BELOW;

/**
 * Rule that try to add agg for Except set op.
 */
public class DrillAddAggForExceptRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillAddAggForExceptRule(RelOptHelper.any(DrillExceptRel.class), "DrillAddAggForExceptRule");
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  public DrillAddAggForExceptRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DrillExceptRel drillExceptRel = call.rel(0);
    return !drillExceptRel.all && !drillExceptRel.isAggAdded() && !findAggRel(drillExceptRel.getInput(0));
  }

  private boolean findAggRel(RelNode relNode) {
    if (relNode instanceof HepRelVertex) {
      return findAggRel(((HepRelVertex) relNode).getCurrentRel());
    }
    if (relNode instanceof DrillAggregateRel) {
      return true;
    }
    if (relNode.getInputs().size() == 1 && relNode.getInput(0) != null) {
      return findAggRel(relNode.getInput(0));
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillExceptRel drillExceptRel = call.rel(0);
    boolean addAggBelow = PrelUtil.getPlannerSettings(call.getPlanner()).getOptions().getOption(EXCEPT_ADD_AGG_BELOW);
    if (addAggBelow) {
      RelNode aggNode = new DrillAggregateRel(drillExceptRel.getCluster(), drillExceptRel.getTraitSet(), drillExceptRel.getInput(0),
        ImmutableBitSet.range(0, drillExceptRel.getInput(0).getRowType().getFieldList().size()), ImmutableList.of(), ImmutableList.of());
      call.transformTo(drillExceptRel.copy(ImmutableList.of(aggNode, drillExceptRel.getInput(1)), true));
    } else {
      call.transformTo(new DrillAggregateRel(drillExceptRel.getCluster(), drillExceptRel.getTraitSet(), drillExceptRel.copy(true),
        ImmutableBitSet.range(0, drillExceptRel.getRowType().getFieldList().size()), ImmutableList.of(), ImmutableList.of()));
    }
  }
}
