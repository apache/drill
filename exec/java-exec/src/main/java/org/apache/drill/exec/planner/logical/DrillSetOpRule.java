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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories.SetOpFactory;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Rule that converts {@link LogicalIntersect} or {@link LogicalMinus} to
 * {@link DrillIntersectRel} or {@link DrillExceptRel}.
 */
public class DrillSetOpRule extends RelOptRule {
  private final SetOpFactory setOpFactory;
  public static final List<RelOptRule> INSTANCES = Arrays.asList(
      new DrillSetOpRule(RelOptHelper.any(LogicalIntersect.class, Convention.NONE), "DrillIntersectRelRule", DrillRelFactories.DRILL_LOGICAL_SET_OP_FACTORY),
      new DrillSetOpRule(RelOptHelper.any(LogicalMinus.class, Convention.NONE), "DrillExceptRelRule", DrillRelFactories.DRILL_LOGICAL_SET_OP_FACTORY)
  );

  public DrillSetOpRule(RelOptRuleOperand operand, String description, SetOpFactory setOpFactory) {
    super(operand, description);
    this.setOpFactory = setOpFactory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SetOp setOp = call.rel(0);
    final List<RelNode> convertedInputs = new ArrayList<>();
    for (RelNode input : setOp.getInputs()) {
      RelNode convertedInput = convert(input, input.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
      convertedInputs.add(convertedInput);
    }
    RelNode newRelNode = this.setOpFactory.createSetOp(setOp.kind, convertedInputs, setOp.all);
    if (newRelNode != null) {
      call.transformTo(newRelNode);
    }
  }

}
