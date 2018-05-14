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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

public class DrillCorrelateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillCorrelateRule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private DrillCorrelateRule() {
    super(RelOptHelper.any(LogicalCorrelate.class, Convention.NONE),
        DrillRelFactories.LOGICAL_BUILDER,
        "DrillCorrelateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalCorrelate correlate = call.rel(0);
    final RelNode left = correlate.getLeft();
    final RelNode right = correlate.getRight();
    final RelNode convertedLeft = convert(left, left.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());
    final RelNode convertedRight = convert(right, right.getTraitSet().plus(DrillRel.DRILL_LOGICAL).simplify());

    final RelTraitSet traits = correlate.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    DrillCorrelateRel correlateRel = new DrillCorrelateRel(correlate.getCluster(),
        traits, convertedLeft, convertedRight, correlate.getCorrelationId(),
        correlate.getRequiredColumns(), correlate.getJoinType());
    call.transformTo(correlateRel);
  }
}
