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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.exec.planner.logical.DrillUnionRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class UnionAllPrule extends Prule {
  public static final RelOptRule INSTANCE = new UnionAllPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private UnionAllPrule() {
    super(
        RelOptHelper.any(DrillUnionRel.class), "Prel.UnionAllPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DrillUnionRel union = (DrillUnionRel) call.rel(0);
    return (! union.isDistinct());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillUnionRel union = (DrillUnionRel) call.rel(0);
    final List<RelNode> inputs = union.getInputs();
    List<RelNode> convertedInputList = Lists.newArrayList();

    /**
     * Generally speaking, it is unsafe to let workload be distributed across multiple union all:
     * When Drill reads an empty file and decides not to skip it (if this empty file is the only file),
     * it produces a record batch with the following properties:
     * 1. The column names are from the items in the select-list
     * 2. The column types are optional integer
     * 3. There is no record in the record batch
     *
     * Therefore, there is no way for Drill to distinguish between a record batch, produced by an empty file, and a
     * record batch, which just happens to have no record (possibly due to limit 0 or filter in the subqueries).
     *
     * If there is one union all whose left or right foot happens to receive one record batch with zero record,
     * then this union all could infer an output schema which is different from the other union all.
     */
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);

    try {
      for (int i = 0; i < inputs.size(); i++) {
        RelNode convertedInput = convert(inputs.get(i), PrelUtil.fixTraits(call, traits));
        convertedInputList.add(convertedInput);
      }

      Preconditions.checkArgument(convertedInputList.size() >= 2, "Union list must be at least two items.");
      RelNode left = convertedInputList.get(0);
      for (int i = 1; i < convertedInputList.size(); i++) {
        left = new UnionAllPrel(union.getCluster(), traits, ImmutableList.of(left, convertedInputList.get(i)),
            false /* compatibility already checked during logical phase */);

      }
      call.transformTo(left);

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

}
