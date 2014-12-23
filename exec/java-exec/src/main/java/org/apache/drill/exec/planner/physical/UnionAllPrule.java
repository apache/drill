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

import org.apache.drill.exec.planner.logical.DrillUnionRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.Lists;

public class UnionAllPrule extends Prule {
  public static final RelOptRule INSTANCE = new UnionAllPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

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
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL);

    try {
      for (int i = 0; i < inputs.size(); i++) {
        RelNode convertedInput = convert(inputs.get(i), PrelUtil.fixTraits(call, traits));
        convertedInputList.add(convertedInput);
      }

      traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
      UnionAllPrel unionAll =
          new UnionAllPrel(union.getCluster(), traits, convertedInputList,
              false /* compatibility already checked during logical phase */);

      call.transformTo(unionAll);

    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

}
