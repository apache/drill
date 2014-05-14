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

import org.apache.drill.exec.planner.common.DrillProjectRelBase;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.relopt.volcano.RelSubset;

public class FilterPrule extends Prule {
  public static final RelOptRule INSTANCE = new FilterPrule();

  private FilterPrule() {
    super(RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(RelNode.class)), "FilterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillFilterRel  filter = (DrillFilterRel) call.rel(0);
    final RelNode input = filter.getChild();

    RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL);
    RelNode convertedInput = convert(input, traits);

    if (convertedInput instanceof RelSubset) {
      RelSubset subset = (RelSubset) convertedInput;
      for (RelNode rel : subset.getRelList()) {
        if (!rel.getTraitSet().getTrait(DrillDistributionTraitDef.INSTANCE).equals(DrillDistributionTrait.DEFAULT)) {
          call.transformTo(new FilterPrel(filter.getCluster(), rel.getTraitSet(), convertedInput, filter.getCondition()));
        }
      }
    } else{
      call.transformTo(new FilterPrel(filter.getCluster(), convertedInput.getTraitSet(), convertedInput, filter.getCondition()));
    }
  }
}
