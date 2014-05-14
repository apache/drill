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

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelTraitSet;

public abstract class Prule extends RelOptRule{
  public Prule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public Prule(RelOptRuleOperand operand) {
    super(operand);
  }


  public static RelNode convert(RelNode rel, RelTraitSet toTraits){

    PlannerSettings settings = PrelUtil.getSettings(rel.getCluster());
    if(settings.isSingleMode()){
      toTraits = toTraits.replace(DrillDistributionTrait.ANY);
    }

    return RelOptRule.convert(rel, toTraits);
  }

  public static boolean isSingleMode(RelOptRuleCall call){
    return PrelUtil.getPlannerSettings(call.getPlanner()).isSingleMode();
  }

}
