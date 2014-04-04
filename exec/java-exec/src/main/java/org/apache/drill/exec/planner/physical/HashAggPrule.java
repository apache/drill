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

import java.util.logging.Logger;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;

public class HashAggPrule extends AggPruleBase {
  public static final RelOptRule INSTANCE = new HashAggPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private HashAggPrule() {
    super(RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillRel.class)), "Prel.HashAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAggregateRel aggregate = (DrillAggregateRel) call.rel(0);
    final RelNode input = call.rel(1);

    if (aggregate.containsDistinctCall() || aggregate.getGroupCount() == 0) {
      // currently, don't use HashAggregate if any of the logical aggrs contains DISTINCT or 
      // if there are no grouping keys 
      return;
    }
    
    DrillDistributionTrait toDist = null;
    RelTraitSet traits = null;

    try {
      if (aggregate.getGroupSet().isEmpty()) {
        toDist = DrillDistributionTrait.SINGLETON;
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist);
        createTransformRequest(call, aggregate, input, traits);
      } else {
        // hash distribute on all grouping keys
        toDist = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, 
                                            ImmutableList.copyOf(getDistributionField(aggregate, true /* get all grouping keys */)));
    
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist);
        createTransformRequest(call, aggregate, input, traits);

        // hash distribute on single grouping key
        toDist = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, 
                                            ImmutableList.copyOf(getDistributionField(aggregate, false /* get single grouping key */)));
    
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(toDist);
        createTransformRequest(call, aggregate, input, traits);
        
        ///TODO: 2 phase hash aggregate plan 
      } 
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

  private void createTransformRequest(RelOptRuleCall call, DrillAggregateRel aggregate, 
                                      RelNode input, RelTraitSet traits) throws InvalidRelException {

    final RelNode convertedInput = convert(input, traits);
    
    HashAggPrel newAgg = new HashAggPrel(aggregate.getCluster(), traits, convertedInput, aggregate.getGroupSet(),
                                         aggregate.getAggCallList());
      
    call.transformTo(newAgg);
  }
}
