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

import net.hydromatic.optiq.util.BitSets;

import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class StreamAggPrule extends AggPruleBase {
  public static final RelOptRule INSTANCE = new StreamAggPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private StreamAggPrule() {
    super(RelOptHelper.any(DrillAggregateRel.class), "Prel.StreamAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAggregateRel aggregate = (DrillAggregateRel) call.rel(0);
    final RelNode input = aggregate.getChild();
    RelCollation collation = getCollation(aggregate);

    RelTraitSet traits = null;

    try {
      if (aggregate.getGroupSet().isEmpty()) {
        DrillDistributionTrait singleDist = DrillDistributionTrait.SINGLETON;
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(singleDist);
        createTransformRequest(call, aggregate, input, traits);
      } else {

        // hash distribute on all grouping keys
        DrillDistributionTrait distOnAllKeys = 
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, 
                                       ImmutableList.copyOf(getDistributionField(aggregate, true)));
         
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(distOnAllKeys);
        createTransformRequest(call, aggregate, input, traits);

        // hash distribute on one grouping key
        DrillDistributionTrait distOnOneKey = 
            new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, 
                                       ImmutableList.copyOf(getDistributionField(aggregate, false)));
    
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(distOnOneKey);
        // Temporarily commenting out the single distkey plan since a few tpch queries (e.g 01.sql) get stuck
        // in VolcanoPlanner.canonize() method. Note that the corresponding single distkey plan for HashAggr works
        // ok.  One possibility is that here we have dist on single key but collation on all keys, so that 
        // might be causing some problem. 
        /// TODO: re-enable this plan after resolving the issue.  
        // createTransformRequest(call, aggregate, input, traits);
       
        
 /*       
        // create a 2-phase plan - commented out for now until we resolve planning for 'ANY' distribution 
        traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(DrillDistributionTrait.ANY);

        RelNode convertedInput = convert(input, traits);
        StreamAggPrel phase1Agg = new StreamAggPrel(aggregate.getCluster(), traits, convertedInput,
                                                    aggregate.getGroupSet(),
                                                    aggregate.getAggCallList());

        int numEndPoints = PrelUtil.getSettings(phase1Agg.getCluster()).numEndPoints();
        
        HashToMergeExchangePrel exch =
            new HashToMergeExchangePrel(phase1Agg.getCluster(), phase1Agg.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(distOnAllKeys),
                                        phase1Agg, ImmutableList.copyOf(getDistributionField(aggregate, true)),
                                        collation,
                                        numEndPoints);
        
        StreamAggPrel phase2Agg =  new StreamAggPrel(aggregate.getCluster(), traits, exch,
                                                     aggregate.getGroupSet(),
                                                     aggregate.getAggCallList());

        call.transformTo(phase2Agg);      
  */     
      } 
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }

  private void createTransformRequest(RelOptRuleCall call, DrillAggregateRel aggregate, 
                                      RelNode input, RelTraitSet traits) throws InvalidRelException {

    final RelNode convertedInput = convert(input, traits);
    
    StreamAggPrel newAgg = new StreamAggPrel(aggregate.getCluster(), traits, convertedInput, aggregate.getGroupSet(),
                                             aggregate.getAggCallList());
      
    call.transformTo(newAgg);
  }
  
  
  private RelCollation getCollation(DrillAggregateRel rel){
    
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group : BitSets.toIter(rel.getGroupSet())) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollationImpl.of(fields);
  }
}
