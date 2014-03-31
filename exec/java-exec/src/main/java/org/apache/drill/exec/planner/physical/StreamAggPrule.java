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
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.trace.EigenbaseTrace;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class StreamAggPrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new StreamAggPrule();
  protected static final Logger tracer = EigenbaseTrace.getPlannerTracer();

  private StreamAggPrule() {
    super(RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillRel.class)), "Prel.StreamAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAggregateRel aggregate = (DrillAggregateRel) call.rel(0);
    final RelNode input = call.rel(1);
    RelCollation collation = getCollation(aggregate);

    DrillDistributionTrait toDist = null;
    if (!aggregate.getGroupSet().isEmpty()) {
      toDist = new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(aggregate)));
    } else {
      toDist = DrillDistributionTrait.SINGLETON;
    }
    
    final RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(collation).plus(toDist);
    
    final RelNode convertedInput = convert(input, traits);
    
    try {          
      StreamAggPrel newAgg = new StreamAggPrel(aggregate.getCluster(), traits, convertedInput, aggregate.getGroupSet(),
          aggregate.getAggCallList());
      
      call.transformTo(newAgg);
    } catch (InvalidRelException e) {
      tracer.warning(e.toString());
    }
  }
  
  
  private RelCollation getCollation(DrillAggregateRel rel){
    
    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group : BitSets.toIter(rel.getGroupSet())) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollationImpl.of(fields);
  }

  private List<DistributionField> getDistributionField(DrillAggregateRel rel) {
    
    //For now, we use the GROUPBY keys as the distribution keys. 
    //Ideally, we should pick a set of distributions keys such that they distribute the rows most "evenly", with least "skewness".
    //This can be done if later on we have more statistics for the data stream. 
    
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int group : BitSets.toIter(rel.getGroupSet())) {
      DistributionField field = new DistributionField(group);
      groupByFields.add(field);
    }    
    
//    if (groupByFields.isEmpty()) {
//      groupByFields.add(new DistributionField(0));
//    }
    
    return groupByFields;
  }
}
