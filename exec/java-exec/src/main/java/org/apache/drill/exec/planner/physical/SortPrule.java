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

import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 * Rule that converts a logical {@link DrillSortRel} to a physical sort.  Convert from Logical Sort into Physical Sort.
 * For Logical Sort, it requires one single data stream as the output.
 *
 */
public class SortPrule extends Prule{
  public static final RelOptRule INSTANCE = new SortPrule();

  private SortPrule() {
    super(RelOptHelper.any(DrillSortRel.class, DrillRel.DRILL_LOGICAL), "Prel.SortPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillSortRel sort = (DrillSortRel) call.rel(0);
    final RelNode input = sort.getChild();

    // Keep the collation in logical sort. Convert input into a RelNode with 1) this collation, 2) Physical, 3) hash distributed on

    DrillDistributionTrait hashDistribution =
        new DrillDistributionTrait(DrillDistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(sort)));

    final RelTraitSet traits = sort.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(hashDistribution);

    final RelNode convertedInput = convert(input, traits);

    if(isSingleMode(call)){
      call.transformTo(convertedInput);
    }else{
      RelNode exch = new SingleMergeExchangePrel(sort.getCluster(), sort.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON), convertedInput, sort.getCollation());
      call.transformTo(exch);  // transform logical "sort" into "SingleMergeExchange".

    }

  }

  private List<DistributionField> getDistributionField(DrillSortRel rel) {
    List<DistributionField> distFields = Lists.newArrayList();

    for (RelFieldCollation relField : rel.getCollation().getFieldCollations()) {
      DistributionField field = new DistributionField(relField.getFieldIndex());
      distFields.add(field);
    }

    return distFields;
  }

}