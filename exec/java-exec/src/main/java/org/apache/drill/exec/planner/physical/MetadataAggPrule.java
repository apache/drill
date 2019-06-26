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
package org.apache.drill.exec.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.MetadataAggRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetadataAggPrule extends Prule {
  public static final MetadataAggPrule INSTANCE = new MetadataAggPrule();

  public MetadataAggPrule() {
    super(RelOptHelper.any(MetadataAggRel.class, DrillRel.DRILL_LOGICAL),
        "MetadataAggPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    MetadataAggRel relNode = call.rel(0);
    RelNode input = relNode.getInput();

    int groupByExprsSize = relNode.getContext().groupByExpressions().size();

    // group by expressions will be returned first
    RelCollation collation = RelCollations.of(IntStream.range(1, groupByExprsSize)
        .mapToObj(RelFieldCollation::new)
        .collect(Collectors.toList()));

    // TODO: update DrillDistributionTrait when implemented parallelization for metadata collecting (see DRILL-7433)
    RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    traits = groupByExprsSize > 0 ? traits.plus(collation) : traits;
    RelNode convertedInput = convert(input, traits);
    call.transformTo(
        new MetadataAggPrel(relNode.getCluster(), traits, convertedInput, relNode.getContext()));
  }
}
