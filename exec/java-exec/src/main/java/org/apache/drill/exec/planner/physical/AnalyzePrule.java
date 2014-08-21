/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.physical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;

import java.util.List;

public class AnalyzePrule extends Prule {
  public static final RelOptRule INSTANCE = new AnalyzePrule();

  private static final List<String> FUNCTIONS = ImmutableList.of(
      "statcount",        // total number of entries in the table
      "nonnullstatcount", // total number of non-null entries in the table
      "ndv",              // total distinctive values in table
      "avg_width"         // Average column width
      //TODO: hll         // Hyperloglog
  );

  public AnalyzePrule() {
    super(RelOptHelper.some(DrillAnalyzeRel.class, DrillRel.DRILL_LOGICAL,
        RelOptHelper.any(RelNode.class)), "Prel.AnalyzePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAnalyzeRel analyze = call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(
        DrillDistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);
    final StatsAggPrel statsAggPrel = new StatsAggPrel(convertedInput, analyze.getCluster(),
        FUNCTIONS);

    final List<String> mapFields = Lists.newArrayList(FUNCTIONS);
    mapFields.add(DrillStatsTable.COL_COLUMN);
    final SingleRel newAnalyze = new UnpivotMapsPrel(statsAggPrel, analyze.getCluster(),
        mapFields);

    call.transformTo(newAnalyze);
  }
}
