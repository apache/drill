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
  );

 /* private static final List<String> PHASE_1_FUNCTIONS = ImmutableList.of(
      "statcount",        // total number of entries in the table
      "nonnullstatcount", // total number of non-null entries in the table
      "hll",              // hyperloglog
      "avg_width"         // Average column width
  );

  private static final List<String> PHASE_2_FUNCTIONS = ImmutableList.of(
      "sum",              // total number of entries in the table
      "sum",              // total number of non-null entries in the table
      "ndv_hll",          // total distinctive values in table (computed using HLL)
      "avg"               // Average column width
      //TODO: hll         // Hyperloglog
  );*/

  private static final List<String> PHASE_1_FUNCTIONS = ImmutableList.of(
      "statcount",        // total number of entries in the table
      "nonnullstatcount", // total number of non-null entries in the table
      "sum_width",
      "hll"
    );
  private static final List<String> PHASE_2_FUNCTIONS = ImmutableList.of(
      "statcount",        // total number of entries in the table
      "nonnullstatcount", // total number of non-null entries in the table
      "avg_width",
      "hll_merge",
      "ndv"
    );

  public AnalyzePrule() {
    super(RelOptHelper.some(DrillAnalyzeRel.class, DrillRel.DRILL_LOGICAL,
        RelOptHelper.any(RelNode.class)), "Prel.AnalyzePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAnalyzeRel analyze = call.rel(0);
    final RelNode input = call.rel(1);
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    final SingleRel newAnalyze;
    final RelTraitSet singleDistTrait = call.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);

    if (settings.isParallelAnalyze()) {
      // Generate parallel ANALYZE plan: Writer<-Unpivot<-StatsAgg(Phase2)<-Exchange<-StatsAgg(Phase1)<-Scan
      final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.DEFAULT);
      final RelNode convertedInput = convert(input, traits);
      final List<String> mapFields1 = Lists.newArrayList(PHASE_1_FUNCTIONS);
      final List<String> mapFields2 = Lists.newArrayList(PHASE_2_FUNCTIONS);
      mapFields1.add(DrillStatsTable.COL_COLUMN);
      mapFields2.add(DrillStatsTable.COL_COLUMN);

      final StatsAggPrel statsAggPrel = new StatsAggPrel(analyze.getCluster(), traits, convertedInput,
          PHASE_1_FUNCTIONS, StatsAggPrel.OperatorPhase.PHASE_1of2);

      UnionExchangePrel exch = new UnionExchangePrel(statsAggPrel.getCluster(), singleDistTrait, statsAggPrel);

      final StatsMergePrel statsMergePrel = new StatsMergePrel(exch.getCluster(), singleDistTrait, exch,
          mapFields1, StatsMergePrel.OperatorPhase.PHASE_2of2);

      newAnalyze = new UnpivotMapsPrel(statsMergePrel.getCluster(), singleDistTrait, statsMergePrel,
          mapFields2);
    } else {
      // Generate serial ANALYZE plan: Writer<-Unpivot<-StatsAgg<-Exchange<-Scan
      final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
      final RelNode convertedInput = convert(input, traits);
      final StatsAggPrel statsAggPrel = new StatsAggPrel(analyze.getCluster(), traits, convertedInput,
          FUNCTIONS, StatsAggPrel.OperatorPhase.PHASE_1of1);
      final List<String> mapFields = Lists.newArrayList(FUNCTIONS);
      mapFields.add(DrillStatsTable.COL_COLUMN);
      newAnalyze = new UnpivotMapsPrel(analyze.getCluster(), traits, statsAggPrel,
          mapFields);
    }

    call.transformTo(newAnalyze);
  }
}
