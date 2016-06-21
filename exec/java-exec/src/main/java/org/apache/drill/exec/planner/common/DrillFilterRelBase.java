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
package org.apache.drill.exec.planner.common;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PrelUtil;

import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.List;

/**
 * Base class for logical and physical Filters implemented in Drill
 */
public abstract class DrillFilterRelBase extends Filter implements DrillRelNode {
  private final int numConjuncts;
  private final List<RexNode> conjunctions;
  private final double filterMinSelectivityEstimateFactor;
  private final double filterMaxSelectivityEstimateFactor;

  protected DrillFilterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == convention;

    // save the number of conjuncts that make up the filter condition such
    // that repeated calls to the costing function can use the saved copy
    conjunctions = RelOptUtil.conjunctions(condition);
    numConjuncts = conjunctions.size();
    // assert numConjuncts >= 1;

    filterMinSelectivityEstimateFactor = PrelUtil.
            getPlannerSettings(cluster.getPlanner()).getFilterMinSelectivityEstimateFactor();
    filterMaxSelectivityEstimateFactor = PrelUtil.
            getPlannerSettings(cluster.getPlanner()).getFilterMaxSelectivityEstimateFactor();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getInput();
    double inputRows = RelMetadataQuery.getRowCount(child);
    double cpuCost = estimateCpuCost();
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, 0, 0);
  }

  protected LogicalExpression getFilterExpression(DrillParseContext context){
    return DrillOptiq.toDrill(context, getInput(), getCondition());
  }

  /* Given the condition (C1 and C2 and C3 and ... C_n), here is how to estimate cpu cost of FILTER :
  *  Let's say child's rowcount is n. We assume short circuit evaluation will be applied to the boolean expression evaluation.
  *  #_of_comparison = n + n * Selectivity(C1) + n * Selectivity(C1 and C2) + ... + n * Selecitivity(C1 and C2 ... and C_n)
  *  cpu_cost = #_of_comparison * DrillCostBase_COMPARE_CPU_COST;
  */
  private double estimateCpuCost() {
    RelNode child = this.getInput();
    double compNum = RelMetadataQuery.getRowCount(child);

    for (int i = 0; i< numConjuncts; i++) {
      RexNode conjFilter = RexUtil.composeConjunction(this.getCluster().getRexBuilder(), conjunctions.subList(0, i + 1), false);
      compNum += Filter.estimateFilteredRows(child, conjFilter);
    }

    return compNum * DrillCostBase.COMPARE_CPU_COST;
  }

  @Override
  public double getRows() {
    // override Calcite's default selectivity estimate - cap lower/upper bounds on the
    // selectivity estimate in order to get desired parallelism
    double selectivity = RelMetadataQuery.getSelectivity(getInput(), condition);
    if (!condition.isAlwaysFalse()) {
      // Cap selectivity at filterMinSelectivityEstimateFactor unless it is always FALSE
      if (selectivity < filterMinSelectivityEstimateFactor) {
        selectivity = filterMinSelectivityEstimateFactor;
      }
    }
    if (!condition.isAlwaysTrue()) {
      // Cap selectivity at filterMaxSelectivityEstimateFactor unless it is always TRUE
      if (selectivity > filterMaxSelectivityEstimateFactor) {
        selectivity = filterMaxSelectivityEstimateFactor;
      }
    }
    return selectivity*RelMetadataQuery.getRowCount(getInput());
  }
}
