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
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;

import java.util.List;

/**
 * Base class for logical and physical Filters implemented in Drill
 */
public abstract class DrillFilterRelBase extends FilterRelBase implements DrillRelNode {
  private final int numConjuncts;
  private final List<RexNode> conjunctions;

  protected DrillFilterRelBase(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == convention;

    // save the number of conjuncts that make up the filter condition such
    // that repeated calls to the costing function can use the saved copy
    conjunctions = RelOptUtil.conjunctions(condition);
    numConjuncts = conjunctions.size();
    assert numConjuncts >= 1;

  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);
    double cpuCost = estimateCpuCost();
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, 0, 0);
  }

  protected LogicalExpression getFilterExpression(DrillParseContext context){
    return DrillOptiq.toDrill(context, getChild(), getCondition());
  }

  /* Given the condition (C1 and C2 and C3 and ... C_n), here is how to estimate cpu cost of FILTER :
  *  Let's say child's rowcount is n. We assume short circuit evaluation will be applied to the boolean expression evaluation.
  *  #_of_comparison = n + n * Selectivity(C1) + n * Selectivity(C1 and C2) + ... + n * Selecitivity(C1 and C2 ... and C_n)
  *  cpu_cost = #_of_comparison * DrillCostBase_COMPARE_CPU_COST;
  */
  private double estimateCpuCost() {
    RelNode child = this.getChild();
    double compNum = RelMetadataQuery.getRowCount(child);

    for (int i = 0; i< numConjuncts; i++) {
      RexNode conjFilter = RexUtil.composeConjunction(this.getCluster().getRexBuilder(), conjunctions.subList(0, i + 1), false);
      compNum += FilterRelBase.estimateFilteredRows(child, conjFilter);
    }

    return compNum * DrillCostBase.COMPARE_CPU_COST;
  }

}
