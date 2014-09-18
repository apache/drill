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

import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

/**
 * Base class for logical and physical Limits implemented in Drill
 */
public abstract class DrillLimitRelBase extends SingleRel implements DrillRelNode {
  protected RexNode offset;
  protected RexNode fetch;

  public DrillLimitRelBase(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child);
    this.offset = offset;
    this.fetch = fetch;
  }

  public RexNode getOffset() {
    return this.offset;
  }

  public RexNode getFetch() {
    return this.fetch;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    int off = offset != null ? RexLiteral.intValue(offset) : 0 ;
    int f = fetch != null ? RexLiteral.intValue(fetch) : 0 ;
    double numRows = off + f;
    double cpuCost = DrillCostBase.COMPARE_CPU_COST * numRows;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(numRows, cpuCost, 0, 0);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("fetch", fetch, fetch != null);
    return pw;
  }

}
