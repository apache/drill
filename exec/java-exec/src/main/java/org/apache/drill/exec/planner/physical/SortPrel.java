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

import java.io.IOException;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class SortPrel extends SortRel implements Prel {

  /** Creates a DrillSortRel. */
  public SortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    super(cluster, traits, input, collation);
  }

  /** Creates a DrillSortRel with offset and fetch. */
  public SortPrel(RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      //We use multiplier 0.05 for TopN operator, and 0.1 for Sort, to make TopN a preferred choice.
      return super.computeSelfCost(planner).multiplyBy(.1); 
    }
    
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);
    // int  rowWidth = child.getRowType().getPrecision();
    int numSortFields = this.collation.getFieldCollations().size();
    double cpuCost = DrillCostBase.COMPARE_CPU_COST * numSortFields * inputRows * (Math.log(inputRows)/Math.log(2)); 
    double diskIOCost = 0; // assume in-memory for now until we enforce operator-level memory constraints
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, diskIOCost, 0);    
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

//    childPOP = PrelUtil.removeSvIfRequired(childPOP, SelectionVectorMode.NONE);
//    Sort g = new Sort(childPOP, PrelUtil.getOrdering(this.collation, getChild().getRowType()), false);

    childPOP = PrelUtil.removeSvIfRequired(childPOP, SelectionVectorMode.NONE, SelectionVectorMode.TWO_BYTE);
    Sort g = new ExternalSort(childPOP, PrelUtil.getOrdering(this.collation, getChild().getRowType()), false);

    return g;
  }

  public SortPrel copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new SortPrel(getCluster(), traitSet, newInput, newCollation);
  }
}
