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
import java.util.List;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.SingleMergeExchange;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class SingleMergeExchangePrel extends ExchangePrel {

  private final RelCollation collation ;

  public SingleMergeExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation) {
    super(cluster, traitSet, input);
    this.collation = collation;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  /**
   * A SingleMergeExchange processes a total of M rows coming from N
   * sorted input streams (from N senders) and merges them into a single
   * output sorted stream. For costing purposes we can assume each sender
   * is sending M/N rows to a single receiver.
   * (See DrillCostBase for symbol notations)
   * C =  CPU cost of SV remover for M/N rows
   *     + Network cost of sending M/N rows to 1 destination.
   * So, C = (s * M/N) + (w * M/N)
   * Cost of merging M rows coming from N senders = (M log2 N) * c
   * Total cost = N * C + (M log2 N) * c
   */
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);
    int  rowWidth = child.getRowType().getFieldCount() * DrillCostBase.AVG_FIELD_WIDTH;
    double svrCpuCost = DrillCostBase.SVR_CPU_COST * inputRows;
    double networkCost = DrillCostBase.BYTE_NETWORK_COST * inputRows * rowWidth;
    int numEndPoints = PrelUtil.getSettings(getCluster()).numEndPoints();
    double mergeCpuCost = DrillCostBase.COMPARE_CPU_COST * inputRows * (Math.log(numEndPoints)/Math.log(2));
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, svrCpuCost + mergeCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SingleMergeExchangePrel(getCluster(), traitSet, sole(inputs), collation);
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);

    if (PrelUtil.getSettings(getCluster()).isSingleMode()) {
      return childPOP;
    }

    SingleMergeExchange g = new SingleMergeExchange(childPOP, PrelUtil.getOrdering(this.collation, getChild().getRowType()));
    return creator.addMetadata(this, g);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (pw.nest()) {
      pw.item("collation", collation);
    } else {
      for (Ord<RelFieldCollation> ord : Ord.zip(collation.getFieldCollations())) {
        pw.item("sort" + ord.i, ord.e);
      }
    }
    return pw;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
