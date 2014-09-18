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

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class OrderedPartitionExchangePrel extends ExchangePrel {

  public OrderedPartitionExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);

    int  rowWidth = child.getRowType().getFieldCount() * DrillCostBase.AVG_FIELD_WIDTH;

    double rangePartitionCpuCost = DrillCostBase.RANGE_PARTITION_CPU_COST * inputRows;
    double svrCpuCost = DrillCostBase.SVR_CPU_COST * inputRows;
    double networkCost = DrillCostBase.BYTE_NETWORK_COST * inputRows * rowWidth;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(inputRows, rangePartitionCpuCost + svrCpuCost, 0, networkCost);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OrderedPartitionExchangePrel(getCluster(), traitSet, sole(inputs));
  }

  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new IOException(this.getClass().getSimpleName() + " not supported yet!");
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }
}
