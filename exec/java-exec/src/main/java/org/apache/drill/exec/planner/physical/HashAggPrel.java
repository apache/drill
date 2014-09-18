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
import java.util.BitSet;
import java.util.List;

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashAggregate;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class HashAggPrel extends AggPrelBase implements Prel{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashAggPrel.class);

  public HashAggPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, BitSet groupSet,
      List<AggregateCall> aggCalls, OperatorPhase phase) throws InvalidRelException {
    super(cluster, traits, child, groupSet, aggCalls, phase);
  }

  @Override
  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet, List<AggregateCall> aggCalls) {
    try {
      return new HashAggPrel(getCluster(), traitSet, input, getGroupSet(), aggCalls,
          this.getOperatorPhase());
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }
    RelNode child = this.getChild();
    double inputRows = RelMetadataQuery.getRowCount(child);

    int numGroupByFields = this.getGroupCount();
    int numAggrFields = this.aggCalls.size();
    // cpu cost of hashing each grouping key
    double cpuCost = DrillCostBase.HASH_CPU_COST * numGroupByFields * inputRows;
    // add cpu cost for computing the aggregate functions
    cpuCost += DrillCostBase.FUNC_CPU_COST * numAggrFields * inputRows;
    double diskIOCost = 0; // assume in-memory for now until we enforce operator-level memory constraints

    // TODO: use distinct row count
    // + hash table template stuff
    double factor = PrelUtil.getPlannerSettings(planner).getOptions()
      .getOption(ExecConstants.HASH_AGG_TABLE_FACTOR_KEY).float_val;
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
      .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).num_val;

    // table + hashValues + links
    double memCost =
      (
        (fieldWidth * numGroupByFields) +
          IntHolder.WIDTH +
          IntHolder.WIDTH
      ) * inputRows * factor;

    DrillCostFactory costFactory = (DrillCostFactory) planner.getCostFactory();
    return costFactory.makeCost(inputRows, cpuCost, diskIOCost, 0 /* network cost */, memCost);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {

    Prel child = (Prel) this.getChild();
    HashAggregate g = new HashAggregate(child.getPhysicalOperator(creator),
        keys.toArray(new NamedExpression[keys.size()]),
        aggExprs.toArray(new NamedExpression[aggExprs.size()]),
        1.0f);

    return creator.addMetadata(this, g);

  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
