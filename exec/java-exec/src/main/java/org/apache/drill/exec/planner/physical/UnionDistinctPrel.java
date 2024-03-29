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

import java.io.IOException;
import java.util.List;

import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.cost.DrillCostBase.DrillCostFactory;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Union;

import com.google.common.collect.Lists;

public class UnionDistinctPrel extends UnionPrel {

  public UnionDistinctPrel(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs)
    throws InvalidRelException {
    super(cluster, traits, inputs, false /* all = false */);
  }


  @Override
  public Union copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    try {
      return new UnionDistinctPrel(this.getCluster(), traitSet, inputs);
    }catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }
    double totalInputRowCount = 0;
    for (int i = 0; i < this.getInputs().size(); i++) {
      totalInputRowCount += mq.getRowCount(this.getInputs().get(i));
    }

    double cpuCost = totalInputRowCount * DrillCostBase.BASE_CPU_COST;
    DrillCostFactory costFactory = (DrillCostFactory)planner.getCostFactory();
    return costFactory.makeCost(totalInputRowCount, cpuCost, 0, 0);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    List<PhysicalOperator> inputPops = Lists.newArrayList();

    for (int i = 0; i < this.getInputs().size(); i++) {
      inputPops.add( ((Prel)this.getInputs().get(i)).getPhysicalOperator(creator));
    }

    ///TODO: change this to UnionDistinct once implemented end-to-end..
    UnionAll unionAll = new UnionAll(inputPops);
    return creator.addMetadata(this, unionAll);
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
