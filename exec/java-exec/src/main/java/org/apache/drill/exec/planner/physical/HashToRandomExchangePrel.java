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
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.SelectionVectorRemover;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait.DistributionField;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;


public class HashToRandomExchangePrel extends SingleRel implements Prel {

  private final List<DistributionField> fields;
  
  public HashToRandomExchangePrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<DistributionField> fields) {
    super(cluster, traitSet, input);
    this.fields = fields;
    assert input.getConvention() == Prel.DRILL_PHYSICAL;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(.1);    
    //return planner.getCostFactory().makeZeroCost();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new HashToRandomExchangePrel(getCluster(), traitSet, sole(inputs), fields);
  }
  
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    
    //Currently, only accepts "NONE". For other, requires SelectionVectorRemover
    if (!childPOP.getSVMode().equals(SelectionVectorMode.NONE)) {
      childPOP = new SelectionVectorRemover(childPOP);
      creator.addPhysicalOperator(childPOP);
    }

    HashToRandomExchange g = new HashToRandomExchange(childPOP, PrelUtil.getHashExpression(this.fields, getChild().getRowType()));
    creator.addPhysicalOperator(g);
    return g;    
  }
  
  public List<DistributionField> getFields() {
    return this.fields;
  }
  
}
