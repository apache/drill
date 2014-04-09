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
import org.apache.drill.exec.physical.config.TopN;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;

public class TopNPrel extends SingleRel implements Prel {

  protected int limit;
  protected final RelCollation collation;
  
  public TopNPrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, int limit, RelCollation collation) {
    super(cluster, traitSet, child);
    this.limit = limit;
    this.collation = collation;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TopNPrel(getCluster(), traitSet, sole(inputs), this.limit, this.collation);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getChild();
    
    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
     
    TopN topN = new TopN(childPOP, PrelUtil.getOrdering(this.collation, getChild().getRowType()), false, this.limit);
    
    creator.addPhysicalOperator(topN);
    
    return topN;
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    //We use multiplier 0.05 for TopN operator, and 0.1 for Sort, to make TopN a preferred choice. 
    return super.computeSelfCost(planner).multiplyBy(0.05);
  }

  
  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("limit", limit);
  }

}
