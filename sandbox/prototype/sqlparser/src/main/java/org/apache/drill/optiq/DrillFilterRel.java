/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.optiq;

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexNode;

import java.util.List;

/**
 * Filter implemented in Drill.
 */
public class DrillFilterRel extends FilterRelBase implements DrillRel {
  protected DrillFilterRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillFilterRel(getCluster(), traitSet, sole(inputs),
        getCondition());
  }

  @Override
  public String getHolder() {
    return ((DrillRel) getChild()).getHolder();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  @Override
  public void implement(DrillImplementor implementor) {
    implementor.visitChild(this, 0, getChild());
    final ObjectNode node = implementor.mapper.createObjectNode();
/*
      E.g. {
	      op: "filter",
	      expr: "donuts.ppu < 1.00"
	    }
*/
    node.put("op", "filter");
    node.put("expr", DrillOptiq.toDrill(getCondition(), getHolder()));
    implementor.add(node);
  }
}

// End DrillFilterRel.java
