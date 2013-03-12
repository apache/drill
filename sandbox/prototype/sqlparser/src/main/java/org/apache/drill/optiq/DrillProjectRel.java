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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Pair;

import java.util.*;

/**
 * Project implemented in Drill.
 */
public class DrillProjectRel extends ProjectRelBase implements DrillRel {
  protected DrillProjectRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, RexNode[] exps, RelDataType rowType) {
    super(cluster, traits, child, exps, rowType, Flags.Boxed,
        Collections.<RelCollation>emptyList());
    assert getConvention() == CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillProjectRel(getCluster(), traitSet, sole(inputs),
        exps.clone(), rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  private List<Pair<RexNode, String>> projects() {
    return Pair.zip(
        Arrays.asList(exps),
        RelOptUtil.getFieldNameList(getRowType()));
  }

  @Override
  public void implement(DrillImplementor implementor) {
    implementor.visitChild(this, 0, getChild());
    final ObjectNode node = implementor.mapper.createObjectNode();
/*
    E.g. {
      op: "transform",
	    transforms: [
	      { ref: "quantity", expr: "donuts.sales"}
	    ]
*/
    node.put("op", "transform");
    final ArrayNode transforms = implementor.mapper.createArrayNode();
    node.put("transforms", transforms);
    for (Pair<RexNode, String> pair : projects()) {
      final ObjectNode objectNode = implementor.mapper.createObjectNode();
      transforms.add(objectNode);
      objectNode.put("expr", DrillOptiq.toDrill(pair.left, "donuts"));
      objectNode.put("ref", pair.right);
    }
    implementor.add(node);
  }
}

// End DrillProjectRel.java
