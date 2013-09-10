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
package org.apache.drill.optiq;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.hydromatic.linq4j.Ord;
import org.eigenbase.rel.UnionRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Union implemented in Drill.
 */
public class DrillUnionRel extends UnionRelBase implements DrillRel {
  /** Creates a DrillUnionRel. */
  public DrillUnionRel(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public DrillUnionRel copy(RelTraitSet traitSet, List<RelNode> inputs,
      boolean all) {
    return new DrillUnionRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // divide cost by two to ensure cheaper than EnumerableDrillRel
    return super.computeSelfCost(planner).multiplyBy(.5);
  }

  @Override
  public int implement(DrillImplementor implementor) {
    List<Integer> inputIds = new ArrayList<>();
    for (Ord<RelNode> input : Ord.zip(inputs)) {
      inputIds.add(implementor.visitChild(this, input.i, input.e));
    }
/*
    E.g. {
      op: "union",
      distinct: true,
	  inputs: [2, 4]
	}
*/
    final ObjectNode union = implementor.mapper.createObjectNode();
    union.put("op", "union");
    union.put("distinct", !all);
    final ArrayNode inputs = implementor.mapper.createArrayNode();
    union.put("inputs", inputs);
    for (Integer inputId : inputIds) {
      inputs.add(inputId);
    }
    return implementor.add(union);
  }
}
