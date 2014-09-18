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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import net.hydromatic.linq4j.Ord;

import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.planner.common.DrillUnionRelBase;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

/**
 * Union implemented in Drill.
 */
public class DrillUnionRel extends DrillUnionRelBase implements DrillRel {
  /** Creates a DrillUnionRel. */
  public DrillUnionRel(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, boolean all) throws InvalidRelException {
    super(cluster, traits, inputs, all);
  }

  @Override
  public DrillUnionRel copy(RelTraitSet traitSet, List<RelNode> inputs,
      boolean all) {
    try {
      return new DrillUnionRel(getCluster(), traitSet, inputs, all);
    } catch (InvalidRelException e) {
      throw new AssertionError(e) ;
    }
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // divide cost by two to ensure cheaper than EnumerableDrillRel
    return super.computeSelfCost(planner).multiplyBy(.5);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Union.Builder builder = Union.builder();
    for (Ord<RelNode> input : Ord.zip(inputs)) {
      builder.addInput(implementor.visitChild(this, input.i, input.e));
    }
    builder.setDistinct(!all);
    return builder.build();
  }

  public static DrillUnionRel convert(Union union, ConversionContext context) throws InvalidRelException{
    throw new UnsupportedOperationException();
  }
}
