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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillSetOpRel;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.slf4j.Logger;

import java.util.List;

/**
 * Intersect implemented in Drill.
 */
public class DrillIntersectRel extends Intersect implements DrillRel, DrillSetOpRel {
  private static final Logger tracer = CalciteTrace.getPlannerTracer();

  public DrillIntersectRel(RelOptCluster cluster, RelTraitSet traits,
                           List<RelNode> inputs, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, all);
    if (checkCompatibility && !this.isCompatible(getRowType(), getInputs())) {
      throw new InvalidRelException("Input row types of the Intersect are not compatible.");
    }
  }

  public static DrillIntersectRel create(List<RelNode> inputs, boolean all) {
    try {
      return new DrillIntersectRel(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs, all, true);
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
      return null;
    }
  }

  @Override
  public DrillIntersectRel copy(RelTraitSet traitSet, List<RelNode> inputs,
                                boolean all) {
    try {
      return new DrillIntersectRel(getCluster(), traitSet, inputs, all,
          false /* don't check compatibility during copy */);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public static DrillIntersectRel convert(org.apache.drill.common.logical.data.Intersect intersect, ConversionContext context) throws InvalidRelException{
    throw new UnsupportedOperationException();
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    org.apache.drill.common.logical.data.Intersect.Builder builder = org.apache.drill.common.logical.data.Intersect.builder();
    for (Ord<RelNode> input : Ord.zip(inputs)) {
      builder.addInput(implementor.visitChild(this, input.i, input.e));
    }
    builder.setDistinct(!all);
    return builder.build();
  }

}
