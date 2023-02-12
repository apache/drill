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
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.drill.common.logical.data.Except;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillSetOpRel;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.slf4j.Logger;

import java.util.List;

/**
 * Minus implemented in Drill.
 */
public class DrillExceptRel extends Minus implements DrillRel, DrillSetOpRel {
  private static final Logger tracer = CalciteTrace.getPlannerTracer();

  public DrillExceptRel(RelOptCluster cluster, RelTraitSet traits,
                       List<RelNode> inputs, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, all);
    if (checkCompatibility && !this.isCompatible(getRowType(), getInputs())) {
      throw new InvalidRelException("Input row types of the Except are not compatible.");
    }
  }

  public static DrillExceptRel create(List<RelNode> inputs, boolean all) {
    try {
      return new DrillExceptRel(inputs.get(0).getCluster(), inputs.get(0).getTraitSet(), inputs, all, true);
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
      return null;
    }
  }

  @Override
  public DrillExceptRel copy(RelTraitSet traitSet, List<RelNode> inputs,
                            boolean all) {
    try {
      return new DrillExceptRel(getCluster(), traitSet, inputs, all, false);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public DrillExceptRel copy(List<RelNode> inputs) {
    try {
      return new DrillExceptRel(getCluster(), traitSet, inputs, all, false);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public DrillExceptRel copy() {
    try {
      return new DrillExceptRel(getCluster(), traitSet, inputs, all, false);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public static DrillExceptRel convert(Except except, ConversionContext context) throws InvalidRelException{
    throw new UnsupportedOperationException();
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    Except.Builder builder = Except.builder();
    for (Ord<RelNode> input : Ord.zip(inputs)) {
      builder.addInput(implementor.visitChild(this, input.i, input.e));
    }
    builder.setDistinct(!all);
    return builder.build();
  }

}
