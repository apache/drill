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
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.planner.common.DrillSetOpRelBase;

import java.util.List;

/**
 * SetOp implemented in Drill.
 */
public class DrillSetOpRel extends DrillSetOpRelBase implements DrillRel {
  private final boolean isAggAdded;

  public DrillSetOpRel(RelOptCluster cluster, RelTraitSet traits,
                       List<RelNode> inputs, SqlKind kind, boolean all, boolean checkCompatibility, boolean isAggAdded) throws InvalidRelException {
    super(cluster, traits, inputs, kind, all, checkCompatibility);
    this.isAggAdded = isAggAdded;
  }

  public DrillSetOpRel(RelOptCluster cluster, RelTraitSet traits,
                       List<RelNode> inputs, SqlKind kind, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, kind, all, checkCompatibility);
    this.isAggAdded = false;
  }

  public boolean isAggAdded() {
    return isAggAdded;
  }

  @Override
  public DrillSetOpRel copy(RelTraitSet traitSet, List<RelNode> inputs,
                            boolean all) {
    try {
      return new DrillSetOpRel(getCluster(), traitSet, inputs, kind, all,
          false /* don't check compatibility during copy */, isAggAdded);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public DrillSetOpRel copy(List<RelNode> inputs, boolean isAggAdded) {
    try {
      return new DrillSetOpRel(getCluster(), traitSet, inputs, kind, all,
        false, isAggAdded);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
  }

  public DrillSetOpRel copy(boolean isAggAdded) {
    try {
      return new DrillSetOpRel(getCluster(), traitSet, inputs, kind, all,
        false, isAggAdded);
    } catch (InvalidRelException e) {
      throw new AssertionError(e);
    }
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

}
