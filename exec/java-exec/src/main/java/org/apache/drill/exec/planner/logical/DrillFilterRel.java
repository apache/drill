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

import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.common.DrillFilterRelBase;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;


public class DrillFilterRel extends DrillFilterRelBase implements DrillRel {
  protected DrillFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(DRILL_LOGICAL, cluster, traits, child, condition);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillFilterRel(getCluster(), traitSet, sole(inputs), getCondition());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    final LogicalOperator input = implementor.visitChild(this, 0, getChild());
    Filter f = new Filter(getFilterExpression(implementor.getContext()));
    f.setInput(input);
    return f;
  }

  public static DrillFilterRel convert(Filter filter, ConversionContext context) throws InvalidRelException{
    RelNode input = context.toRel(filter.getInput());
    return new DrillFilterRel(context.getCluster(), context.getLogicalTraits(), input, context.toRex(filter.getExpr()));
  }

}
