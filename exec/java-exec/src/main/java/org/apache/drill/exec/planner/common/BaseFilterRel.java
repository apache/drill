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
package org.apache.drill.exec.planner.common;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.data.Filter;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.logical.DrillImplementor;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;


public abstract class BaseFilterRel extends FilterRelBase{
  protected BaseFilterRel(Convention convention, RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, traits, child, condition);
    assert getConvention() == convention;
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  protected LogicalExpression getFilterExpression(DrillParseContext context){
    return DrillOptiq.toDrill(context, getChild(), getCondition());
  }
}
