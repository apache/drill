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

import org.apache.drill.common.logical.data.LogicalOperator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.ValuesRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexLiteral;

/**
 * Values implemented in Drill.
 */
public class DrillValuesRel extends ValuesRelBase implements DrillRel {
  protected DrillValuesRel(RelOptCluster cluster, RelDataType rowType, List<List<RexLiteral>> tuples, RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    assert getConvention() == DRILL_LOGICAL;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new DrillValuesRel(getCluster(), rowType, tuples, traitSet);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    // Update when https://issues.apache.org/jira/browse/DRILL-57 fixed
    throw new UnsupportedOperationException();
  }
}
