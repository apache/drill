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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.logical.data.InsertWriter;
import org.apache.drill.common.logical.data.LogicalOperator;

import java.util.List;

public class DrillTableModify extends TableModify implements DrillRel {

  protected DrillTableModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
    Prepare.CatalogReader catalogReader, RelNode input, Operation operation,
    List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList,
      sourceExpressionList, flattened);
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());

    InsertWriter logicalOperators = new InsertWriter(getTable());
    logicalOperators.setInput(childOp);
    return logicalOperators;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillTableModify(getCluster(), traitSet, getTable(), getCatalogReader(),
      inputs.get(0), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    double inputRowCount = mq.getRowCount(getInput());
    double dIo = inputRowCount + 1; // ensure non-zero cost
    return planner.getCostFactory().makeCost(rowCount, 0, dIo).multiplyBy(10);
  }
}
