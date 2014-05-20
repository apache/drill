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
import org.apache.drill.common.logical.data.Writer;
import org.apache.drill.exec.planner.common.DrillWriterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;

public class DrillWriterRel extends DrillWriterRelBase implements DrillRel {

  public DrillWriterRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, CreateTableEntry createTableEntry) {
    super(DRILL_LOGICAL, cluster, traitSet, input, createTableEntry);
    setRowType();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillWriterRel(getCluster(), traitSet, sole(inputs), getCreateTableEntry());
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getChild());
    return Writer
        .builder()
        .setInput(childOp)
        .setCreateTableEntry(getCreateTableEntry())
        .build();
  }
}
