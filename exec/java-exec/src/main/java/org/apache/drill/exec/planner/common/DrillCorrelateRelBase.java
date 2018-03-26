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
package org.apache.drill.exec.planner.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.cost.DrillCostBase;
import org.apache.drill.exec.planner.physical.PrelUtil;


public abstract class DrillCorrelateRelBase extends Correlate implements DrillRelNode {
  public DrillCorrelateRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
                               CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType semiJoinType) {
    super(cluster, traits, left, right, correlationId, requiredColumns, semiJoinType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
                                              RelMetadataQuery mq) {
    DrillCostBase.DrillCostFactory costFactory = (DrillCostBase.DrillCostFactory) planner.getCostFactory();

    double rowCount = mq.getRowCount(this.getLeft());
    long fieldWidth = PrelUtil.getPlannerSettings(planner).getOptions()
        .getOption(ExecConstants.AVERAGE_FIELD_WIDTH_KEY).num_val;

    double rowSize = (this.getLeft().getRowType().getFieldList().size()) * fieldWidth;

    double cpuCost = rowCount * rowSize * DrillCostBase.BASE_CPU_COST;
    double memCost = 0;
    return costFactory.makeCost(rowCount, cpuCost, 0, 0, memCost);
  }
}
