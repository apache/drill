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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class DrillScanRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillScanRule(SelectionBasedTableScan.class);

  private DrillScanRule(Class<? extends TableScan> scan) {
    super(RelOptHelper.any(scan),
        DrillRelFactories.LOGICAL_BUILDER, "DrillScanRule:" + scan.getSimpleName());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan access = call.rel(0);
    RelTraitSet traits = access.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    call.transformTo(new DrillScanRel(access.getCluster(), traits, access.getTable()));
  }
}
