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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class DrillPushLimitToScanRule extends RelOptRule {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillPushLimitToScanRule.class);

  private DrillPushLimitToScanRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static DrillPushLimitToScanRule LIMIT_ON_SCAN = new DrillPushLimitToScanRule(
      RelOptHelper.some(DrillLimitRel.class, RelOptHelper.any(DrillScanRel.class)), "DrillPushLimitToScanRule_LimitOnScan") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillScanRel scanRel = call.rel(1);
      return scanRel.getGroupScan().supportsLimitPushdown(); // For now only applies to Parquet.
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DrillLimitRel limitRel = call.rel(0);
        DrillScanRel scanRel = call.rel(1);
        doOnMatch(call, limitRel, scanRel, null);
    }
  };

  public static DrillPushLimitToScanRule LIMIT_ON_PROJECT = new DrillPushLimitToScanRule(
      RelOptHelper.some(DrillLimitRel.class, RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))), "DrillPushLimitToScanRule_LimitOnProject") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      DrillScanRel scanRel = call.rel(2);
      return scanRel.getGroupScan().supportsLimitPushdown(); // For now only applies to Parquet.
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      DrillLimitRel limitRel = call.rel(0);
      DrillProjectRel projectRel = call.rel(1);
      DrillScanRel scanRel = call.rel(2);
      doOnMatch(call, limitRel, scanRel, projectRel);
    }
  };


  protected void doOnMatch(RelOptRuleCall call, DrillLimitRel limitRel, DrillScanRel scanRel, DrillProjectRel projectRel){
    try {
      final int rowCountRequested = (int) limitRel.getRows();

      final GroupScan newGroupScan = scanRel.getGroupScan().applyLimit(rowCountRequested);

      if (newGroupScan == null ) {
        return;
      }

      DrillScanRel newScanRel = new DrillScanRel(scanRel.getCluster(),
          scanRel.getTraitSet(),
          scanRel.getTable(),
          newGroupScan,
          scanRel.getRowType(),
          scanRel.getColumns(),
          scanRel.partitionFilterPushdown());

      final RelNode newLimit;
      if (projectRel != null) {
        final RelNode newProject = projectRel.copy(projectRel.getTraitSet(), ImmutableList.of((RelNode)newScanRel));
        newLimit = limitRel.copy(limitRel.getTraitSet(), ImmutableList.of((RelNode)newProject));
      } else {
        newLimit = limitRel.copy(limitRel.getTraitSet(), ImmutableList.of((RelNode)newScanRel));
      }

      call.transformTo(newLimit);
      logger.debug("Converted to a new DrillScanRel" + newScanRel.getGroupScan());
    }  catch (Exception e) {
      logger.warn("Exception while using the pruned partitions.", e);
    }

  }
}
