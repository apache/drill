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

import org.apache.drill.exec.physical.base.GroupScan;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;

public class PartitionPruningUtil {
  public static void rewritePlan(RelOptRuleCall call, DrillFilterRel filterRel, DrillProjectRel projectRel, DrillScanRel scanRel, GroupScan newScan, DirPathBuilder builder) {
    RexNode origFilterCondition = filterRel.getCondition();
    RexNode newFilterCondition = builder.getFinalCondition();

    if (newFilterCondition.isAlwaysTrue()) {

      final DrillScanRel newScanRel =
          new DrillScanRel(scanRel.getCluster(),
              scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scanRel.getTable(),
              newScan,
              scanRel.getRowType(),
              scanRel.getColumns());

      if (projectRel != null) {
        DrillProjectRel newProjectRel = new DrillProjectRel(projectRel.getCluster(), projectRel.getTraitSet(),
            newScanRel, projectRel.getProjects(), filterRel.getRowType());

        call.transformTo(newProjectRel);
      } else {
        call.transformTo(newScanRel);
      }
    } else {
      DrillRel inputRel;
      final DrillScanRel newScanRel =
          new DrillScanRel(scanRel.getCluster(),
              scanRel.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scanRel.getTable(),
              newScan,
              scanRel.getRowType(),
              scanRel.getColumns());
      if (projectRel != null) {
        DrillProjectRel newProjectRel = new DrillProjectRel(projectRel.getCluster(), projectRel.getTraitSet(),
            newScanRel, projectRel.getProjects(), projectRel.getRowType());
        inputRel = newProjectRel;
      } else {
        inputRel = newScanRel;
      }
      final DrillFilterRel newFilterRel = new DrillFilterRel(filterRel.getCluster(), filterRel.getTraitSet(),
          inputRel, origFilterCondition /* for now keep the original condition until we add more test coverage */);

      call.transformTo(newFilterRel);
    }
  }

  public static String truncatePrefixFromPath(String fileName) {
    String pathPrefixComponent[] = fileName.split(":", 2);
    if (pathPrefixComponent.length == 1) {
      return pathPrefixComponent[0];
    } else {
      return pathPrefixComponent[1];
    }
  }
}
