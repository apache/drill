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

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.common.DrillRelOptUtil.ProjectPushInfo;
import org.apache.drill.exec.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * When table support project push down, rule can be applied to reduce number of read columns
 * thus improving scan operator performance.
 */
public class DrillPushProjectIntoScanRule extends RelOptRule {
  public static final RelOptRule INSTANCE =
      new DrillPushProjectIntoScanRule(LogicalProject.class,
          EnumerableTableScan.class,
          "DrillPushProjIntoEnumerableScan");

  public static final RelOptRule DRILL_LOGICAL_INSTANCE =
      new DrillPushProjectIntoScanRule(LogicalProject.class,
          DrillScanRel.class,
          "DrillPushProjIntoDrillRelScan");

  private DrillPushProjectIntoScanRule(Class<? extends Project> projectClass, Class<? extends TableScan> scanClass, String description) {
    super(RelOptHelper.some(projectClass, RelOptHelper.any(scanClass)), description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final TableScan scan = call.rel(1);

    try {

      if (scan.getRowType().getFieldList().isEmpty()) {
        return;
      }

      ProjectPushInfo projectPushInfo = DrillRelOptUtil.getFieldsInformation(scan.getRowType(), project.getProjects());
      if (!canPushProjectIntoScan(scan.getTable(), projectPushInfo)) {
        return;
      }

      final DrillScanRel newScan =
          new DrillScanRel(scan.getCluster(),
              scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scan.getTable(),
              projectPushInfo.createNewRowType(project.getInput().getCluster().getTypeFactory()),
              projectPushInfo.getFields());

      List<RexNode> newProjects = new ArrayList<>();
      for (RexNode n : project.getChildExps()) {
        newProjects.add(n.accept(projectPushInfo.getInputReWriter()));
      }

      final DrillProjectRel newProject =
          new DrillProjectRel(project.getCluster(),
              project.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              newScan,
              newProjects,
              project.getRowType());

      if (ProjectRemoveRule.isTrivial(newProject)) {
        call.transformTo(newScan);
      } else {
        call.transformTo(newProject);
      }
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

  /**
   * Push project into scan be done only if this is not a star query and
   * table supports project push down.
   *
   * @param table table instance
   * @param projectPushInfo fields information
   * @return true if push project into scan can be performed, false otherwise
   */
  private boolean canPushProjectIntoScan(RelOptTable table, ProjectPushInfo projectPushInfo) throws IOException {
    DrillTable drillTable = Utilities.getDrillTable(table);
    return !Utilities.isStarQuery(projectPushInfo.getFields())
        && drillTable.getGroupScan().canPushdownProjects(projectPushInfo.getFields());
  }
}
