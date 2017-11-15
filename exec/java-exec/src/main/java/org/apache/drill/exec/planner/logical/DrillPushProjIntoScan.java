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

import java.io.IOException;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.PrelUtil.ProjectPushInfo;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;

public class DrillPushProjIntoScan extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillPushProjIntoScan(LogicalProject.class, EnumerableTableScan.class);

  private DrillPushProjIntoScan(Class<? extends Project> projectClass,
      Class<? extends TableScan> scanClass) {
    super(RelOptHelper.some(projectClass, RelOptHelper.any(scanClass)),
        DrillRelFactories.LOGICAL_BUILDER, "DrillPushProjIntoScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project proj = call.rel(0);
    final TableScan scan = call.rel(1);

    try {
      ProjectPushInfo columnInfo = PrelUtil.getColumns(scan.getRowType(), proj.getProjects());

      // get DrillTable, either wrapped in RelOptTable, or DrillTranslatableTable.
      DrillTable table = scan.getTable().unwrap(DrillTable.class);
      if (table == null) {
        table = scan.getTable().unwrap(DrillTranslatableTable.class).getDrillTable();
      }

      if (columnInfo == null || columnInfo.isStarQuery() //
          || !table //
          .getGroupScan().canPushdownProjects(columnInfo.columns)) {
        return;
      }

      final DrillScanRel newScan =
          new DrillScanRel(scan.getCluster(),
              scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scan.getTable(),
              columnInfo.createNewRowType(proj.getInput().getCluster().getTypeFactory()),
              columnInfo.columns);


      List<RexNode> newProjects = Lists.newArrayList();
      for (RexNode n : proj.getChildExps()) {
        newProjects.add(n.accept(columnInfo.getInputRewriter()));
      }

      final DrillProjectRel newProj =
          new DrillProjectRel(proj.getCluster(),
              proj.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              newScan,
              newProjects,
              proj.getRowType());

      if (ProjectRemoveRule.isTrivial(newProj)) {
        call.transformTo(newScan);
      } else {
        call.transformTo(newProj);
      }
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

}
