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

import java.io.IOException;
import java.util.List;

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.PrelUtil.ProjectPushInfo;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;

import com.google.common.collect.Lists;

public class DrillPushProjIntoScan extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillPushProjIntoScan();

  private DrillPushProjIntoScan() {
    super(RelOptHelper.some(ProjectRel.class, RelOptHelper.any(EnumerableTableAccessRel.class)), "DrillPushProjIntoScan");
  }


  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel proj = (ProjectRel) call.rel(0);
    final EnumerableTableAccessRel scan = (EnumerableTableAccessRel) call.rel(1);

    try {
      ProjectPushInfo columnInfo = PrelUtil.getColumns(scan.getRowType(), proj.getProjects());

      if (columnInfo == null || columnInfo.isStarQuery() //
          || !scan.getTable().unwrap(DrillTable.class) //
          .getGroupScan().canPushdownProjects(columnInfo.columns)) {
        return;
      }

      final DrillScanRel newScan =
          new DrillScanRel(scan.getCluster(),
              scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
              scan.getTable(),
              columnInfo.createNewRowType(proj.getChild().getCluster().getTypeFactory()),
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

      if (RemoveTrivialProjectRule.isTrivial(newProj)) {
        call.transformTo(newScan);
      } else {
        call.transformTo(newProj);
      }
    } catch (IOException e) {
      throw new DrillRuntimeException(e);
    }
  }

}
