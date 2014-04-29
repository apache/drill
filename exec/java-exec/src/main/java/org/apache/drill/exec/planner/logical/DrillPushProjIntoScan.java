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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import net.hydromatic.optiq.rules.java.JavaRules.EnumerableTableAccessRel;

import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.rules.PushProjector;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;

import com.google.hive12.common.collect.Lists;

public class DrillPushProjIntoScan extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillPushProjIntoScan();

  private DrillPushProjIntoScan() {
    super(RelOptHelper.some(ProjectRel.class, RelOptHelper.any(EnumerableTableAccessRel.class)), "DrillPushProjIntoScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel proj = (ProjectRel) call.rel(0);
    final EnumerableTableAccessRel scan = (EnumerableTableAccessRel) call.rel(1);

    List<Integer> columnsIds = getRefColumnIds(proj);

    RelDataType newScanRowType = createStructType(scan.getCluster().getTypeFactory(), getProjectedFields(scan.getRowType(),columnsIds));

    final DrillScanRel newScan = new DrillScanRel(scan.getCluster(), scan.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
        scan.getTable(), newScanRowType);

    List<RexNode> convertedExprs = getConvertedProjExp(proj, scan, columnsIds);

    final DrillProjectRel newProj = new DrillProjectRel(proj.getCluster(), proj.getTraitSet().plus(DrillRel.DRILL_LOGICAL),
        newScan, convertedExprs, proj.getRowType());

    if (RemoveTrivialProjectRule.isTrivial(newProj)) {
      call.transformTo(newScan);
    } else {
      call.transformTo(newProj);
    }

  }

  private List<RexNode> getConvertedProjExp(ProjectRel proj, RelNode child, List<Integer> columnsIds) {
    PushProjector pushProjector =
        new PushProjector(
            proj, null, child, PushProjector.ExprCondition.FALSE);
    ProjectRel topProject = pushProjector.convertProject(null);

    if (topProject !=null)
      return topProject.getProjects();
    else
      return proj.getProjects();
  }

  private  RelDataType createStructType(
      RelDataTypeFactory typeFactory,
      final List<RelDataTypeField> fields
      ) {
    final RelDataTypeFactory.FieldInfoBuilder builder =
        typeFactory.builder();
    for (RelDataTypeField field : fields) {
      builder.add(field.getName(), field.getType());
    }
    return builder.build();
  }


  private List<Integer> getRefColumnIds(ProjectRelBase proj) {
    RefFieldsVisitor v = new RefFieldsVisitor();

    for (RexNode exp : proj.getProjects()) {
      v.apply(exp);
    }
    return new ArrayList<Integer>(v.getReferencedFieldIndex());
  }

  private List<RelDataTypeField> getProjectedFields(RelDataType rowType, List<Integer> columnIds) {
    List<RelDataTypeField> oldFields = rowType.getFieldList();
    List<RelDataTypeField> newFields = Lists.newArrayList();

    for (Integer id : columnIds) {
      newFields.add(oldFields.get(id));
    }

    return newFields;
  }

  /** Visitor that finds the set of inputs that are used. */
  public static class RefFieldsVisitor extends RexShuttle {
    public final SortedSet<Integer> inputPosReferenced =
        new TreeSet<Integer>();

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      inputPosReferenced.add(inputRef.getIndex());
      return inputRef;
    }

    public Set<Integer> getReferencedFieldIndex() {
      return this.inputPosReferenced;
    }
  }

}