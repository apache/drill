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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.util.Utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.planner.logical.FieldsReWriterUtil.DesiredField;
import static org.apache.drill.exec.planner.logical.FieldsReWriterUtil.FieldsReWriter;

/**
 * When table support project push down, rule can be applied to reduce number of read columns
 * thus improving scan operator performance.
 */
public class DrillPushProjectIntoScanRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillPushProjectIntoScanRule(LogicalProject.class, EnumerableTableScan.class);

  private DrillPushProjectIntoScanRule(Class<? extends Project> projectClass,
                                       Class<? extends TableScan> scanClass) {
    super(RelOptHelper.some(projectClass, RelOptHelper.any(scanClass)),
        DrillRelFactories.LOGICAL_BUILDER, "DrillPushProjectIntoScanRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final TableScan scan = call.rel(1);

    try {

      if (scan.getRowType().getFieldList().isEmpty()) {
        return;
      }

      ProjectPushInfo projectPushInfo = getFieldsInformation(scan.getRowType(), project.getProjects());
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

  private ProjectPushInfo getFieldsInformation(RelDataType rowType, List<RexNode> projects) {
    ProjectFieldsVisitor fieldsVisitor = new ProjectFieldsVisitor(rowType);
    for (RexNode exp : projects) {
      PathSegment segment = exp.accept(fieldsVisitor);
      fieldsVisitor.addField(segment);
    }

    return fieldsVisitor.getInfo();
  }

  /**
   * Stores information about fields, their names and types.
   * Is responsible for creating mapper which used in field re-writer visitor.
   */
  private static class ProjectPushInfo {
    private final List<SchemaPath> fields;
    private final FieldsReWriter reWriter;
    private final List<String> fieldNames;
    private final List<RelDataType> types;

    ProjectPushInfo(List<SchemaPath> fields, Map<String, DesiredField> desiredFields) {
      this.fields = fields;
      this.fieldNames = new ArrayList<>();
      this.types = new ArrayList<>();

      Map<RexNode, Integer> mapper = new HashMap<>();

      int index = 0;
      for (Map.Entry<String, DesiredField> entry : desiredFields.entrySet()) {
        fieldNames.add(entry.getKey());
        DesiredField desiredField = entry.getValue();
        types.add(desiredField.getType());
        for (RexNode node : desiredField.getNodes()) {
          mapper.put(node, index);
        }
        index++;
      }
      this.reWriter = new FieldsReWriter(mapper);
    }

    List<SchemaPath> getFields() {
      return fields;
    }

    FieldsReWriter getInputReWriter() {
      return reWriter;
    }

    /**
     * Creates new row type based on stores types and field names.
     *
     * @param factory factory for data type descriptors.
     * @return new row type
     */
    RelDataType createNewRowType(RelDataTypeFactory factory) {
      return factory.createStructType(types, fieldNames);
    }
  }

  /**
   * Visitor that finds the set of inputs that are used.
   */
  private static class ProjectFieldsVisitor extends RexVisitorImpl<PathSegment> {
    private final List<String> fieldNames;
    private final List<RelDataTypeField> fields;

    private final Set<SchemaPath> newFields = Sets.newLinkedHashSet();
    private final Map<String, DesiredField> desiredFields = new LinkedHashMap<>();

    ProjectFieldsVisitor(RelDataType rowType) {
      super(true);
      this.fieldNames = rowType.getFieldNames();
      this.fields = rowType.getFieldList();
    }

    void addField(PathSegment segment) {
      if (segment != null && segment instanceof PathSegment.NameSegment) {
        newFields.add(new SchemaPath((PathSegment.NameSegment) segment));
      }
    }

    ProjectPushInfo getInfo() {
      return new ProjectPushInfo(ImmutableList.copyOf(newFields), ImmutableMap.copyOf(desiredFields));
    }

    @Override
    public PathSegment visitInputRef(RexInputRef inputRef) {
      int index = inputRef.getIndex();
      String name = fieldNames.get(index);
      RelDataTypeField field = fields.get(index);
      addDesiredField(name, field.getType(), inputRef);
      return new PathSegment.NameSegment(name);
    }

    @Override
    public PathSegment visitCall(RexCall call) {
      String itemStarFieldName = FieldsReWriterUtil.getFieldNameFromItemStarField(call, fieldNames);
      if (itemStarFieldName != null) {
        addDesiredField(itemStarFieldName, call.getType(), call);
        return new PathSegment.NameSegment(itemStarFieldName);
      }

      if (SqlStdOperatorTable.ITEM.equals(call.getOperator())) {
        PathSegment mapOrArray = call.operands.get(0).accept(this);
        if (mapOrArray != null) {
          if (call.operands.get(1) instanceof RexLiteral) {
            return mapOrArray.cloneWithNewChild(Utilities.convertLiteral((RexLiteral) call.operands.get(1)));
          }
          return mapOrArray;
        }
      } else {
        for (RexNode operand : call.operands) {
          addField(operand.accept(this));
        }
      }
      return null;
    }

    private void addDesiredField(String name, RelDataType type, RexNode originalNode) {
      DesiredField desiredField = desiredFields.get(name);
      if (desiredField == null) {
        desiredFields.put(name, new DesiredField(name, type, originalNode));
      } else {
        desiredField.addNode(originalNode);
      }
    }
  }

}
