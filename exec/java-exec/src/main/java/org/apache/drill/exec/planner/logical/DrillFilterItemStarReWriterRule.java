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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.drill.exec.util.Utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.exec.planner.logical.FieldsReWriterUtil.DesiredField;
import static org.apache.drill.exec.planner.logical.FieldsReWriterUtil.FieldsReWriter;

/**
 * Rule will transform filter -> project -> scan call with item star fields in filter
 * into project -> filter -> project -> scan where item star fields are pushed into scan
 * and replaced with actual field references.
 *
 * This will help partition pruning and push down rules to detect fields that can be pruned or push downed.
 * Item star operator appears when sub-select or cte with star are used as source.
 */
public class DrillFilterItemStarReWriterRule extends RelOptRule {

  public static final DrillFilterItemStarReWriterRule INSTANCE = new DrillFilterItemStarReWriterRule(
      RelOptHelper.some(Filter.class, RelOptHelper.some(Project.class, RelOptHelper.any( TableScan.class))),
      "DrillFilterItemStarReWriterRule");

  private DrillFilterItemStarReWriterRule(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filterRel = call.rel(0);
    Project projectRel = call.rel(1);
    TableScan scanRel = call.rel(2);

    ItemStarFieldsVisitor itemStarFieldsVisitor = new ItemStarFieldsVisitor(filterRel.getRowType().getFieldNames());
    filterRel.getCondition().accept(itemStarFieldsVisitor);

    // there are no item fields, no need to proceed further
    if (!itemStarFieldsVisitor.hasItemStarFields()) {
      return;
    }

    Map<String, DesiredField> itemStarFields = itemStarFieldsVisitor.getItemStarFields();

    // create new scan
    RelNode newScan = constructNewScan(scanRel, itemStarFields.keySet());

    // combine original and new projects
    List<RexNode> newProjects = new ArrayList<>(projectRel.getProjects());

    // prepare node mapper to replace item star calls with new input field references
    Map<RexNode, Integer> fieldMapper = new HashMap<>();

    // since scan might have already some fields, new field reference index should start from the last used in scan
    // NB: field reference index starts from 0 thus original field count can be taken as starting index
    int index = scanRel.getRowType().getFieldCount();

    for (DesiredField desiredField : itemStarFields.values()) {
      RexInputRef inputRef = new RexInputRef(index, desiredField.getType());
      // add references to item star fields in new project
      newProjects.add(inputRef);
      for (RexNode node : desiredField.getNodes()) {
        // if field is referenced in more then one call, add each call to field mapper
        fieldMapper.put(node, index);
      }
      // increment index for the next node reference
      index++;
    }

    // create new project row type
    RelDataType newProjectRowType = getNewRowType(
        projectRel.getCluster().getTypeFactory(),
        projectRel.getRowType().getFieldList(),
        itemStarFields.keySet());

    // create new project
    RelNode newProject = new LogicalProject(projectRel.getCluster(), projectRel.getTraitSet(), newScan, newProjects, newProjectRowType);

    // transform filter condition
    FieldsReWriter fieldsReWriter = new FieldsReWriter(fieldMapper);
    RexNode newCondition = filterRel.getCondition().accept(fieldsReWriter);

    // create new filter
    RelNode newFilter = new LogicalFilter(filterRel.getCluster(), filterRel.getTraitSet(), newProject, newCondition, ImmutableSet.<CorrelationId>of());

    // wrap with project to have the same row type as before
    Project wrapper = projectRel.copy(projectRel.getTraitSet(), newFilter, projectRel.getProjects(), projectRel.getRowType());

    call.transformTo(wrapper);
  }

  /**
   * Creates new row type with merged original and new fields.
   *
   * @param typeFactory type factory
   * @param originalFields original fields
   * @param newFields new fields
   * @return new row type with original and new fields
   */
  private RelDataType getNewRowType(RelDataTypeFactory typeFactory,
                                    List<RelDataTypeField> originalFields,
                                    Collection<String> newFields) {
    RelDataTypeHolder relDataTypeHolder = new RelDataTypeHolder();

    // add original fields
    for (RelDataTypeField field : originalFields) {
      relDataTypeHolder.getField(typeFactory, field.getName());
    }

    // add new fields
    for (String fieldName : newFields) {
      relDataTypeHolder.getField(typeFactory, fieldName);
    }

    return new RelDataTypeDrillImpl(relDataTypeHolder, typeFactory);
  }

  /**
   * Constructs new scan based on the original scan.
   * Preserves all original fields and add new fields.
   *
   * @param scanRel original scan
   * @param newFields new fields
   * @return new scan with original and new fields
   */
  private RelNode constructNewScan(TableScan scanRel, Collection<String> newFields) {
    // create new scan row type
    RelDataType newScanRowType = getNewRowType(
        scanRel.getCluster().getTypeFactory(),
        scanRel.getRowType().getFieldList(),
        newFields);

    // create new scan
    RelOptTable table = scanRel.getTable();
    Class elementType = EnumerableTableScan.deduceElementType(table.unwrap(Table.class));

    DrillTable unwrap = Utilities.getDrillTable(table);
    DrillTranslatableTable newTable = new DrillTranslatableTable(
        new DynamicDrillTable(unwrap.getPlugin(), unwrap.getStorageEngineName(), unwrap.getUserName(), unwrap.getSelection()));
    RelOptTableImpl newOptTableImpl = RelOptTableImpl.create(table.getRelOptSchema(), newScanRowType, newTable, ImmutableList.<String>of());

    return new EnumerableTableScan(scanRel.getCluster(), scanRel.getTraitSet(), newOptTableImpl, elementType);
  }

  /**
   * Traverses given node and stores all item star fields.
   * For the fields with the same name, stores original calls in a list, does not duplicate fields.
   * Holds state, should not be re-used.
   */
  private class ItemStarFieldsVisitor extends RexVisitorImpl<RexNode> {

    private final Map<String, DesiredField> itemStarFields = new HashMap<>();
    private final List<String> fieldNames;

    ItemStarFieldsVisitor(List<String> fieldNames) {
      super(true);
      this.fieldNames = fieldNames;
    }

    boolean hasItemStarFields() {
      return !itemStarFields.isEmpty();
    }

    Map<String, DesiredField> getItemStarFields() {
      return itemStarFields;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // need to figure out field name and index
      String fieldName = FieldsReWriterUtil.getFieldNameFromItemStarField(call, fieldNames);
      if (fieldName != null) {
        // if there is call to the already existing field, store call, do not duplicate field
        DesiredField desiredField = itemStarFields.get(fieldName);
        if (desiredField == null) {
          itemStarFields.put(fieldName, new DesiredField(fieldName, call.getType(), call));
        } else {
          desiredField.addNode(call);
        }
      }

      return super.visitCall(call);
    }

  }

}
