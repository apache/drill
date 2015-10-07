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
package org.apache.drill.exec.skiprecord;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;

import java.util.List;

public class RecordContextVisitor extends RelShuttleImpl {
  public static final int MAX_VARCHAR_LENGTH = 65535;
  public static final String VIRTUAL_COLUMN_PREFIX = "$";
  public static final String FILE_NAME = "File_Name";
  public static final String ROW_NUMBER = "Row_Number";
  private int[] rangeVirtualCols;

  public RecordContextVisitor() {
    this.rangeVirtualCols = new int[2];
  }

  public RelNode visit(RelNode other) {
    if(!(other instanceof DrillProjectRel)) {
      return super.visit(other);
    }

    final DrillProjectRel project = (DrillProjectRel) other;
    final DrillProjectRel drillProjectRel = (DrillProjectRel) visitChild(project, 0, project.getInput());

    final int fromInclu = rangeVirtualCols[0];
    final int toExclu = rangeVirtualCols[1];
    if(fromInclu == toExclu) {
      return drillProjectRel;
    }

    final List<RelDataTypeField> newFields
        = Lists.newArrayList(drillProjectRel.getRowType().getFieldList());
    final List<RexNode> newExps
        = Lists.newArrayList(drillProjectRel.getChildExps());

    // Add the projects for the virtual columns
    final RelDataType inRowType = drillProjectRel.getInput(0).getRowType();
    for(int i = 0; i < inRowType.getFieldCount(); ++i) {
      final RelDataTypeField inField = inRowType.getFieldList().get(i);
      final String fieldName = inField.getName();
      final RelDataType type = inField.getType();
      if(fieldName.startsWith(VIRTUAL_COLUMN_PREFIX)) {
        final RelDataTypeField newField = new RelDataTypeFieldImpl(fieldName, newFields.size(), type);
        newFields.add(newField);
        final RexNode proj = new RexInputRef(i, type);
        newExps.add(proj);
      }
    }

    /*
    for(int i = inRowType.getFieldCount() - (toExclu - fromInclu); i < inRowType.getFieldCount(); ++i) {
      final RelDataTypeField oldField = inRowType.getFieldList().get(i);
      final RelDataTypeField newField = new RelDataTypeFieldImpl(oldField.getName(), newFields.size(), oldField.getType());
      newFields.add(newField);

      final RexNode proj = new RexInputRef(i, oldField.getType());
      newExps.add(proj);
    }*/

    return drillProjectRel.copy(
        drillProjectRel.getTraitSet(),
        drillProjectRel.getInput(),
        newExps,
        new RelRecordType(newFields));
  }

  @Override
  public RelNode visit(TableScan scan) {
    final int toExclu = rangeVirtualCols[1];
    final DrillScanRel drillScanRel = (DrillScanRel) scan;

    final GroupScan groupScan = drillScanRel.getGroupScan();

    final RelDataTypeFactory factory = scan.getCluster().getTypeFactory();
    final RelDataType relDataType = scan.getRowType();
    final RelDataType varcharType = factory.createSqlType(SqlTypeName.VARCHAR, MAX_VARCHAR_LENGTH);

    final List<String> recordContext = groupScan.getRecordContextInScan();
    final List<String> virtualColName = Lists.newArrayList();
    for(final String rc : recordContext) {
      virtualColName.add(VIRTUAL_COLUMN_PREFIX + rc + toExclu);
    }

    final List<RelDataTypeField> newFields = Lists.newArrayList(relDataType.getFieldList());
    final List<SchemaPath> newColumns = Lists.newArrayList(drillScanRel.getColumns());
    for(final String colName : virtualColName) {
      newFields.add(new RelDataTypeFieldImpl(colName,
              relDataType.getFieldCount(),
              varcharType));

      newColumns.add(SchemaPath.getSimplePath(colName));
    }

    final DrillScanRel newDrillScanRel = new DrillScanRel(
        drillScanRel.getCluster(),
        drillScanRel.getTraitSet(),
        drillScanRel.getTable(),
        new RelRecordType(newFields),
        newColumns,
        drillScanRel.partitionFilterPushdown());

    ++rangeVirtualCols[1];
    return newDrillScanRel;
  }
}