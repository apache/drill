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
import net.hydromatic.optiq.impl.ViewTable;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.*;

import java.util.List;

public class DrillViewTable extends ViewTable {
  private RelDataType rowType;
  private boolean starSchema = false; // is the view schema "*"?
  private RelDataTypeHolder holder = new RelDataTypeHolder();

  public DrillViewTable(String viewSql, List<String> schemaPath, RelDataType rowType) {
    super(Object.class, null, viewSql, schemaPath);

    this.rowType = rowType;
    if (rowType.getFieldCount() == 1 && rowType.getFieldNames().get(0).equals("*"))
      starSchema = true;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    // if the view's schema is a "*" schema, create dynamic row type. Otherwise create fixed row type.
    if (starSchema)
      return new RelDataTypeDrillImpl(holder, typeFactory);

    return rowType;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    RelDataType rowType = relOptTable.getRowType();
    RelNode rel = context.expandView(rowType, getViewSql(), getSchemaPath());

    // if the View's field list is not "*", try to create a cast.
    if (!starSchema)
      return RelOptUtil.createCastRel(rel, rowType, true);

    return rel;
  }
}
