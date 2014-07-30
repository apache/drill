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

import java.util.List;

import net.hydromatic.optiq.Schema.TableType;
import net.hydromatic.optiq.Statistic;
import net.hydromatic.optiq.Statistics;
import net.hydromatic.optiq.TranslatableTable;

import org.apache.drill.exec.dotdrill.View;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

public class DrillViewTable implements TranslatableTable, DrillViewInfoProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillViewTable.class);

  private final View view;

  public DrillViewTable(List<String> path, View view){
    this.view = view;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return view.getRowType(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    RelDataType rowType = relOptTable.getRowType();
    RelNode rel = context.expandView(rowType, view.getSql(), view.getWorkspaceSchemaPath());

    if (view.isDynamic()){
      return rel;
    }else{
      // if the View's field list is not "*", try to create a cast.
      return RelOptUtil.createCastRel(rel, rowType, true);
    }
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.VIEW;
  }

  @Override
  public String getViewSql() {
    return view.getSql();
  }
}
