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
package org.apache.drill.exec.store.mpjdbc;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

public class MPJdbcFilterRule extends StoragePluginOptimizerRule {
  public static final StoragePluginOptimizerRule INSTANCE = new MPJdbcFilterRule();
  public MPJdbcFilterRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
    // TODO Auto-generated constructor stub
  }

  public MPJdbcFilterRule() {
    // TODO Auto-generated constructor stub
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "MPJdbcFilterRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO Auto-generated method stub
    final ScanPrel scan = (ScanPrel) call.rel(1);
    final FilterPrel filter = (FilterPrel) call.rel(0);
    final RexNode condition = filter.getCondition();
    final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(), scan, condition);
    MPJdbcGroupScan grpScan= (MPJdbcGroupScan) scan.getGroupScan();
    MPJdbcFilterBuilder builder = new MPJdbcFilterBuilder(grpScan,conditionExp);
    MPJdbcScanSpec result = builder.parseTree();
  }

}
