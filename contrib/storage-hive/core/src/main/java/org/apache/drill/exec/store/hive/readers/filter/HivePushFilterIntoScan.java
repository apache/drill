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
package org.apache.drill.exec.store.hive.readers.filter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.hive.HiveScan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg.SARG_PUSHDOWN;

public abstract class HivePushFilterIntoScan extends StoragePluginOptimizerRule {

  private HivePushFilterIntoScan(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static final StoragePluginOptimizerRule FILTER_ON_SCAN =
      new HivePushFilterIntoScan(RelOptHelper.some(FilterPrel.class,
          RelOptHelper.any(ScanPrel.class)), "HivePushFilterIntoScan:Filter_On_Scan") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final ScanPrel scan = call.rel(1);
      final FilterPrel filter = call.rel(0);
      final RexNode condition = filter.getCondition();

      HiveScan groupScan = (HiveScan) scan.getGroupScan();
      if (groupScan.isFilterPushedDown()) {
        /*
         * The rule can get triggered again due to the transformed "scan => filter" sequence
         * created by the earlier execution of this rule when we could not do a complete
         * conversion of Optiq Filter's condition to Hive Filter. In such cases, we rely upon
         * this flag to not do a re-processing of the rule on the already transformed call.
         */
        return;
      }

      doPushFilterToScan(call, filter, null, scan, groupScan, condition);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanPrel scan = call.rel(1);
      if (scan.getGroupScan() instanceof HiveScan) {
        return super.matches(call);
      }
      return false;
    }
  };

  public static final StoragePluginOptimizerRule FILTER_ON_PROJECT =
      new HivePushFilterIntoScan(RelOptHelper.some(FilterPrel.class,
          RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
          "HivePushFilterIntoScan:Filter_On_Project") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final ScanPrel scan = call.rel(2);
      final ProjectPrel project = call.rel(1);
      final FilterPrel filter = call.rel(0);

      HiveScan groupScan = (HiveScan) scan.getGroupScan();
      if (groupScan.isFilterPushedDown()) {
        /*
         * The rule can get triggered again due to the transformed "scan => filter" sequence
         * created by the earlier execution of this rule when we could not do a complete
         * conversion of Optiq Filter's condition to Hive Filter. In such cases, we rely upon
         * this flag to not do a re-processing of the rule on the already transformed call.
         */
        return;
      }

      // convert the filter to one that references the child of the project
      final RexNode condition = RelOptUtil.pushPastProject(filter.getCondition(), project);

      doPushFilterToScan(call, filter, project, scan, groupScan, condition);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final ScanPrel scan = call.rel(2);
      if (scan.getGroupScan() instanceof HiveScan) {
        return super.matches(call);
      }
      return false;
    }
  };


  protected void doPushFilterToScan(final RelOptRuleCall call, final FilterPrel filter,
      final ProjectPrel project, final ScanPrel scan, final HiveScan groupScan,
      final RexNode condition) {

    HashMap<String, SqlTypeName> dataTypeMap = new HashMap<>();
    List<RelDataTypeField> fieldList = scan.getRowType().getFieldList();
    for (RelDataTypeField relDataTypeField : fieldList) {
      String name = relDataTypeField.getName();
      SqlTypeName sqlTypeName = relDataTypeField.getValue().getSqlTypeName();
      dataTypeMap.put(name, sqlTypeName);
    }

    final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())),
            scan, condition);
    final HiveFilterBuilder orcFilterBuilder = new HiveFilterBuilder(conditionExp, dataTypeMap);
    final HiveFilter newScanSpec = orcFilterBuilder.parseTree();

    if (newScanSpec == null) {
      return; //no filter pushdown ==> No transformation.
    }

    String searchArgument = newScanSpec.getSearchArgumentString();
    Map<String, String> confProperties = groupScan.getConfProperties();
    confProperties.put(SARG_PUSHDOWN, searchArgument);
    HiveScan newGroupsScan = null;
    try {
      newGroupsScan = new HiveScan(groupScan.getUserName(), groupScan.getHiveReadEntry(),
          groupScan.getStoragePlugin(), groupScan.getColumns(), null, confProperties, groupScan.getMaxRecords());
    } catch (ExecutionSetupException e) {
      //Why does constructor method HiveScan throw exception?
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    newGroupsScan.setFilterPushedDown(true);

    final ScanPrel newScanPrel = new ScanPrel(scan.getCluster(), filter.getTraitSet(),
        newGroupsScan, scan.getRowType(), scan.getTable());
    // Depending on whether is a project in the middle, assign either scan or copy of project to
    // childRel.
    final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(),
        ImmutableList.of(newScanPrel));
    /*
     * we could not convert the entire filter condition expression into an Hive orc filter.
     */
    call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
  }
}
