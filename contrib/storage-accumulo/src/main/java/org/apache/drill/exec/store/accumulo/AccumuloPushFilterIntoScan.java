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
package org.apache.drill.exec.store.accumulo;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import com.google.common.collect.ImmutableList;

/**
 * Optimizer rule to push filter predicates into Accumulo scans.
 *
 * <p>This rule matches Filter → Scan patterns (optionally with a Project in between)
 * and pushes row_key predicates down to the Accumulo scan as row ranges.</p>
 *
 * <p>Supported patterns:</p>
 * <ul>
 *   <li>Filter → ScanPrel (AccumuloGroupScan)</li>
 *   <li>Filter → Project → ScanPrel (AccumuloGroupScan)</li>
 * </ul>
 */
public abstract class AccumuloPushFilterIntoScan extends StoragePluginOptimizerRule {

  private AccumuloPushFilterIntoScan(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  /**
   * Rule for Filter directly on Scan.
   */
  public static final StoragePluginOptimizerRule FILTER_ON_SCAN =
      new AccumuloPushFilterIntoScan(
          RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
          "AccumuloPushFilterIntoScan:Filter_On_Scan") {

        @Override
        public void onMatch(RelOptRuleCall call) {
          final ScanPrel scan = call.rel(1);
          final FilterPrel filter = call.rel(0);
          final RexNode condition = filter.getCondition();

          AccumuloGroupScan groupScan = (AccumuloGroupScan) scan.getGroupScan();
          if (groupScan.isFilterPushedDown()) {
            // Already processed - don't re-process
            return;
          }

          doPushFilterToScan(call, filter, null, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
          final ScanPrel scan = call.rel(1);
          if (scan.getGroupScan() instanceof AccumuloGroupScan) {
            return super.matches(call);
          }
          return false;
        }
      };

  /**
   * Rule for Filter on Project on Scan.
   */
  public static final StoragePluginOptimizerRule FILTER_ON_PROJECT =
      new AccumuloPushFilterIntoScan(
          RelOptHelper.some(FilterPrel.class,
              RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
          "AccumuloPushFilterIntoScan:Filter_On_Project") {

        @Override
        public void onMatch(RelOptRuleCall call) {
          final ScanPrel scan = call.rel(2);
          final ProjectPrel project = call.rel(1);
          final FilterPrel filter = call.rel(0);

          AccumuloGroupScan groupScan = (AccumuloGroupScan) scan.getGroupScan();
          if (groupScan.isFilterPushedDown()) {
            // Already processed - don't re-process
            return;
          }

          // Push filter through project
          final RexNode condition = RelOptUtil.pushPastProject(filter.getCondition(), project);

          doPushFilterToScan(call, filter, project, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
          final ScanPrel scan = call.rel(2);
          if (scan.getGroupScan() instanceof AccumuloGroupScan) {
            return super.matches(call);
          }
          return false;
        }
      };

  /**
   * Pushes filter conditions to the Accumulo scan.
   */
  protected void doPushFilterToScan(
      final RelOptRuleCall call,
      final FilterPrel filter,
      final ProjectPrel project,
      final ScanPrel scan,
      final AccumuloGroupScan groupScan,
      final RexNode condition) {

    // Convert RexNode to Drill LogicalExpression
    final LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())),
        scan,
        condition);

    // Build Accumulo scan spec from filter
    final AccumuloFilterBuilder filterBuilder =
        new AccumuloFilterBuilder(groupScan, conditionExp);
    final AccumuloScanSpec newScanSpec = filterBuilder.parseTree();

    if (newScanSpec == null) {
      // No filter could be pushed down
      return;
    }

    // Create new group scan with pushed filter
    final AccumuloGroupScan newGroupScan = groupScan.cloneWithNewScanSpec(newScanSpec);
    newGroupScan.setFilterPushedDown(true);

    // Create new scan prel
    final ScanPrel newScanPrel = new ScanPrel(
        scan.getCluster(),
        filter.getTraitSet(),
        newGroupScan,
        scan.getRowType(),
        scan.getTable());

    // If there's a project, keep it
    final RelNode childRel = project == null
        ? newScanPrel
        : project.copy(project.getTraitSet(), ImmutableList.of(newScanPrel));

    if (filterBuilder.isAllExpressionsConverted()) {
      // All filter conditions were pushed - remove the filter operator
      call.transformTo(childRel);
    } else {
      // Partial pushdown - keep filter for remaining conditions
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
    }
  }
}
