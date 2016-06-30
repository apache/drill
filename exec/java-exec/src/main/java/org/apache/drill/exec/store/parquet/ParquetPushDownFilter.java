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
package org.apache.drill.exec.store.parquet;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import com.google.common.collect.ImmutableList;

public abstract class ParquetPushDownFilter extends StoragePluginOptimizerRule {
    public static final StoragePluginOptimizerRule getFilterOnProject(final OptimizerRulesContext context){
        return new ParquetPushDownFilter(
                RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
                "ParquetPushDownFilter:Filter_On_Project", context) {

            @Override
            public boolean matches(RelOptRuleCall call) {
                if (!enabled) {
                    return false;
                }
                final ScanPrel scan = call.rel(2);
                if (scan.getGroupScan() instanceof ParquetGroupScan) {
                    return super.matches(call);
                }
                return false;
            }

            @Override
            public void onMatch(RelOptRuleCall call) {
                final FilterPrel filterRel = call.rel(0);
                final ProjectPrel projectRel = call.rel(1);
                final ScanPrel scanRel = call.rel(2);
                doOnMatch(call, filterRel, projectRel, scanRel);
            };
        };
    }

    public static final StoragePluginOptimizerRule getFilterOnScan(final OptimizerRulesContext context){
        return new ParquetPushDownFilter(
                RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
                "ParquetPushDownFilter:Filter_On_Scan", context) {

            @Override
            public boolean matches(RelOptRuleCall call) {
                if (!enabled) {
                    return false;
                }
                final ScanPrel scan = call.rel(1);
                if (scan.getGroupScan() instanceof ParquetGroupScan) {
                    return super.matches(call);
                }
                return false;
            }

            @Override
            public void onMatch(RelOptRuleCall call) {
                final FilterPrel filterRel = call.rel(0);
                final ScanPrel scanRel = call.rel(1);
                doOnMatch(call, filterRel, null, scanRel);
            }
        };
    }

    private final OptimizerRulesContext context;
    // private final boolean useNewReader;
    protected final boolean enabled;

    private ParquetPushDownFilter(RelOptRuleOperand operand, String id, OptimizerRulesContext context) {
        super(operand, id);
        this.context = context;
        this.enabled = context.getPlannerSettings().isParquetFilterPushEnabled();
        // this.useNewReader = context.getPlannerSettings()getOptions().getOption(ExecConstants.PARQUET_NEW_RECORD_READER).bool_val;
    }

    protected void doOnMatch(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan) {
        ParquetGroupScan groupScan = (ParquetGroupScan) scan.getGroupScan();
        if (groupScan.getFilter() != null) {
            return;
        }

        RexNode condition = null;
        if(project == null){
            condition = filter.getCondition();
        }else{
            // get the filter as if it were below the projection.
            condition = RelOptUtil.pushFilterPastProject(filter.getCondition(), project);
        }

        LogicalExpression conditionExp = DrillOptiq.toDrill(
                new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        ParquetFilterBuilder parquetFilterBuilder = new ParquetFilterBuilder(groupScan,
                conditionExp);
        ParquetGroupScan newGroupScan = parquetFilterBuilder.parseTree();
        if (newGroupScan == null) {
            return; // no filter pushdown so nothing to apply.
        }

        final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(),
                newGroupScan, scan.getRowType());

        RelNode inputPrel = newScanPrel;

        if(project != null){
            inputPrel = project.copy(project.getTraitSet(), ImmutableList.of(inputPrel));
        }

        // Normally we could eliminate the filter if all expressions were pushed down;
        // however, the Parquet filter implementation is type specific (whereas Drill is not)
        final RelNode newFilter = filter.copy(filter.getTraitSet(), ImmutableList.of(inputPrel));
        call.transformTo(newFilter);
    }
}
