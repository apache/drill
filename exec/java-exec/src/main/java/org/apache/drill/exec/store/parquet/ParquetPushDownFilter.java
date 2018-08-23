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
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate;
import org.apache.drill.exec.expr.stat.ParquetFilterPredicate.RowsMatch;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.common.DrillRelOptUtil;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class ParquetPushDownFilter extends StoragePluginOptimizerRule {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetPushDownFilter.class);

  private static final Collection<String> BANNED_OPERATORS;

  static {
    BANNED_OPERATORS = new ArrayList<>(1);
    BANNED_OPERATORS.add("flatten");
  }

  public static RelOptRule getFilterOnProject(OptimizerRulesContext optimizerRulesContext) {
    return new ParquetPushDownFilter(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))),
        "ParquetPushDownFilter:Filter_On_Project", optimizerRulesContext) {

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(2);
        if (scan.getGroupScan() instanceof AbstractParquetGroupScan) {
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
      }

    };
  }

  public static StoragePluginOptimizerRule getFilterOnScan(OptimizerRulesContext optimizerContext) {
    return new ParquetPushDownFilter(
        RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)),
        "ParquetPushDownFilter:Filter_On_Scan", optimizerContext) {

      @Override
      public boolean matches(RelOptRuleCall call) {
        final ScanPrel scan = call.rel(1);
        if (scan.getGroupScan() instanceof AbstractParquetGroupScan) {
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

  // private final boolean useNewReader;
  protected final OptimizerRulesContext optimizerContext;

  private ParquetPushDownFilter(RelOptRuleOperand operand, String id, OptimizerRulesContext optimizerContext) {
    super(operand, id);
    this.optimizerContext = optimizerContext;
  }

  protected void doOnMatch(RelOptRuleCall call, FilterPrel filter, ProjectPrel project, ScanPrel scan) {
    AbstractParquetGroupScan groupScan = (AbstractParquetGroupScan) scan.getGroupScan();
    if (groupScan.getFilter() != null && !groupScan.getFilter().equals(ValueExpressions.BooleanExpression.TRUE)) {
      return;
    }

    RexNode condition;
    if (project == null) {
      condition = filter.getCondition();
    } else {
      // get the filter as if it were below the projection.
      condition = RelOptUtil.pushPastProject(filter.getCondition(), project);
    }

    if (condition == null || condition.isAlwaysTrue()) {
      return;
    }

    // get a conjunctions of the filter condition. For each conjunction, if it refers to ITEM or FLATTEN expression
    // then we could not pushed down. Otherwise, it's qualified to be pushed down.
    List<RexNode> predList = RelOptUtil.conjunctions(condition);

    List<RexNode> qualifiedPredList = new ArrayList<>();

    for (RexNode pred : predList) {
      if (DrillRelOptUtil.findOperators(pred, ImmutableList.of(), BANNED_OPERATORS) == null) {
        qualifiedPredList.add(pred);
      }
    }

    final RexNode qualifedPred = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), qualifiedPredList, true);

    if (qualifedPred == null) {
      return;
    }

    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, qualifedPred);


    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    final GroupScan newGroupScan = groupScan.applyFilter(conditionExp,optimizerContext,
        optimizerContext.getFunctionRegistry(), optimizerContext.getPlannerSettings().getOptions());
    if (timer != null) {
      logger.debug("Took {} ms to apply filter on parquet row groups. ", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
    }

    if (newGroupScan == null ) {
      return;
    }

    RelNode newScan = ScanPrel.create(scan, scan.getTraitSet(), newGroupScan, scan.getRowType());

    if (project != null) {
      newScan = project.copy(project.getTraitSet(), ImmutableList.of(newScan));
    }

    if (newGroupScan instanceof AbstractParquetGroupScan) {
      RowsMatch matchAll = RowsMatch.ALL;
      List<RowGroupInfo> rowGroupInfos = ((AbstractParquetGroupScan) newGroupScan).rowGroupInfos;
      for (RowGroupInfo rowGroup : rowGroupInfos) {
        if (rowGroup.getRowsMatch() != RowsMatch.ALL) {
          matchAll = RowsMatch.SOME;
          break;
        }
      }
      if (matchAll == ParquetFilterPredicate.RowsMatch.ALL) {
        call.transformTo(newScan);
      }
    }

    final RelNode newFilter = filter.copy(filter.getTraitSet(), ImmutableList.<RelNode>of(newScan));
    call.transformTo(newFilter);
  }
}
