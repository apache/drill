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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RuleSet;

import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.ConvertCountToDirectScan;
import org.apache.drill.exec.planner.physical.FilterPrule;
import org.apache.drill.exec.planner.physical.HashAggPrule;
import org.apache.drill.exec.planner.physical.HashJoinPrule;
import org.apache.drill.exec.planner.physical.LimitPrule;
import org.apache.drill.exec.planner.physical.MergeJoinPrule;
import org.apache.drill.exec.planner.physical.NestedLoopJoinPrule;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.physical.ProjectPrule;
import org.apache.drill.exec.planner.physical.PushLimitToTopN;
import org.apache.drill.exec.planner.physical.ScanPrule;
import org.apache.drill.exec.planner.physical.ScreenPrule;
import org.apache.drill.exec.planner.physical.SortConvertPrule;
import org.apache.drill.exec.planner.physical.SortPrule;
import org.apache.drill.exec.planner.physical.StreamAggPrule;
import org.apache.drill.exec.planner.physical.ValuesPrule;
import org.apache.drill.exec.planner.physical.WindowPrule;
import org.apache.drill.exec.planner.physical.UnionAllPrule;
import org.apache.drill.exec.planner.physical.WriterPrule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

public class DrillRuleSets {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRuleSets.class);

  public static RuleSet DRILL_BASIC_RULES = null;

  /**
   * Get a list of logical rules that can be turned on or off by session/system options.
   *
   * If a rule is intended to always be included with the logical set, it should be added
   * to the immutable list created in the getDrillBasicRules() method below.
   *
   * @param optimizerRulesContext - used to get the list of planner settings, other rules may
   *                                also in the future need to get other query state from this,
   *                                such as the available list of UDFs (as is used by the
   *                                DrillMergeProjectRule created in getDrillBasicRules())
   * @return - a list of rules that have been filtered to leave out
   *         rules that have been turned off by system or session settings
   */
  public static RuleSet getDrillUserConfigurableLogicalRules(OptimizerRulesContext optimizerRulesContext) {
    PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    // This list is used to store rules that can be turned on an off
    // by user facing planning options
    Builder userConfigurableRules = ImmutableSet.<RelOptRule>builder();

    if (ps.isConstantFoldingEnabled()) {
      // TODO - DRILL-2218
      userConfigurableRules.add(ReduceExpressionsRule.PROJECT_INSTANCE);

      userConfigurableRules.add(DrillReduceExpressionsRule.FILTER_INSTANCE_DRILL);
      userConfigurableRules.add(DrillReduceExpressionsRule.CALC_INSTANCE_DRILL);
    }

    return new DrillRuleSet(userConfigurableRules.build());
  }

  /**
   * Get an immutable list of rules that will always be used when running
   * logical planning.
   *
   * This would be a static member, rather than a method, but some of
   * the rules need a reference to state that isn't available at class
   * load time. The current example is the DrillMergeProjectRule which
   * needs access to the registry of Drill UDFs, which is populated by
   * scanning the class path a Drillbit startup.
   *
   * If a logical rule needs to be user configurable, such as turning
   * it on and off with a system/session option, add it in the
   * getDrillUserConfigurableLogicalRules() method instead of here.
   *
   * @param optimizerRulesContext - shared state used during planning, currently used here
   *                                to gain access to the function registry described above.
   * @return - a RuleSet containing the logical rules that will always
   *           be used, either by VolcanoPlanner directly, or
   *           used VolcanoPlanner as pre-processing for LOPTPlanner.
   *
   * Note : Join permutation rule is excluded here.
   */
  public static RuleSet getDrillBasicRules(OptimizerRulesContext optimizerRulesContext) {
    if (DRILL_BASIC_RULES == null) {

      DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.<RelOptRule> builder().add( //
      // Add support for Distinct Union (by using Union-All followed by Distinct)
      UnionToDistinctRule.INSTANCE,

      // Add support for WHERE style joins.
      DrillFilterJoinRules.DRILL_FILTER_ON_JOIN,
      DrillFilterJoinRules.DRILL_JOIN,
      // End support for WHERE style joins.

      /*
       Filter push-down related rules
       */
      DrillPushFilterPastProjectRule.INSTANCE,
      FilterSetOpTransposeRule.INSTANCE,

      FilterMergeRule.INSTANCE,
      AggregateRemoveRule.INSTANCE,
      ProjectRemoveRule.NAME_CALC_INSTANCE,
      SortRemoveRule.INSTANCE,

      DrillMergeProjectRule.getInstance(true, RelFactories.DEFAULT_PROJECT_FACTORY,
          optimizerRulesContext.getFunctionRegistry()),
      AggregateExpandDistinctAggregatesRule.INSTANCE,
      DrillReduceAggregatesRule.INSTANCE,

      /*
       Projection push-down related rules
       */
      DrillPushProjectPastFilterRule.INSTANCE,
      DrillPushProjectPastJoinRule.INSTANCE,
      DrillPushProjIntoScan.INSTANCE,
      DrillProjectSetOpTransposeRule.INSTANCE,

      PruneScanRule.getFilterOnProject(optimizerRulesContext),
      PruneScanRule.getFilterOnScan(optimizerRulesContext),
      PruneScanRule.getFilterOnProjectParquet(optimizerRulesContext),
      PruneScanRule.getFilterOnScanParquet(optimizerRulesContext),

      /*
       Convert from Calcite Logical to Drill Logical Rules.
       */
      ExpandConversionRule.INSTANCE,
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillWindowRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionAllRule.INSTANCE,
      DrillValuesRule.INSTANCE
      )
      .build());
    }

    return DRILL_BASIC_RULES;
  }

  // Ruleset for join permutation, used only in VolcanoPlanner.
  public static RuleSet getJoinPermRules(OptimizerRulesContext optimizerRulesContext) {
    return new DrillRuleSet(ImmutableSet.<RelOptRule> builder().add( //
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT
        ).build());
  }

  public static final RuleSet DRILL_PHYSICAL_DISK = new DrillRuleSet(ImmutableSet.of( //
      ProjectPrule.INSTANCE

    ));

  public static final RuleSet getPhysicalRules(OptimizerRulesContext optimizerRulesContext) {
    List<RelOptRule> ruleList = new ArrayList<RelOptRule>();

    PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    ruleList.add(ConvertCountToDirectScan.AGG_ON_PROJ_ON_SCAN);
    ruleList.add(ConvertCountToDirectScan.AGG_ON_SCAN);
    ruleList.add(SortConvertPrule.INSTANCE);
    ruleList.add(SortPrule.INSTANCE);
    ruleList.add(ProjectPrule.INSTANCE);
    ruleList.add(ScanPrule.INSTANCE);
    ruleList.add(ScreenPrule.INSTANCE);
    ruleList.add(ExpandConversionRule.INSTANCE);
    ruleList.add(FilterPrule.INSTANCE);
    ruleList.add(LimitPrule.INSTANCE);
    ruleList.add(WriterPrule.INSTANCE);
    ruleList.add(WindowPrule.INSTANCE);
    ruleList.add(PushLimitToTopN.INSTANCE);
    ruleList.add(UnionAllPrule.INSTANCE);
    ruleList.add(ValuesPrule.INSTANCE);

    if (ps.isHashAggEnabled()) {
      ruleList.add(HashAggPrule.INSTANCE);
    }

    if (ps.isStreamAggEnabled()) {
      ruleList.add(StreamAggPrule.INSTANCE);
    }

    if (ps.isHashJoinEnabled()) {
      ruleList.add(HashJoinPrule.DIST_INSTANCE);

      if(ps.isBroadcastJoinEnabled()){
        ruleList.add(HashJoinPrule.BROADCAST_INSTANCE);
      }
    }

    if (ps.isMergeJoinEnabled()) {
      ruleList.add(MergeJoinPrule.DIST_INSTANCE);

      if(ps.isBroadcastJoinEnabled()){
        ruleList.add(MergeJoinPrule.BROADCAST_INSTANCE);
      }

    }

    // NLJ plans consist of broadcasting the right child, hence we need
    // broadcast join enabled.
    if (ps.isNestedLoopJoinEnabled() && ps.isBroadcastJoinEnabled()) {
      ruleList.add(NestedLoopJoinPrule.INSTANCE);
    }

    return new DrillRuleSet(ImmutableSet.copyOf(ruleList));
  }

  public static RuleSet create(ImmutableSet<RelOptRule> rules) {
    return new DrillRuleSet(rules);
  }

  public static RuleSet mergedRuleSets(RuleSet...ruleSets) {
    Builder<RelOptRule> relOptRuleSetBuilder = ImmutableSet.builder();
    for (RuleSet ruleSet : ruleSets) {
      for (RelOptRule relOptRule : ruleSet) {
        relOptRuleSetBuilder.add(relOptRule);
      }
    }
    return new DrillRuleSet(relOptRuleSetBuilder.build());
  }

  private static class DrillRuleSet implements RuleSet{
    final ImmutableSet<RelOptRule> rules;

    public DrillRuleSet(ImmutableSet<RelOptRule> rules) {
      super();
      this.rules = rules;
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }
}
