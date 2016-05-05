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
package org.apache.drill.exec.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.logical.DrillAggregateRule;
import org.apache.drill.exec.planner.logical.DrillFilterAggregateTransposeRule;
import org.apache.drill.exec.planner.logical.DrillFilterJoinRules;
import org.apache.drill.exec.planner.logical.DrillFilterRule;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.planner.logical.DrillJoinRule;
import org.apache.drill.exec.planner.logical.DrillLimitRule;
import org.apache.drill.exec.planner.logical.DrillMergeProjectRule;
import org.apache.drill.exec.planner.logical.DrillProjectRule;
import org.apache.drill.exec.planner.logical.DrillPushFilterPastProjectRule;
import org.apache.drill.exec.planner.logical.DrillPushLimitToScanRule;
import org.apache.drill.exec.planner.logical.DrillPushProjIntoScan;
import org.apache.drill.exec.planner.logical.DrillPushProjectPastFilterRule;
import org.apache.drill.exec.planner.logical.DrillPushProjectPastJoinRule;
import org.apache.drill.exec.planner.logical.DrillReduceAggregatesRule;
import org.apache.drill.exec.planner.logical.DrillReduceExpressionsRule;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.logical.DrillScanRule;
import org.apache.drill.exec.planner.logical.DrillSortRule;
import org.apache.drill.exec.planner.logical.DrillUnionAllRule;
import org.apache.drill.exec.planner.logical.DrillValuesRule;
import org.apache.drill.exec.planner.logical.DrillWindowRule;
import org.apache.drill.exec.planner.logical.partition.ParquetPruneScanRule;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.ConvertCountToDirectScan;
import org.apache.drill.exec.planner.physical.DirectScanPrule;
import org.apache.drill.exec.planner.physical.FilterPrule;
import org.apache.drill.exec.planner.physical.HashAggPrule;
import org.apache.drill.exec.planner.physical.HashJoinPrule;
import org.apache.drill.exec.planner.physical.LimitPrule;
import org.apache.drill.exec.planner.physical.LimitUnionExchangeTransposeRule;
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
import org.apache.drill.exec.planner.physical.UnionAllPrule;
import org.apache.drill.exec.planner.physical.ValuesPrule;
import org.apache.drill.exec.planner.physical.WindowPrule;
import org.apache.drill.exec.planner.physical.WriterPrule;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.StoragePlugin;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;

public enum PlannerPhase {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRuleSets.class);

  LOGICAL_PRUNE_AND_JOIN("Loigcal Planning (with join and partition pruning)") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          getDrillBasicRules(context),
          getPruneScanRules(context),
          getJoinPermRules(context),
          getDrillUserConfigurableLogicalRules(context),
          getStorageRules(context, plugins, this));
    }
  },

  WINDOW_REWRITE("Window Function rewrites") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return RuleSets.ofList(
          ReduceExpressionsRule.CALC_INSTANCE,
          ProjectToWindowRule.PROJECT
          );
    }
  },

  LOGICAL_PRUNE("Logical Planning (with partition pruning)") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          getDrillBasicRules(context),
          getPruneScanRules(context),
          getDrillUserConfigurableLogicalRules(context),
          getStorageRules(context, plugins, this));
    }
  },

  JOIN_PLANNING("LOPT Join Planning") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          RuleSets.ofList(
              DRILL_JOIN_TO_MULTIJOIN_RULE,
              DRILL_LOPT_OPTIMIZE_JOIN_RULE,
              ProjectRemoveRule.INSTANCE),
          getStorageRules(context, plugins, this)
          );
    }
  },

  SUM_CONVERSION("Convert SUM to $SUM0") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          RuleSets.ofList(
              DrillReduceAggregatesRule.INSTANCE_SUM,
              DrillReduceAggregatesRule.INSTANCE_WINDOW_SUM),
          getStorageRules(context, plugins, this)
          );
    }
  },

  PARTITION_PRUNING("Partition Prune Planning") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(getPruneScanRules(context), getStorageRules(context, plugins, this));
    }
  },

  DIRECTORY_PRUNING("Directory Prune Planning") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(getDirPruneScanRules(context), getStorageRules(context, plugins, this));
    }
  },

  LOGICAL("Logical Planning (no pruning or join).") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          PlannerPhase.getDrillBasicRules(context),
          PlannerPhase.getDrillUserConfigurableLogicalRules(context),
          getStorageRules(context, plugins, this));
    }
  },

  PHYSICAL("Physical Planning") {
    public RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins) {
      return PlannerPhase.mergedRuleSets(
          PlannerPhase.getPhysicalRules(context),
          getStorageRules(context, plugins, this));
    }
  };

  public final String description;

  PlannerPhase(String description) {
    this.description = description;
  }

  public abstract RuleSet getRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins);

  private static RuleSet getStorageRules(OptimizerRulesContext context, Collection<StoragePlugin> plugins,
      PlannerPhase phase) {
    final Builder<RelOptRule> rules = ImmutableSet.builder();
    for(StoragePlugin plugin : plugins){
      if(plugin instanceof AbstractStoragePlugin){
        rules.addAll(((AbstractStoragePlugin) plugin).getOptimizerRules(context, phase));
      }else{
        rules.addAll(plugin.getOptimizerRules(context));
      }
    }
    return RuleSets.ofList(rules.build());
  }


  static final RelOptRule DRILL_JOIN_TO_MULTIJOIN_RULE = new JoinToMultiJoinRule(DrillJoinRel.class);
  static final RelOptRule DRILL_LOPT_OPTIMIZE_JOIN_RULE = new LoptOptimizeJoinRule(
      DrillRelFactories.DRILL_LOGICAL_JOIN_FACTORY,
      DrillRelFactories.DRILL_LOGICAL_PROJECT_FACTORY,
      DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY);

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
  static RuleSet getDrillUserConfigurableLogicalRules(OptimizerRulesContext optimizerRulesContext) {
    final PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

    // This list is used to store rules that can be turned on an off
    // by user facing planning options
    final Builder<RelOptRule> userConfigurableRules = ImmutableSet.<RelOptRule>builder();

    if (ps.isConstantFoldingEnabled()) {
      // TODO - DRILL-2218
      userConfigurableRules.add(ReduceExpressionsRule.PROJECT_INSTANCE);
      userConfigurableRules.add(DrillReduceExpressionsRule.FILTER_INSTANCE_DRILL);
      userConfigurableRules.add(DrillReduceExpressionsRule.CALC_INSTANCE_DRILL);
    }

    return RuleSets.ofList(userConfigurableRules.build());
  }

  /*
   * These basic rules don't require any context, so singleton instances can be used.
   * These are merged with per-query rules in getDrillBasicRules() below.
   */
  final static ImmutableSet<RelOptRule> staticRuleSet = ImmutableSet.<RelOptRule> builder().add(
      // Add support for Distinct Union (by using Union-All followed by Distinct)
      UnionToDistinctRule.INSTANCE,

      // Add support for WHERE style joins.
      DrillFilterJoinRules.DRILL_FILTER_ON_JOIN,
      DrillFilterJoinRules.DRILL_JOIN,
      JoinPushExpressionsRule.INSTANCE,
      // End support for WHERE style joins.

      /*
       Filter push-down related rules
       */
      DrillPushFilterPastProjectRule.INSTANCE,
      // Due to infinite loop in planning (DRILL-3257), temporarily disable this rule
      //FilterSetOpTransposeRule.INSTANCE,
      DrillFilterAggregateTransposeRule.INSTANCE,

      FilterMergeRule.INSTANCE,
      AggregateRemoveRule.INSTANCE,
      ProjectRemoveRule.INSTANCE,
      SortRemoveRule.INSTANCE,

      AggregateExpandDistinctAggregatesRule.JOIN,
      DrillReduceAggregatesRule.INSTANCE,

      /*
       Projection push-down related rules
       */
      DrillPushProjectPastFilterRule.INSTANCE,
      DrillPushProjectPastJoinRule.INSTANCE,
      // Due to infinite loop in planning (DRILL-3257), temporarily disable this rule
      //DrillProjectSetOpTransposeRule.INSTANCE,
      ProjectWindowTransposeRule.INSTANCE,
      DrillPushProjIntoScan.INSTANCE,

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
      ).build();

  /**
   * Get an immutable list of rules that will always be used when running
   * logical planning.
   *
   * This cannot be a static singleton because some of the rules need to
   * reference state owned by the current query (including its allocator).
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
  static RuleSet getDrillBasicRules(OptimizerRulesContext optimizerRulesContext) {
    /*
     * We have to create another copy of the ruleset with the context dependent elements;
     * this cannot be reused across queries.
     */
    final ImmutableSet<RelOptRule> basicRules = ImmutableSet.<RelOptRule>builder()
        .addAll(staticRuleSet)
        .add(
            DrillMergeProjectRule.getInstance(true, RelFactories.DEFAULT_PROJECT_FACTORY,
                optimizerRulesContext.getFunctionRegistry())
            )
        .build();

    return RuleSets.ofList(basicRules);
  }

  /**
   *   Get an immutable list of partition pruning rules that will be used in logical planning.
   */
  static RuleSet getPruneScanRules(OptimizerRulesContext optimizerRulesContext) {
    final ImmutableSet<RelOptRule> pruneRules = ImmutableSet.<RelOptRule>builder()
        .add(
            PruneScanRule.getDirFilterOnProject(optimizerRulesContext),
            PruneScanRule.getDirFilterOnScan(optimizerRulesContext),
            ParquetPruneScanRule.getFilterOnProjectParquet(optimizerRulesContext),
            ParquetPruneScanRule.getFilterOnScanParquet(optimizerRulesContext),
            DrillPushLimitToScanRule.LIMIT_ON_SCAN,
            DrillPushLimitToScanRule.LIMIT_ON_PROJECT
        )
        .build();

    return RuleSets.ofList(pruneRules);
  }

  /**
   *  Get an immutable list of directory-based partition pruing rules that will be used in Calcite logical planning.
   * @param optimizerRulesContext
   * @return
   */
  static RuleSet getDirPruneScanRules(OptimizerRulesContext optimizerRulesContext) {
    final ImmutableSet<RelOptRule> pruneRules = ImmutableSet.<RelOptRule>builder()
        .add(
            PruneScanRule.getDirFilterOnProject(optimizerRulesContext),
            PruneScanRule.getDirFilterOnScan(optimizerRulesContext)
        )
        .build();

    return RuleSets.ofList(pruneRules);

  }

  // Ruleset for join permutation, used only in VolcanoPlanner.
  static RuleSet getJoinPermRules(OptimizerRulesContext optimizerRulesContext) {
    return RuleSets.ofList(ImmutableSet.<RelOptRule> builder().add( //
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT
        ).build());
  }

  static final RuleSet DRILL_PHYSICAL_DISK = RuleSets.ofList(ImmutableSet.of(
      ProjectPrule.INSTANCE
    ));

  static final RuleSet getPhysicalRules(OptimizerRulesContext optimizerRulesContext) {
    final List<RelOptRule> ruleList = new ArrayList<RelOptRule>();
    final PlannerSettings ps = optimizerRulesContext.getPlannerSettings();

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
    ruleList.add(LimitUnionExchangeTransposeRule.INSTANCE);
    ruleList.add(UnionAllPrule.INSTANCE);
    ruleList.add(ValuesPrule.INSTANCE);
    ruleList.add(DirectScanPrule.INSTANCE);

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

    return RuleSets.ofList(ImmutableSet.copyOf(ruleList));
  }

  static RuleSet create(ImmutableSet<RelOptRule> rules) {
    return RuleSets.ofList(rules);
  }

  static RuleSet mergedRuleSets(RuleSet... ruleSets) {
    final Builder<RelOptRule> relOptRuleSetBuilder = ImmutableSet.builder();
    for (final RuleSet ruleSet : ruleSets) {
      for (final RelOptRule relOptRule : ruleSet) {
        relOptRuleSetBuilder.add(relOptRule);
      }
    }
    return RuleSets.ofList(relOptRuleSetBuilder.build());
  }

}
