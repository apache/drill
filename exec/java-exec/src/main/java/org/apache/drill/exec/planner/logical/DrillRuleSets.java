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

import net.hydromatic.optiq.tools.RuleSet;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.partition.PruneScanRule;
import org.apache.drill.exec.planner.physical.ConvertCountToDirectScan;
import org.apache.drill.exec.planner.physical.FilterPrule;
import org.apache.drill.exec.planner.physical.HashAggPrule;
import org.apache.drill.exec.planner.physical.HashJoinPrule;
import org.apache.drill.exec.planner.physical.LimitPrule;
import org.apache.drill.exec.planner.physical.MergeJoinPrule;
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
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.rules.MergeFilterRule;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.ReduceExpressionsRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.RemoveDistinctRule;
import org.eigenbase.rel.rules.RemoveSortRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.volcano.AbstractConverter.ExpandConversionRule;

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
   * @param queryContext - used to get the list of planner settings, other rules may
   *                     also in the future need to get other query state from this,
   *                     such as the available list of UDFs (as is used by the
   *                     DrillMergeProjectRule created in getDrillBasicRules())
   * @return - a list of rules that have been filtered to leave out
   *         rules that have been turned off by system or session settings
   */
  public static RuleSet getDrillUserConfigurableLogicalRules(QueryContext queryContext) {
    PlannerSettings ps = queryContext.getPlannerSettings();

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
   * @param context - shared state used during planning, currently used here
   *                to gain access to the fucntion registry described above.
   * @return - a RuleSet containing the logical rules that will always
   *           be used.
   */
  public static RuleSet getDrillBasicRules(QueryContext context) {
    if (DRILL_BASIC_RULES == null) {

      DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.<RelOptRule> builder().add( //
        // Add support for WHERE style joins.
//      PushFilterPastProjectRule.INSTANCE, // Replaced by DrillPushFilterPastProjectRule
      DrillPushFilterPastProjectRule.INSTANCE,
      PushFilterPastJoinRule.FILTER_ON_JOIN,
      PushFilterPastJoinRule.JOIN,
      PushJoinThroughJoinRule.RIGHT,
      PushJoinThroughJoinRule.LEFT,
      // End support for WHERE style joins.

      //Add back rules
      DrillMergeFilterRule.INSTANCE,
      ExpandConversionRule.INSTANCE,
//      SwapJoinRule.INSTANCE,
      RemoveDistinctRule.INSTANCE,
//      UnionToDistinctRule.INSTANCE,
      RemoveTrivialProjectRule.INSTANCE,
//      RemoveTrivialCalcRule.INSTANCE,
      RemoveSortRule.INSTANCE,

//      TableAccessRule.INSTANCE, //
      //MergeProjectRule.INSTANCE, //
      DrillMergeProjectRule.getInstance(true, RelFactories.DEFAULT_PROJECT_FACTORY, context.getFunctionRegistry()),
      RemoveDistinctAggregateRule.INSTANCE, //
      // ReduceAggregatesRule.INSTANCE, // replaced by DrillReduceAggregatesRule
      DrillValuesRule.INSTANCE,

      /*
      Projection push-down related rules
      */
//      PushProjectPastFilterRule.INSTANCE,
      DrillPushProjectPastFilterRule.INSTANCE,
//      PushProjectPastJoinRule.INSTANCE,
      DrillPushProjectPastJoinRule.INSTANCE,

//      SwapJoinRule.INSTANCE, //
//      PushJoinThroughJoinRule.RIGHT, //
//      PushJoinThroughJoinRule.LEFT, //
//      PushSortPastProjectRule.INSTANCE, //

      DrillPushProjIntoScan.INSTANCE,

//      DrillPushPartitionFilterIntoScan.FILTER_ON_PROJECT,
//      DrillPushPartitionFilterIntoScan.FILTER_ON_SCAN,
      PruneScanRule.getFilterOnProject(context),
      PruneScanRule.getFilterOnScan(context),

      ////////////////////////////////
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillWindowRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionRule.INSTANCE,

      DrillReduceAggregatesRule.INSTANCE
      )
      .build());
    }

    return DRILL_BASIC_RULES;
  }

  public static final RuleSet DRILL_PHYSICAL_DISK = new DrillRuleSet(ImmutableSet.of( //
      ProjectPrule.INSTANCE

    ));

  public static final RuleSet getPhysicalRules(QueryContext qcontext) {
    List<RelOptRule> ruleList = new ArrayList<RelOptRule>();

    PlannerSettings ps = qcontext.getPlannerSettings();

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

    // ruleList.add(UnionDistinctPrule.INSTANCE);

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
