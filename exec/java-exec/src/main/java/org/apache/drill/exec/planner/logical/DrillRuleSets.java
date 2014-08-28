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
import org.apache.drill.exec.planner.physical.UnionAllPrule;
import org.apache.drill.exec.planner.physical.WriterPrule;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushProjectPastFilterRule;
import org.eigenbase.rel.rules.PushProjectPastJoinRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
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

  public static RuleSet getDrillBasicRules(QueryContext context) {
    if (DRILL_BASIC_RULES == null) {
    DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.of( //
        // Add support for WHERE style joins.
      PushFilterPastProjectRule.INSTANCE,
      PushFilterPastJoinRule.FILTER_ON_JOIN,
      PushFilterPastJoinRule.JOIN,
      PushJoinThroughJoinRule.RIGHT,
      PushJoinThroughJoinRule.LEFT,
      // End support for WHERE style joins.

      //Add back rules

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
      ReduceAggregatesRule.INSTANCE, //
      PushProjectPastJoinRule.INSTANCE,
//      PushProjectPastFilterRule.INSTANCE,
      DrillPushProjectPastFilterRule.INSTANCE,
//      SwapJoinRule.INSTANCE, //
//      PushJoinThroughJoinRule.RIGHT, //
//      PushJoinThroughJoinRule.LEFT, //
//      PushSortPastProjectRule.INSTANCE, //

      DrillPushProjIntoScan.INSTANCE,

      ////////////////////////////////
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionRule.INSTANCE
      ));
    }
    return DRILL_BASIC_RULES;
  }

  /* 
  public static final RuleSet DRILL_PHYSICAL_MEM = new DrillRuleSet(ImmutableSet.of( //
//      DrillScanRule.INSTANCE,
//      DrillFilterRule.INSTANCE,
//      DrillProjectRule.INSTANCE,
//      DrillAggregateRule.INSTANCE,
//
//      DrillLimitRule.INSTANCE,
//      DrillSortRule.INSTANCE,
//      DrillJoinRule.INSTANCE,
//      DrillUnionRule.INSTANCE,

      ConvertCountToDirectScan.AGG_ON_PROJ_ON_SCAN,
      ConvertCountToDirectScan.AGG_ON_SCAN,

      SortConvertPrule.INSTANCE,
      SortPrule.INSTANCE,
      ProjectPrule.INSTANCE,
      ScanPrule.INSTANCE,
      ScreenPrule.INSTANCE,
      ExpandConversionRule.INSTANCE,
      StreamAggPrule.INSTANCE,
      HashAggPrule.INSTANCE,
      MergeJoinPrule.INSTANCE,
      HashJoinPrule.INSTANCE,
      FilterPrule.INSTANCE,
      LimitPrule.INSTANCE,
      WriterPrule.INSTANCE,
      PushLimitToTopN.INSTANCE

//    ExpandConversionRule.INSTANCE,
//    SwapJoinRule.INSTANCE,
//    RemoveDistinctRule.INSTANCE,
//    UnionToDistinctRule.INSTANCE,
//    RemoveTrivialProjectRule.INSTANCE,
//    RemoveTrivialCalcRule.INSTANCE,
//    RemoveSortRule.INSTANCE,
//
//    TableAccessRule.INSTANCE, //
//    MergeProjectRule.INSTANCE, //
//    PushFilterPastProjectRule.INSTANCE, //
//    PushFilterPastJoinRule.FILTER_ON_JOIN, //
//    RemoveDistinctAggregateRule.INSTANCE, //
//    ReduceAggregatesRule.INSTANCE, //
//    SwapJoinRule.INSTANCE, //
//    PushJoinThroughJoinRule.RIGHT, //
//    PushJoinThroughJoinRule.LEFT, //
//    PushSortPastProjectRule.INSTANCE, //
    ));
*/
  public static final RuleSet DRILL_PHYSICAL_DISK = new DrillRuleSet(ImmutableSet.of( //
      ProjectPrule.INSTANCE

    ));

  public static final RuleSet getPhysicalRules(QueryContext qcontext) {
    List<RelOptRule> ruleList = new ArrayList<RelOptRule>(); 

    
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
    ruleList.add(PushLimitToTopN.INSTANCE);
    ruleList.add(UnionAllPrule.INSTANCE);
    // ruleList.add(UnionDistinctPrule.INSTANCE);
    
    PlannerSettings ps = qcontext.getPlannerSettings();
    
    if (ps.isHashAggEnabled()) {
      ruleList.add(HashAggPrule.INSTANCE);        
    }
    
    if (ps.isStreamAggEnabled()) {
      ruleList.add(StreamAggPrule.INSTANCE);        
    }
    
    if (ps.isHashJoinEnabled()) {
      ruleList.add(HashJoinPrule.INSTANCE);        
    }
    
    if (ps.isMergeJoinEnabled()) {
      ruleList.add(MergeJoinPrule.INSTANCE);        
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
