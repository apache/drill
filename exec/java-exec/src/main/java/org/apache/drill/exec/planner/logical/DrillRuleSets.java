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

import java.util.Iterator;

import net.hydromatic.optiq.tools.RuleSet;

import org.apache.drill.exec.planner.physical.ProjectPrule;
import org.apache.drill.exec.planner.physical.ScanPrule;
import org.eigenbase.rel.rules.MergeProjectRule;
import org.eigenbase.rel.rules.PushFilterPastJoinRule;
import org.eigenbase.rel.rules.PushFilterPastProjectRule;
import org.eigenbase.rel.rules.PushJoinThroughJoinRule;
import org.eigenbase.rel.rules.PushSortPastProjectRule;
import org.eigenbase.rel.rules.ReduceAggregatesRule;
import org.eigenbase.rel.rules.RemoveDistinctAggregateRule;
import org.eigenbase.rel.rules.RemoveDistinctRule;
import org.eigenbase.rel.rules.RemoveSortRule;
import org.eigenbase.rel.rules.RemoveTrivialCalcRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rel.rules.SwapJoinRule;
import org.eigenbase.rel.rules.TableAccessRule;
import org.eigenbase.rel.rules.UnionToDistinctRule;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.volcano.AbstractConverter.ExpandConversionRule;

import com.google.common.collect.ImmutableSet;

public class DrillRuleSets {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillRuleSets.class);

  public static final RuleSet DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.of( //
      
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionRule.INSTANCE
      ));
  
  public static final RuleSet DRILL_PHYSICAL_MEM = new DrillRuleSet(ImmutableSet.of( //
      DrillScanRule.INSTANCE,
      DrillFilterRule.INSTANCE,
      DrillProjectRule.INSTANCE,
      DrillAggregateRule.INSTANCE,

      DrillLimitRule.INSTANCE,
      DrillSortRule.INSTANCE,
      DrillJoinRule.INSTANCE,
      DrillUnionRule.INSTANCE,
      ProjectPrule.INSTANCE,
      ScanPrule.INSTANCE

//    ExpandConversionRule.instance,
//    SwapJoinRule.instance,
//    RemoveDistinctRule.instance,
//    UnionToDistinctRule.instance,
//    RemoveTrivialProjectRule.instance,
//    RemoveTrivialCalcRule.instance,
//    RemoveSortRule.INSTANCE,
//
//    TableAccessRule.instance, //
//    MergeProjectRule.instance, //
//    PushFilterPastProjectRule.instance, //
//    PushFilterPastJoinRule.FILTER_ON_JOIN, //
//    RemoveDistinctAggregateRule.instance, //
//    ReduceAggregatesRule.instance, //
//    SwapJoinRule.instance, //
//    PushJoinThroughJoinRule.RIGHT, //
//    PushJoinThroughJoinRule.LEFT, //
//    PushSortPastProjectRule.INSTANCE, //      
    ));
  
  public static final RuleSet DRILL_PHYSICAL_DISK = new DrillRuleSet(ImmutableSet.of( //
      ProjectPrule.INSTANCE
  
    ));
  
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
