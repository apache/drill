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
package org.apache.drill.exec.planner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterRemoveIsNotDistinctFromRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.drill.exec.planner.logical.DrillConditions;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * Contains rule instances which use custom RelBuilder.
 */
public interface RuleInstance {

  RelOptRule UNION_TO_DISTINCT_RULE =
    UnionToDistinctRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule SEMI_JOIN_PROJECT_RULE = new SemiJoinRule.ProjectToSemiJoinRule(
    SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .withDescription("DrillSemiJoinRule:project")
      .as(SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.class)) {

    public boolean matches(RelOptRuleCall call) {
      Preconditions.checkArgument(call.rel(1) instanceof Join);
      Join join = call.rel(1);
      return !(join.getCondition().isAlwaysTrue() || join.getCondition().isAlwaysFalse());
    }
  };

  SemiJoinRule JOIN_TO_SEMI_JOIN_RULE = new SemiJoinRule.JoinToSemiJoinRule(
    SemiJoinRule.JoinToSemiJoinRule.JoinToSemiJoinRuleConfig.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .withDescription("DrillJoinToSemiJoinRule")
      .as(SemiJoinRule.JoinToSemiJoinRule.JoinToSemiJoinRuleConfig.class)) {
    public boolean matches(RelOptRuleCall call) {
      Join join = call.rel(0);
      return !(join.getCondition().isAlwaysTrue() || join.getCondition().isAlwaysFalse());
    }
  };

  RelOptRule JOIN_PUSH_EXPRESSIONS_RULE =
    JoinPushExpressionsRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule FILTER_MERGE_RULE =
    FilterMergeRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule DRILL_FILTER_MERGE_RULE =
    FilterMergeRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelBuilder.proto(DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY))
      .toRule();

  RelOptRule FILTER_CORRELATE_RULE =
    FilterCorrelateRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule AGGREGATE_REMOVE_RULE =
    AggregateRemoveRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule AGGREGATE_EXPAND_DISTINCT_AGGREGATES_RULE =
    AggregateExpandDistinctAggregatesRule.Config.JOIN
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  /**
   * Instance of the rule that works on logical joins only, and pushes to the
   * right.
   */
  RelOptRule JOIN_PUSH_THROUGH_JOIN_RULE_RIGHT =
    JoinPushThroughJoinRule.Config.RIGHT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  /**
   * Instance of the rule that works on logical joins only, and pushes to the
   * left.
   */
  RelOptRule JOIN_PUSH_THROUGH_JOIN_RULE_LEFT =
    JoinPushThroughJoinRule.Config.LEFT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule CALC_INSTANCE =
    ReduceExpressionsRule.CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule FILTER_SET_OP_TRANSPOSE_RULE =
    FilterSetOpTransposeRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule PROJECT_SET_OP_TRANSPOSE_RULE =
    ProjectSetOpTransposeRule.Config.DEFAULT
      .withPreserveExprCondition(DrillConditions.PRESERVE_ITEM)
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule PROJECT_REMOVE_RULE =
    ProjectRemoveRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW_RULE =
    ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule SORT_REMOVE_RULE =
    SortRemoveRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule PROJECT_WINDOW_TRANSPOSE_RULE =
    ProjectWindowTransposeRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule EXPAND_CONVERSION_RULE =
    AbstractConverter.ExpandConversionRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  /**
   * Instance of the rule that infers predicates from on a
   * {@link org.apache.calcite.rel.core.Join} and creates
   * {@link org.apache.calcite.rel.core.Filter}s if those predicates can be pushed
   * to its inputs.
   */
  RelOptRule DRILL_JOIN_PUSH_TRANSITIVE_PREDICATES_RULE =
    JoinPushTransitivePredicatesRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelBuilder.proto(
        DrillRelFactories.DRILL_LOGICAL_JOIN_FACTORY, DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY))
      .toRule();

  RelOptRule REMOVE_IS_NOT_DISTINCT_FROM_RULE =
    FilterRemoveIsNotDistinctFromRule.Config.DEFAULT
      .withRelBuilderFactory(DrillRelBuilder.proto(DrillRelFactories.DRILL_LOGICAL_FILTER_FACTORY))
      .toRule();

  RelOptRule SUB_QUERY_FILTER_REMOVE_RULE =
      SubQueryRemoveRule.Config.FILTER
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .toRule();

  RelOptRule SUB_QUERY_PROJECT_REMOVE_RULE =
    SubQueryRemoveRule.Config.PROJECT
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();

  RelOptRule SUB_QUERY_JOIN_REMOVE_RULE =
    SubQueryRemoveRule.Config.JOIN
      .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
      .toRule();
}
