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
package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public abstract class DrillReduceExpressionsRule
  extends ReduceExpressionsRule<ReduceExpressionsRule.Config> {

  public static final DrillReduceFilterRule FILTER_INSTANCE_DRILL =
      new DrillReduceFilterRule();

  public static final DrillReduceCalcRule CALC_INSTANCE_DRILL =
      new DrillReduceCalcRule();

  public static final DrillReduceProjectRule PROJECT_INSTANCE_DRILL =
      new DrillReduceProjectRule();

  protected DrillReduceExpressionsRule(Config config) {
    super(config);
  }

  private static class DrillReduceFilterRule extends FilterReduceExpressionsRule {

    DrillReduceFilterRule() {
      super(FilterReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Filter.class)
        .withMatchNullability(false)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(FilterReduceExpressionsRuleConfig.class));
    }

    /**
     * Drills schema flexibility requires us to override the default behavior of calcite
     * to produce an EmptyRel in the case of a constant false filter. We need to propagate
     * schema at runtime, so we cannot just produce a simple operator at planning time to
     * expose the planning time known schema. Instead we have to insert a limit 0.
     */
    @Override
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Filter filter) {
      return createEmptyEmptyRelHelper(filter);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final List<RexNode> expList =
        Lists.newArrayList(filter.getCondition());
      RexNode newConditionExp;
      boolean reduced;
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(filter.getInput());
      if (reduceExpressionsNoSimplify(filter, expList, predicates, true,
        config.treatDynamicCallsAsConstant())) {
        assert expList.size() == 1;
        newConditionExp = expList.get(0);
        reduced = true;
      } else {
        // No reduction, but let's still test the original
        // predicate to see if it was already a constant,
        // in which case we don't need any runtime decision
        // about filtering.
        newConditionExp = filter.getCondition();
        reduced = false;
      }

      // Even if no reduction, let's still test the original
      // predicate to see if it was already a constant,
      // in which case we don't need any runtime decision
      // about filtering.
      if (newConditionExp.isAlwaysTrue()) {
        call.transformTo(
          filter.getInput());
      } else if (newConditionExp instanceof RexLiteral
        || RexUtil.isNullLiteral(newConditionExp, true)) {
        call.transformTo(createEmptyRelOrEquivalent(call, filter));
      } else if (reduced) {
        call.transformTo(call.builder()
          .push(filter.getInput())
          .filter(newConditionExp).build());
      } else {
        if (newConditionExp instanceof RexCall) {
          boolean reverse = newConditionExp.getKind() == SqlKind.NOT;
          if (reverse) {
            newConditionExp = ((RexCall) newConditionExp).getOperands().get(0);
          }
          reduceNotNullableFilter(call, filter, newConditionExp, reverse);
        }
        return;
      }

      // New plan is absolutely better than old plan.
      call.getPlanner().prune(filter);
    }

    private void reduceNotNullableFilter(
      RelOptRuleCall call,
      Filter filter,
      RexNode rexNode,
      boolean reverse) {
      // If the expression is a IS [NOT] NULL on a non-nullable
      // column, then we can either remove the filter or replace
      // it with an Empty.
      boolean alwaysTrue;
      switch (rexNode.getKind()) {
        case IS_NULL:
        case IS_UNKNOWN:
          alwaysTrue = false;
          break;
        case IS_NOT_NULL:
          alwaysTrue = true;
          break;
        default:
          return;
      }
      if (reverse) {
        alwaysTrue = !alwaysTrue;
      }
      RexNode operand = ((RexCall) rexNode).getOperands().get(0);
      if (operand instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) operand;
        if (!inputRef.getType().isNullable()) {
          if (alwaysTrue) {
            call.transformTo(filter.getInput());
          } else {
            call.transformTo(createEmptyRelOrEquivalent(call, filter));
          }
          // New plan is absolutely better than old plan.
          call.getPlanner().prune(filter);
        }
      }
    }
  }

  private static class DrillReduceCalcRule extends CalcReduceExpressionsRule {

    DrillReduceCalcRule() {
      super(CalcReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Calc.class)
        .withMatchNullability(true)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(CalcReduceExpressionsRuleConfig.class));
    }

    /**
     * Drills schema flexibility requires us to override the default behavior of calcite
     * to produce an EmptyRel in the case of a constant false filter. We need to propagate
     * schema at runtime, so we cannot just produce a simple operator at planning time to
     * expose the planning time known schema. Instead we have to insert a limit 0.
     */
    @Override
    protected RelNode createEmptyRelOrEquivalent(RelOptRuleCall call, Calc input) {
      return createEmptyEmptyRelHelper(input);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Calc calc = call.rel(0);
      RexProgram program = calc.getProgram();
      final List<RexNode> exprList = program.getExprList();

      // Form a list of expressions with sub-expressions fully expanded.
      final List<RexNode> expandedExprList = new ArrayList<>();
      final RexShuttle shuttle =
        new RexShuttle() {
          @Override
          public RexNode visitLocalRef(RexLocalRef localRef) {
            return expandedExprList.get(localRef.getIndex());
          }
        };
      for (RexNode expr : exprList) {
        expandedExprList.add(expr.accept(shuttle));
      }
      final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
      if (reduceExpressionsNoSimplify(calc, expandedExprList, predicates, false,
        config.treatDynamicCallsAsConstant())) {
        final RexProgramBuilder builder =
          new RexProgramBuilder(
            calc.getInput().getRowType(),
            calc.getCluster().getRexBuilder());
        final List<RexLocalRef> list = new ArrayList<>();
        for (RexNode expr : expandedExprList) {
          list.add(builder.registerInput(expr));
        }
        if (program.getCondition() != null) {
          final int conditionIndex =
            program.getCondition().getIndex();
          final RexNode newConditionExp =
            expandedExprList.get(conditionIndex);
          if (newConditionExp.isAlwaysTrue()) {
            // condition is always TRUE - drop it.
          } else if (newConditionExp instanceof RexLiteral
            || RexUtil.isNullLiteral(newConditionExp, true)) {
            // condition is always NULL or FALSE - replace calc
            // with empty.
            call.transformTo(createEmptyRelOrEquivalent(call, calc));
            return;
          } else {
            builder.addCondition(list.get(conditionIndex));
          }
        }
        int k = 0;
        for (RexLocalRef projectExpr : program.getProjectList()) {
          final int index = projectExpr.getIndex();
          builder.addProject(
            list.get(index).getIndex(),
            program.getOutputRowType().getFieldNames().get(k++));
        }
        call.transformTo(
          calc.copy(calc.getTraitSet(), calc.getInput(), builder.getProgram()));

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(calc);
      }
    }
  }

  private static class DrillReduceProjectRule extends ProjectReduceExpressionsRule {

    DrillReduceProjectRule() {
      super(ProjectReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Project.class)
        .withMatchNullability(true)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(ProjectReduceExpressionsRuleConfig.class));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates =
        mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList =
        Lists.newArrayList(project.getProjects());
      if (reduceExpressionsNoSimplify(project, expList, predicates, false,
        config.treatDynamicCallsAsConstant())) {
        assert !project.getProjects().equals(expList)
          : "Reduced expressions should be different from original expressions";
        call.transformTo(
          call.builder()
            .push(project.getInput())
            .project(expList, project.getRowType().getFieldNames())
            .build());

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(project);
      }
    }
  }

  protected static boolean reduceExpressionsNoSimplify(RelNode rel, List<RexNode> expList,
    RelOptPredicateList predicates, boolean unknownAsFalse, boolean treatDynamicCallsAsConstant) {
    RelOptCluster cluster = rel.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexExecutor executor =
      Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    RexSimplify simplify =
      new RexSimplify(rexBuilder, predicates, executor);

    // Simplify predicates in place
    RexUnknownAs unknownAs = RexUnknownAs.falseIf(unknownAsFalse);

    return ReduceExpressionsRule.reduceExpressionsInternal(rel, simplify, unknownAs,
      expList, predicates, treatDynamicCallsAsConstant);
  }

  private static RelNode createEmptyEmptyRelHelper(SingleRel input) {
    return LogicalSort.create(input.getInput(), RelCollations.EMPTY,
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
  }
}
