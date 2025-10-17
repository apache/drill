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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.math.BigDecimal;

public class ReduceAndSimplifyExpressionsRules {

  public static final ReduceAndSimplifyFilterRule FILTER_INSTANCE_DRILL =
      new ReduceAndSimplifyFilterRule();

  public static final ReduceAndSimplifyCalcRule CALC_INSTANCE_DRILL =
      new ReduceAndSimplifyCalcRule();

  public static final ReduceAndSimplifyProjectRule PROJECT_INSTANCE_DRILL =
      new ReduceAndSimplifyProjectRule();

  private static class ReduceAndSimplifyFilterRule extends ReduceExpressionsRule.FilterReduceExpressionsRule {

    ReduceAndSimplifyFilterRule() {
      super(FilterReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Filter.class)
        .withMatchNullability(false)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .withDescription("ReduceAndSimplifyFilterRule")
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

      // DRILL: Skip simplification for expressions with large OR chains
      // Calcite 1.37's RexSimplify has exponential complexity with large OR expressions
      // (created from IN clauses with expressions like: WHERE x IN (1, 1+1, 1, ...))
      int orCount = countOrNodes(filter.getCondition());
      if (orCount > 10) {
        return; // Skip this rule for complex OR expressions
      }

      try {
        super.onMatch(call);
      } catch (ClassCastException | IllegalArgumentException e) {
        // noop - Calcite 1.35+ may throw IllegalArgumentException for type mismatches
      } catch (RuntimeException e) {
        // Calcite 1.35+ wraps IllegalArgumentException in RuntimeException during transformTo
        if (e.getCause() instanceof IllegalArgumentException) {
          // noop - ignore type mismatch errors
        } else {
          throw e;
        }
      }
    }
  }

  private static class ReduceAndSimplifyCalcRule extends ReduceExpressionsRule.CalcReduceExpressionsRule {

    ReduceAndSimplifyCalcRule() {
      super(CalcReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Calc.class)
        .withMatchNullability(true)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .withDescription("ReduceAndSimplifyCalcRule")
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
      try {
        super.onMatch(call);
      } catch (ClassCastException | IllegalArgumentException e) {
        // noop - Calcite 1.35+ may throw IllegalArgumentException for type mismatches
      } catch (RuntimeException e) {
        // Calcite 1.35+ wraps IllegalArgumentException in RuntimeException during transformTo
        if (e.getCause() instanceof IllegalArgumentException) {
          // noop - ignore type mismatch errors
        } else {
          throw e;
        }
      }
    }
  }

  private static class ReduceAndSimplifyProjectRule extends ReduceExpressionsRule.ProjectReduceExpressionsRule {

    ReduceAndSimplifyProjectRule() {
      super(ProjectReduceExpressionsRuleConfig.DEFAULT
        .withOperandFor(Project.class)
        .withMatchNullability(true)
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .withDescription("ReduceAndSimplifyProjectRule")
        .as(ProjectReduceExpressionsRuleConfig.class));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      try {
        super.onMatch(call);
      } catch (ClassCastException | IllegalArgumentException e) {
        // noop - Calcite 1.35+ may throw IllegalArgumentException for type mismatches
      } catch (RuntimeException e) {
        // Calcite 1.35+ wraps IllegalArgumentException in RuntimeException during transformTo
        if (e.getCause() instanceof IllegalArgumentException) {
          // noop - ignore type mismatch errors
        } else {
          throw e;
        }
      }
    }
  }

  private static RelNode createEmptyEmptyRelHelper(SingleRel input) {
    return LogicalSort.create(input.getInput(), RelCollations.EMPTY,
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)),
        input.getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0)));
  }

  /**
   * Count the number of OR nodes in a RexNode tree
   * Large OR chains (from IN clauses) cause exponential planning time in Calcite 1.37
   */
  private static int countOrNodes(RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      int count = call.getKind() == SqlKind.OR ? 1 : 0;
      for (RexNode operand : call.getOperands()) {
        count += countOrNodes(operand);
      }
      return count;
    }
    return 0;
  }
}
