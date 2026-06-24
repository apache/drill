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
package org.apache.drill.exec.store.phoenix.rules;

import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.util.Optionality;
import org.apache.drill.exec.planner.logical.DrillRelFactories;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Custom aggregate rule for Phoenix that handles DrillSqlAggOperator which uses
 * SqlKind.OTHER_FUNCTION instead of the specific aggregate SqlKind.
 */
public class PhoenixAggregateRule extends ConverterRule {

  private static final Map<String, SqlKind> DRILL_AGG_TO_SQL_KIND = new HashMap<>();
  static {
    DRILL_AGG_TO_SQL_KIND.put("COUNT", SqlKind.COUNT);
    DRILL_AGG_TO_SQL_KIND.put("SUM", SqlKind.SUM);
    DRILL_AGG_TO_SQL_KIND.put("MIN", SqlKind.MIN);
    DRILL_AGG_TO_SQL_KIND.put("MAX", SqlKind.MAX);
    DRILL_AGG_TO_SQL_KIND.put("AVG", SqlKind.AVG);
    DRILL_AGG_TO_SQL_KIND.put("ANY_VALUE", SqlKind.ANY_VALUE);
  }

  /**
   * Wrapper for DrillSqlAggOperator that overrides getKind() to return the correct SqlKind
   * based on the function name instead of OTHER_FUNCTION.
   */
  private static class DrillSqlAggOperatorWrapper extends org.apache.calcite.sql.SqlAggFunction {
    private final DrillSqlAggOperator wrapped;
    private final SqlKind kind;
    private final boolean isCount;

    public DrillSqlAggOperatorWrapper(DrillSqlAggOperator wrapped, SqlKind kind) {
      super(wrapped.getName(), wrapped.getSqlIdentifier(), kind,
          wrapped.getReturnTypeInference(), wrapped.getOperandTypeInference(),
          wrapped.getOperandTypeChecker(), wrapped.getFunctionType(),
          wrapped.requiresOrder(), wrapped.requiresOver(), Optionality.FORBIDDEN);
      this.wrapped = wrapped;
      this.kind = kind;
      this.isCount = kind == SqlKind.COUNT;
    }

    @Override
    public SqlKind getKind() {
      return kind;
    }

    @Override
    public SqlSyntax getSyntax() {
      // COUNT with zero arguments should use FUNCTION_STAR syntax for COUNT(*)
      if (isCount) {
        return SqlSyntax.FUNCTION_STAR;
      }
      return super.getSyntax();
    }
  }

  /**
   * Transform aggregate calls that use DrillSqlAggOperator (which has SqlKind.OTHER_FUNCTION)
   * to use a wrapped version with the correct SqlKind based on the function name.
   */
  private static List<AggregateCall> transformDrillAggCalls(List<AggregateCall> aggCalls, Aggregate agg) {
    List<AggregateCall> transformed = new ArrayList<>();
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.getAggregation() instanceof DrillSqlAggOperator) {
        String funcName = aggCall.getAggregation().getName().toUpperCase();
        SqlKind kind = DRILL_AGG_TO_SQL_KIND.get(funcName);
        if (kind != null) {
          // Wrap the DrillSqlAggOperator with the correct SqlKind
          DrillSqlAggOperatorWrapper wrappedOp = new DrillSqlAggOperatorWrapper(
              (DrillSqlAggOperator) aggCall.getAggregation(), kind);

          // Create a new AggregateCall with the wrapped operator
          AggregateCall newCall = AggregateCall.create(
              wrappedOp,
              aggCall.isDistinct(),
              aggCall.isApproximate(),
              aggCall.ignoreNulls(),
              aggCall.getArgList(),
              aggCall.filterArg,
              aggCall.distinctKeys,
              aggCall.collation,
              agg.getGroupCount(),
              agg.getInput(),
              aggCall.type,
              aggCall.name
          );
          transformed.add(newCall);
        } else {
          transformed.add(aggCall);
        }
      } else {
        transformed.add(aggCall);
      }
    }
    return transformed;
  }

  /**
   * Create a custom JdbcAggregateRule for Convention.NONE
   */
  public static PhoenixAggregateRule create(RelTrait in, PhoenixConvention out) {
    return new PhoenixAggregateRule(in, out);
  }

  private PhoenixAggregateRule(RelTrait in, PhoenixConvention out) {
    super((ConverterRule.Config) Config.INSTANCE
        .withConversion(LogicalAggregate.class, (Predicate<RelNode>) r -> true,
            in, out, "PhoenixAggregateRule:" + in.toString())
        .withRelBuilderFactory(DrillRelFactories.LOGICAL_BUILDER)
        .as(Config.class));
  }

  @Override
  public RelNode convert(RelNode rel) {
    Aggregate agg = (Aggregate) rel;
    RelTraitSet traitSet = agg.getTraitSet().replace(out);

    // Transform DrillSqlAggOperator calls to have correct SqlKind
    List<AggregateCall> transformedCalls = transformDrillAggCalls(agg.getAggCallList(), agg);

    try {
      return new JdbcRules.JdbcAggregate(
          agg.getCluster(),
          traitSet,
          convert(agg.getInput(), traitSet.simplify()),
          agg.getGroupSet(),
          agg.getGroupSets(),
          transformedCalls
      );
    } catch (InvalidRelException e) {
      return null;
    }
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    // Only single group sets are supported
    if (agg.getGroupSets().size() != 1) {
      return false;
    }
    return super.matches(call);
  }
}
