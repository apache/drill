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
package org.apache.drill.exec.expr.stat;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.LogicalExpressionBase;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.hasNoNulls;
import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isAllNulls;
import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isNullOrEmpty;

/**
 * IS predicates for parquet filter pushdown.
 */
public class ParquetIsPredicate<C extends Comparable<C>> extends LogicalExpressionBase
    implements ParquetFilterPredicate<C> {

  private final LogicalExpression expr;

  private final BiFunction<Statistics<C>, RangeExprEvaluator<C>, RowsMatch> predicate;

  private ParquetIsPredicate(LogicalExpression expr, BiFunction<Statistics<C>, RangeExprEvaluator<C>, RowsMatch> predicate) {
    super(expr.getPosition());
    this.expr = expr;
    this.predicate = predicate;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    final List<LogicalExpression> args = new ArrayList<>();
    args.add(expr);
    return args.iterator();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  /**
   * Apply the filter condition against the meta of the rowgroup.
   */
  public RowsMatch matches(RangeExprEvaluator<C> evaluator) {
    Statistics<C> exprStat = expr.accept(evaluator, null);
    return isNullOrEmpty(exprStat) ? RowsMatch.SOME : predicate.apply(exprStat, evaluator);
  }

  /**
   * After the applying of the filter against the statistics of the rowgroup, if the result is RowsMatch.ALL,
   * then we still must know if the rowgroup contains some null values, because it can change the filter result.
   * If it contains some null values, then we change the RowsMatch.ALL into RowsMatch.SOME, which sya that maybe
   * some values (the null ones) should be disgarded.
   */
  private static RowsMatch checkNull(Statistics exprStat) {
    return hasNoNulls(exprStat) ? RowsMatch.ALL : RowsMatch.SOME;
  }

  /**
   * IS NULL predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createIsNullPredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<C>(expr,
      (exprStat, evaluator) -> {
        // for arrays we are not able to define exact number of nulls
        // [1,2,3] vs [1,2] -> in second case 3 is absent and thus it's null but statistics shows no nulls
        if (expr instanceof TypedFieldExpr) {
          TypedFieldExpr typedFieldExpr = (TypedFieldExpr) expr;
          if (typedFieldExpr.getPath().isArray()) {
            return RowsMatch.SOME;
          }
        }
        if (hasNoNulls(exprStat)) {
          return RowsMatch.NONE;
        }
        return isAllNulls(exprStat, evaluator.getRowCount()) ? RowsMatch.ALL : RowsMatch.SOME;
      });
  }

  /**
   * IS NOT NULL predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createIsNotNullPredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<C>(expr,
      (exprStat, evaluator) -> isAllNulls(exprStat, evaluator.getRowCount()) ? RowsMatch.NONE : checkNull(exprStat)
    );
  }

  /**
   * IS TRUE predicate.
   */
  private static LogicalExpression createIsTruePredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return RowsMatch.NONE;
      }
      if (!exprStat.hasNonNullValue()) {
        return RowsMatch.SOME;
      }
      if (!((BooleanStatistics) exprStat).getMax()) {
        return RowsMatch.NONE;
      }
      return ((BooleanStatistics) exprStat).getMin() ? checkNull(exprStat) : RowsMatch.SOME;
    });
  }

  /**
   * IS FALSE predicate.
   */
  private static LogicalExpression createIsFalsePredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return RowsMatch.NONE;
      }
      if (!exprStat.hasNonNullValue()) {
        return RowsMatch.SOME;
      }
      if (((BooleanStatistics) exprStat).getMin()) {
        return RowsMatch.NONE;
      }
      return ((BooleanStatistics) exprStat).getMax() ? RowsMatch.SOME : checkNull(exprStat);
    });
  }

  /**
   * IS NOT TRUE predicate.
   */
  private static LogicalExpression createIsNotTruePredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return RowsMatch.ALL;
      }
      if (!exprStat.hasNonNullValue()) {
        return RowsMatch.SOME;
      }
      if (((BooleanStatistics) exprStat).getMin()) {
        return hasNoNulls(exprStat) ? RowsMatch.NONE : RowsMatch.SOME;
      }
      return ((BooleanStatistics) exprStat).getMax() ? RowsMatch.SOME : RowsMatch.ALL;
    });
  }

  /**
   * IS NOT FALSE predicate.
   */
  private static LogicalExpression createIsNotFalsePredicate(LogicalExpression expr) {
    return new ParquetIsPredicate<Boolean>(expr, (exprStat, evaluator) -> {
      if (isAllNulls(exprStat, evaluator.getRowCount())) {
        return RowsMatch.ALL;
      }
      if (!exprStat.hasNonNullValue()) {
        return RowsMatch.SOME;
      }
      if (!((BooleanStatistics) exprStat).getMax()) {
        return hasNoNulls(exprStat) ? RowsMatch.NONE : RowsMatch.SOME;
      }
      return ((BooleanStatistics) exprStat).getMin() ? RowsMatch.ALL : RowsMatch.SOME;
    });
  }

  public static <C extends Comparable<C>> LogicalExpression createIsPredicate(String function, LogicalExpression expr) {
    switch (function) {
      case FunctionGenerationHelper.IS_NULL:
        return ParquetIsPredicate.<C>createIsNullPredicate(expr);
      case FunctionGenerationHelper.IS_NOT_NULL:
        return ParquetIsPredicate.<C>createIsNotNullPredicate(expr);
      case FunctionGenerationHelper.IS_TRUE:
        return createIsTruePredicate(expr);
      case FunctionGenerationHelper.IS_NOT_TRUE:
        return createIsNotTruePredicate(expr);
      case FunctionGenerationHelper.IS_FALSE:
        return createIsFalsePredicate(expr);
      case FunctionGenerationHelper.IS_NOT_FALSE:
        return createIsNotFalsePredicate(expr);
      default:
        logger.warn("Unhandled IS function. Function name: {}", function);
        return null;
    }
  }
}
