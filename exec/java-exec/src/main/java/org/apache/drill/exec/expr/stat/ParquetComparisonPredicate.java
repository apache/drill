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
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.parquet.column.statistics.Statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isNullOrEmpty;
import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isAllNulls;

/**
 * Comparison predicates for parquet filter pushdown.
 */
public class ParquetComparisonPredicate<C extends Comparable<C>> extends LogicalExpressionBase
    implements ParquetFilterPredicate<C> {
  private final LogicalExpression left;
  private final LogicalExpression right;
  private final BiPredicate<Statistics<C>, Statistics<C>> predicate;

  private ParquetComparisonPredicate(
      LogicalExpression left,
      LogicalExpression right,
      BiPredicate<Statistics<C>, Statistics<C>> predicate
  ) {
    super(left.getPosition());
    this.left = left;
    this.right = right;
    this.predicate = predicate;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    final List<LogicalExpression> args = new ArrayList<>();
    args.add(left);
    args.add(right);
    return args.iterator();
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitUnknown(this, value);
  }

  /**
   * Semantics of canDrop() is very similar to what is implemented in Parquet library's
   * {@link org.apache.parquet.filter2.statisticslevel.StatisticsFilter} and
   * {@link org.apache.parquet.filter2.predicate.FilterPredicate}
   *
   * Main difference :
   * 1. A RangeExprEvaluator is used to compute the min/max of an expression, such as CAST function
   * of a column. CAST function could be explicitly added by Drill user (It's recommended to use CAST
   * function after DRILL-4372, if user wants to reduce planning time for limit 0 query), or implicitly
   * inserted by Drill, when the types of compare operands are not identical. Therefore, it's important
   * to allow CAST function to appear in the filter predicate.
   * 2. We do not require list of ColumnChunkMetaData to do the evaluation, while Parquet library's
   * StatisticsFilter has such requirement. Drill's ParquetTableMetaData does not maintain ColumnChunkMetaData,
   * making it impossible to directly use Parquet library's StatisticFilter in query planning time.
   * 3. We allows both sides of comparison operator to be a min/max range. As such, we support
   * expression_of(Column1)   <   expression_of(Column2),
   * where Column1 and Column2 are from same parquet table.
   */
  @Override
  public boolean canDrop(RangeExprEvaluator<C> evaluator) {
    Statistics<C> leftStat = left.accept(evaluator, null);
    if (isNullOrEmpty(leftStat)) {
      return false;
    }

    Statistics<C> rightStat = right.accept(evaluator, null);
    if (isNullOrEmpty(rightStat)) {
      return false;
    }

    // if either side is ALL null, = is evaluated to UNKNOWN -> canDrop
    if (isAllNulls(leftStat, evaluator.getRowCount()) || isAllNulls(rightStat, evaluator.getRowCount())) {
      return true;
    }

    return (leftStat.hasNonNullValue() && rightStat.hasNonNullValue()) && predicate.test(leftStat, rightStat);
  }

  /**
   * EQ (=) predicate
   */
  private static <C extends Comparable<C>> LogicalExpression createEqualPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when left's max < right's min, or right's max < left's min
      final C leftMin = leftStat.genericGetMin();
      final C rightMin = rightStat.genericGetMin();
      return (leftStat.compareMaxToValue(rightMin) < 0) || (rightStat.compareMaxToValue(leftMin) < 0);
    }) {
      @Override
      public String toString() {
        return left + " = " + right;
      }
    };
  }

  /**
   * GT (>) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createGTPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when left's max <= right's min.
      final C rightMin = rightStat.genericGetMin();
      return leftStat.compareMaxToValue(rightMin) <= 0;
    });
  }

  /**
   * GE (>=) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createGEPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when left's max < right's min.
      final C rightMin = rightStat.genericGetMin();
      return leftStat.compareMaxToValue(rightMin) < 0;
    });
  }

  /**
   * LT (<) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createLTPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when right's max <= left's min.
      final C leftMin = leftStat.genericGetMin();
      return rightStat.compareMaxToValue(leftMin) <= 0;
    });
  }

  /**
   * LE (<=) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createLEPredicate(
      LogicalExpression left, LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when right's max < left's min.
      final C leftMin = leftStat.genericGetMin();
      return rightStat.compareMaxToValue(leftMin) < 0;
    });
  }

  /**
   * NE (!=) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createNEPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // can drop when there is only one unique value.
      final C leftMax = leftStat.genericGetMax();
      final C rightMax = rightStat.genericGetMax();
      return leftStat.compareMinToValue(leftMax) == 0 && rightStat.compareMinToValue(rightMax) == 0 &&
          leftStat.compareMaxToValue(rightMax) == 0;
    });
  }

  public static <C extends Comparable<C>> LogicalExpression createComparisonPredicate(
      String function,
      LogicalExpression left,
      LogicalExpression right
  ) {
    switch (function) {
      case FunctionGenerationHelper.EQ:
        return ParquetComparisonPredicate.<C>createEqualPredicate(left, right);
      case FunctionGenerationHelper.GT:
        return ParquetComparisonPredicate.<C>createGTPredicate(left, right);
      case FunctionGenerationHelper.GE:
        return ParquetComparisonPredicate.<C>createGEPredicate(left, right);
      case FunctionGenerationHelper.LT:
        return ParquetComparisonPredicate.<C>createLTPredicate(left, right);
      case FunctionGenerationHelper.LE:
        return ParquetComparisonPredicate.<C>createLEPredicate(left, right);
      case FunctionGenerationHelper.NE:
        return ParquetComparisonPredicate.<C>createNEPredicate(left, right);
      default:
        return null;
    }
  }
}
