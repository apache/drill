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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.parquet.column.statistics.Statistics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.hasNoNulls;
import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isNullOrEmpty;
import static org.apache.drill.exec.expr.stat.ParquetPredicatesHelper.isAllNulls;

/**
 * Comparison predicates for parquet filter pushdown.
 */
public class ParquetComparisonPredicate<C extends Comparable<C>> extends LogicalExpressionBase
    implements ParquetFilterPredicate<C> {
  private final LogicalExpression left;
  private final LogicalExpression right;

  private final BiFunction<Statistics<C>, Statistics<C>, RowsMatch> predicate;

  private ParquetComparisonPredicate(
      LogicalExpression left,
      LogicalExpression right,
      BiFunction<Statistics<C>, Statistics<C>, RowsMatch> predicate
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
   * Semantics of matches() is very similar to what is implemented in Parquet library's
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
  public RowsMatch matches(RangeExprEvaluator<C> evaluator) {
    Statistics<C> leftStat = left.accept(evaluator, null);
    if (isNullOrEmpty(leftStat)) {
      return RowsMatch.SOME;
    }
    Statistics<C> rightStat = right.accept(evaluator, null);
    if (isNullOrEmpty(rightStat)) {
      return RowsMatch.SOME;
    }
    if (isAllNulls(leftStat, evaluator.getRowCount()) || isAllNulls(rightStat, evaluator.getRowCount())) {
      return RowsMatch.NONE;
    }
    if (!leftStat.hasNonNullValue() || !rightStat.hasNonNullValue()) {
      return RowsMatch.SOME;
    }

    if (left.getMajorType().getMinorType() == TypeProtos.MinorType.VARDECIMAL) {
      /*
        to compare correctly two decimal statistics we need to ensure that min and max values have the same scale,
        otherwise adjust statistics to the highest scale
        since decimal value is stored as unscaled we need to move dot to the right on the difference between scales
       */
      int leftScale = left.getMajorType().getScale();
      int rightScale = right.getMajorType().getScale();
      if (leftScale > rightScale) {
        rightStat = adjustDecimalStatistics(rightStat, leftScale - rightScale);
      } else if (leftScale < rightScale) {
        leftStat = adjustDecimalStatistics(leftStat, rightScale - leftScale);
      }
    }

    return predicate.apply(leftStat, rightStat);
  }

  /**
   * Creates decimal statistics where min and max values are re-created using given scale.
   *
   * @param statistics statistics that needs to be adjusted
   * @param scale adjustment scale
   * @return adjusted statistics
   */
  @SuppressWarnings("unchecked")
  private Statistics<C> adjustDecimalStatistics(Statistics<C> statistics, int scale) {
    byte[] minBytes = new BigDecimal(new BigInteger(statistics.getMinBytes()))
      .setScale(scale, RoundingMode.HALF_UP).unscaledValue().toByteArray();
    byte[] maxBytes = new BigDecimal(new BigInteger(statistics.getMaxBytes()))
      .setScale(scale, RoundingMode.HALF_UP).unscaledValue().toByteArray();
    return (Statistics<C>) Statistics.getBuilderForReading(statistics.type())
        .withMin(minBytes)
        .withMax(maxBytes)
        .withNumNulls(statistics.getNumNulls())
        .build();
  }

  /**
   * If one rowgroup contains some null values, change the RowsMatch.ALL into RowsMatch.SOME (null values should be discarded by filter)
   */
  private static RowsMatch checkNull(Statistics leftStat, Statistics rightStat) {
    return !hasNoNulls(leftStat) || !hasNoNulls(rightStat) ? RowsMatch.SOME : RowsMatch.ALL;
  }

  /**
   * EQ (=) predicate
   */
  private static <C extends Comparable<C>> LogicalExpression createEqualPredicate(
      LogicalExpression left,
      LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      // compare left max and right min
      int leftToRightComparison = leftStat.compareMaxToValue(rightStat.genericGetMin());
      // compare right max and left min
      int rightToLeftComparison = rightStat.compareMaxToValue(leftStat.genericGetMin());

      // if both comparison results are equal to 0 and both statistics have no nulls,
      // it means that min and max values in each statistics are the same and match each other,
      // return that all rows match the condition
      if (leftToRightComparison == 0 && rightToLeftComparison == 0 && hasNoNulls(leftStat) && hasNoNulls(rightStat)) {
        return RowsMatch.ALL;
      }

      // if at least one comparison result is negative, it means that none of the rows match the condition
      return leftToRightComparison < 0 || rightToLeftComparison < 0 ? RowsMatch.NONE : RowsMatch.SOME;
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
      if (leftStat.compareMaxToValue(rightStat.genericGetMin()) <= 0) {
        return RowsMatch.NONE;
      }
      return leftStat.compareMinToValue(rightStat.genericGetMax()) > 0 ? checkNull(leftStat, rightStat) : RowsMatch.SOME;
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
      if (leftStat.compareMaxToValue(rightStat.genericGetMin()) < 0) {
        return RowsMatch.NONE;
      }
      return leftStat.compareMinToValue(rightStat.genericGetMax()) >= 0 ? checkNull(leftStat, rightStat) : RowsMatch.SOME;
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
      if (rightStat.compareMaxToValue(leftStat.genericGetMin()) <= 0) {
        return RowsMatch.NONE;
      }
      return leftStat.compareMaxToValue(rightStat.genericGetMin()) < 0 ? checkNull(leftStat, rightStat) : RowsMatch.SOME;
    });
  }

  /**
   * LE (<=) predicate.
   */
  private static <C extends Comparable<C>> LogicalExpression createLEPredicate(
      LogicalExpression left, LogicalExpression right
  ) {
    return new ParquetComparisonPredicate<C>(left, right, (leftStat, rightStat) -> {
      if (rightStat.compareMaxToValue(leftStat.genericGetMin()) < 0) {
        return RowsMatch.NONE;
      }
      return leftStat.compareMaxToValue(rightStat.genericGetMin()) <= 0 ? checkNull(leftStat, rightStat) : RowsMatch.SOME;
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
      if (leftStat.compareMaxToValue(rightStat.genericGetMin()) < 0 || rightStat.compareMaxToValue(leftStat.genericGetMin()) < 0) {
        return checkNull(leftStat, rightStat);
      }
      return leftStat.compareMaxToValue(rightStat.genericGetMax()) == 0 && leftStat.compareMinToValue(rightStat.genericGetMin()) == 0 ? RowsMatch.NONE : RowsMatch.SOME;
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
