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
import org.apache.parquet.column.statistics.Statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Comparison predicates for parquet filter pushdown.
 */
public class ParquetComparisonPredicates {
  public static abstract  class ParquetCompPredicate extends LogicalExpressionBase implements ParquetFilterPredicate {
    protected final LogicalExpression left;
    protected final LogicalExpression right;

    public ParquetCompPredicate(LogicalExpression left, LogicalExpression right) {
      super(left.getPosition());
      this.left = left;
      this.right = right;
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

  }

  /**
   * EQ (=) predicate
   */
  public static class EqualPredicate extends ParquetCompPredicate {
    public EqualPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    /**
        Semantics of canDrop() is very similar to what is implemented in Parquet library's
        {@link org.apache.parquet.filter2.statisticslevel.StatisticsFilter} and
        {@link org.apache.parquet.filter2.predicate.FilterPredicate}

        Main difference :
     1. A RangeExprEvaluator is used to compute the min/max of an expression, such as CAST function
        of a column. CAST function could be explicitly added by Drill user (It's recommended to use CAST
        function after DRILL-4372, if user wants to reduce planning time for limit 0 query), or implicitly
        inserted by Drill, when the types of compare operands are not identical. Therefore, it's important
         to allow CAST function to appear in the filter predicate.
     2. We do not require list of ColumnChunkMetaData to do the evaluation, while Parquet library's
        StatisticsFilter has such requirement. Drill's ParquetTableMetaData does not maintain ColumnChunkMetaData,
        making it impossible to directly use Parquet library's StatisticFilter in query planning time.
     3. We allows both sides of comparison operator to be a min/max range. As such, we support
           expression_of(Column1)   <   expression_of(Column2),
        where Column1 and Column2 are from same parquet table.
     */
    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, = is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when left's max < right's min, or right's max < left's min
      if ( ( leftStat.genericGetMax().compareTo(rightStat.genericGetMin()) < 0
            || rightStat.genericGetMax().compareTo(leftStat.genericGetMin()) < 0)) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return left.toString()  + " = " + right.toString();
    }
  }

  /**
   * GT (>) predicate.
   */
  public static class GTPredicate extends ParquetCompPredicate {
    public GTPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, = is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when left's max <= right's min.
      if ( leftStat.genericGetMax().compareTo(rightStat.genericGetMin()) <= 0 ) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * GE (>=) predicate.
   */
  public static class GEPredicate extends ParquetCompPredicate {
    public GEPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, = is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when left's max < right's min.
      if ( leftStat.genericGetMax().compareTo(rightStat.genericGetMin()) < 0 ) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * LT (<) predicate.
   */
  public static class LTPredicate extends ParquetCompPredicate {
    public LTPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, = is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when right's max <= left's min.
      if ( rightStat.genericGetMax().compareTo(leftStat.genericGetMin()) <= 0 ) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * LE (<=) predicate.
   */
  public static class LEPredicate extends ParquetCompPredicate {
    public LEPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, = is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when right's max < left's min.
      if ( rightStat.genericGetMax().compareTo(leftStat.genericGetMin()) < 0 ) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * NE (!=) predicate.
   */
  public static class NEPredicate extends ParquetCompPredicate {
    public NEPredicate(LogicalExpression left, LogicalExpression right) {
      super(left, right);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics leftStat = left.accept(evaluator, null);
      Statistics rightStat = right.accept(evaluator, null);

      if (leftStat == null ||
          rightStat == null ||
          leftStat.isEmpty() ||
          rightStat.isEmpty()) {
        return false;
      }

      // if either side is ALL null, comparison is evaluated to UNKNOW -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(leftStat, evaluator.getRowCount()) ||
          ParquetPredicatesHelper.isAllNulls(rightStat, evaluator.getRowCount())) {
        return true;
      }

      // can drop when there is only one unique value.
      if ( leftStat.genericGetMin().compareTo(leftStat.genericGetMax()) == 0 &&
           rightStat.genericGetMin().compareTo(rightStat.genericGetMax()) ==0 &&
           leftStat.genericGetMax().compareTo(rightStat.genericGetMax()) == 0) {
        return true;
      } else {
        return false;
      }
    }
  }
}
