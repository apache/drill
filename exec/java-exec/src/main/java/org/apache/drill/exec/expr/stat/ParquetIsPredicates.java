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
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedFieldExpr;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.parquet.column.statistics.Statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * IS predicates for parquet filter pushdown.
 */
public class ParquetIsPredicates {

  public static abstract class ParquetIsPredicate extends LogicalExpressionBase implements ParquetFilterPredicate {
    protected final LogicalExpression expr;

    public ParquetIsPredicate(LogicalExpression expr) {
      super(expr.getPosition());
      this.expr = expr;
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
  }

  /**
   * IS NULL predicate.
   */
  public static class IsNullPredicate extends ParquetIsPredicate {
    private final boolean isArray;

    public IsNullPredicate(LogicalExpression expr) {
      super(expr);
      this.isArray = isArray(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {

      // for arrays we are not able to define exact number of nulls
      // [1,2,3] vs [1,2] -> in second case 3 is absent and thus it's null but statistics shows no nulls
      if (isArray) {
        return false;
      }

      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if there are no nulls  -> canDrop
      if (!ParquetPredicatesHelper.hasNulls(exprStat)) {
        return true;
      } else {
        return false;
      }
    }

    private boolean isArray(LogicalExpression expression) {
      if (expression instanceof TypedFieldExpr) {
        TypedFieldExpr typedFieldExpr = (TypedFieldExpr) expression;
        SchemaPath schemaPath = typedFieldExpr.getPath();
        return schemaPath.isArray();
      }
      return false;
    }

  }

  /**
   * IS NOT NULL predicate.
   */
  public static class IsNotNullPredicate extends ParquetIsPredicate {
    public IsNotNullPredicate(LogicalExpression expr) {
      super(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if there are all nulls  -> canDrop
      if (ParquetPredicatesHelper.isAllNulls(exprStat, evaluator.getRowCount())) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * IS TRUE predicate.
   */
  public static class IsTruePredicate extends ParquetIsPredicate {
    public IsTruePredicate(LogicalExpression expr) {
      super(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if max value is not true or if there are all nulls  -> canDrop
      if (exprStat.genericGetMax().compareTo(true) != 0 ||
          ParquetPredicatesHelper.isAllNulls(exprStat, evaluator.getRowCount())) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * IS FALSE predicate.
   */
  public static class IsFalsePredicate extends ParquetIsPredicate {
    public IsFalsePredicate(LogicalExpression expr) {
      super(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if min value is not false or if there are all nulls  -> canDrop
      if (exprStat.genericGetMin().compareTo(false) != 0 ||
          ParquetPredicatesHelper.isAllNulls(exprStat, evaluator.getRowCount())) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * IS NOT TRUE predicate.
   */
  public static class IsNotTruePredicate extends ParquetIsPredicate {
    public IsNotTruePredicate(LogicalExpression expr) {
      super(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if min value is not false or if there are no nulls  -> canDrop
      if (exprStat.genericGetMin().compareTo(false) != 0 && !ParquetPredicatesHelper.hasNulls(exprStat)) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * IS NOT FALSE predicate.
   */
  public static class IsNotFalsePredicate extends ParquetIsPredicate {
    public IsNotFalsePredicate(LogicalExpression expr) {
      super(expr);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      Statistics exprStat = expr.accept(evaluator, null);

      if (!ParquetPredicatesHelper.hasStats(exprStat)) {
        return false;
      }

      //if max value is not true or if there are no nulls  -> canDrop
      if (exprStat.genericGetMax().compareTo(true) != 0 && !ParquetPredicatesHelper.hasNulls(exprStat)) {
        return true;
      } else {
        return false;
      }
    }
  }
}
