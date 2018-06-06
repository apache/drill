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

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.ExprVisitor;

import java.util.List;

/**
 * Boolean predicates for parquet filter pushdown.
 */
public abstract class ParquetBooleanPredicates<C extends Comparable<C>> extends BooleanOperator
    implements ParquetFilterPredicate<C> {

  private ParquetBooleanPredicates(String name, List<LogicalExpression> args, ExpressionPosition pos) {
    super(name, args, pos);
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitBooleanOperator(this, value);
  }

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> LogicalExpression createAndPredicate(
      String name,
      List<LogicalExpression> args,
      ExpressionPosition pos
  ) {
    return new ParquetBooleanPredicates<C>(name, args, pos) {
      @Override
      public boolean canDrop(RangeExprEvaluator<C> evaluator) {
        // "and" : as long as one branch is OK to drop, we can drop it.
        for (LogicalExpression child : this) {
          if (child instanceof ParquetFilterPredicate && ((ParquetFilterPredicate)child).canDrop(evaluator)) {
            return true;
          }
        }
        return false;
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <C extends Comparable<C>> LogicalExpression createOrPredicate(
      String name,
      List<LogicalExpression> args,
      ExpressionPosition pos
  ) {
    return new ParquetBooleanPredicates<C>(name, args, pos) {
      @Override
      public boolean canDrop(RangeExprEvaluator<C> evaluator) {
        for (LogicalExpression child : this) {
          // "or" : as long as one branch is NOT ok to drop, we can NOT drop it.
          if (!(child instanceof ParquetFilterPredicate) || !((ParquetFilterPredicate)child).canDrop(evaluator)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static <C extends Comparable<C>> LogicalExpression createBooleanPredicate(
      String function,
      String name,
      List<LogicalExpression> args,
      ExpressionPosition pos
  ) {
    switch (function) {
      case "booleanOr":
        return ParquetBooleanPredicates.<C>createOrPredicate(name, args, pos);
      case "booleanAnd":
        return ParquetBooleanPredicates.<C>createAndPredicate(name, args, pos);
      default:
        logger.warn("Unknown Boolean '{}' predicate.", function);
        return null;
    }
  }
}
