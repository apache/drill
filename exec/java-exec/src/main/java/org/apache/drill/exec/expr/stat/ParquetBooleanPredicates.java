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
public class ParquetBooleanPredicates {
  public static abstract class ParquetBooleanPredicate extends BooleanOperator implements ParquetFilterPredicate {
    public ParquetBooleanPredicate(String name, List<LogicalExpression> args, ExpressionPosition pos) {
      super(name, args, pos);
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitBooleanOperator(this, value);
    }
  }

  public static class AndPredicate extends ParquetBooleanPredicate {
    public AndPredicate(String name, List<LogicalExpression> args, ExpressionPosition pos) {
      super(name, args, pos);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      // "and" : as long as one branch is OK to drop, we can drop it.
      for (LogicalExpression child : this) {
        if (child instanceof ParquetFilterPredicate && ((ParquetFilterPredicate) child).canDrop(evaluator)) {
          return true;
        }
      }
      return false;
    }
  }

  public static class OrPredicate extends ParquetBooleanPredicate {
    public OrPredicate(String name, List<LogicalExpression> args, ExpressionPosition pos) {
      super(name, args, pos);
    }

    @Override
    public boolean canDrop(RangeExprEvaluator evaluator) {
      for (LogicalExpression child : this) {
        // "long" : as long as one branch is NOT ok to drop, we can NOT drop it.
        if (! ((ParquetFilterPredicate) child).canDrop(evaluator)) {
          return false;
        }
      }

      return true;
    }
  }
}
