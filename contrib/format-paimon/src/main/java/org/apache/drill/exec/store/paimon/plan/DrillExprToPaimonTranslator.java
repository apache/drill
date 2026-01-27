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
package org.apache.drill.exec.store.paimon.plan;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Optional;

public class DrillExprToPaimonTranslator
  extends AbstractExprVisitor<Object, DrillExprToPaimonTranslator.Context, RuntimeException> {

  public static final ExprVisitor<Object, Context, RuntimeException> INSTANCE =
    new DrillExprToPaimonTranslator();

  public static Predicate translate(LogicalExpression expression, RowType rowType) {
    if (expression == null || rowType == null) {
      return null;
    }
    Object value = expression.accept(INSTANCE, new Context(rowType));
    return value instanceof Predicate ? (Predicate) value : null;
  }

  @Override
  public Object visitFunctionCall(FunctionCall call, Context context) {
    switch (call.getName()) {
      case FunctionNames.AND: {
        Predicate left = asPredicate(call.arg(0).accept(this, context));
        Predicate right = asPredicate(call.arg(1).accept(this, context));
        if (left != null && right != null) {
          return PredicateBuilder.and(left, right);
        }
        return null;
      }
      case FunctionNames.OR: {
        Predicate left = asPredicate(call.arg(0).accept(this, context));
        Predicate right = asPredicate(call.arg(1).accept(this, context));
        if (left != null && right != null) {
          return PredicateBuilder.or(left, right);
        }
        return null;
      }
      case FunctionNames.NOT: {
        Predicate predicate = asPredicate(call.arg(0).accept(this, context));
        if (predicate == null) {
          return null;
        }
        Optional<Predicate> negated = predicate.negate();
        return negated.orElse(null);
      }
      case FunctionNames.IS_NULL: {
        Object arg = call.arg(0).accept(this, context);
        if (arg instanceof SchemaPath) {
          return buildIsNullPredicate(context, (SchemaPath) arg, true);
        }
        return null;
      }
      case FunctionNames.IS_NOT_NULL: {
        Object arg = call.arg(0).accept(this, context);
        if (arg instanceof SchemaPath) {
          return buildIsNullPredicate(context, (SchemaPath) arg, false);
        }
        return null;
      }
      case FunctionNames.LT:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.LT);
      case FunctionNames.LE:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.LE);
      case FunctionNames.GT:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.GT);
      case FunctionNames.GE:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.GE);
      case FunctionNames.EQ:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.EQ);
      case FunctionNames.NE:
        return buildComparisonPredicate(context, call.arg(0), call.arg(1), Comparison.NE);
      default:
        return null;
    }
  }

  @Override
  public Object visitSchemaPath(SchemaPath path, Context context) {
    return path;
  }

  @Override
  public Object visitBooleanConstant(ValueExpressions.BooleanExpression e, Context context) {
    return new LiteralValue(e.getBoolean());
  }

  @Override
  public Object visitFloatConstant(ValueExpressions.FloatExpression fExpr, Context context) {
    return new LiteralValue(fExpr.getFloat());
  }

  @Override
  public Object visitIntConstant(ValueExpressions.IntExpression intExpr, Context context) {
    return new LiteralValue(intExpr.getInt());
  }

  @Override
  public Object visitLongConstant(ValueExpressions.LongExpression longExpr, Context context) {
    return new LiteralValue(longExpr.getLong());
  }

  @Override
  public Object visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Context context) {
    return new LiteralValue(dExpr.getDouble());
  }

  @Override
  public Object visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr, Context context) {
    return new LiteralValue(BigDecimal.valueOf(decExpr.getIntFromDecimal(), decExpr.getScale()));
  }

  @Override
  public Object visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr, Context context) {
    return new LiteralValue(BigDecimal.valueOf(decExpr.getLongFromDecimal(), decExpr.getScale()));
  }

  @Override
  public Object visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr, Context context) {
    return new LiteralValue(decExpr.getBigDecimal());
  }

  @Override
  public Object visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr, Context context) {
    return new LiteralValue(decExpr.getBigDecimal());
  }

  @Override
  public Object visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Context context) {
    return new LiteralValue(decExpr.getBigDecimal());
  }

  @Override
  public Object visitDateConstant(ValueExpressions.DateExpression dateExpr, Context context) {
    return new LiteralValue(new Date(dateExpr.getDate()));
  }

  @Override
  public Object visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Context context) {
    return new LiteralValue(new Time(timeExpr.getTime()));
  }

  @Override
  public Object visitTimeStampConstant(ValueExpressions.TimeStampExpression timestampExpr, Context context) {
    return new LiteralValue(new Timestamp(timestampExpr.getTimeStamp()));
  }

  @Override
  public Object visitQuotedStringConstant(ValueExpressions.QuotedString e, Context context) {
    return new LiteralValue(e.getString());
  }

  @Override
  public Object visitNullExpression(NullExpression e, Context context) {
    return new LiteralValue(null);
  }

  @Override
  public Object visitUnknown(LogicalExpression e, Context context) {
    return null;
  }

  private Predicate buildComparisonPredicate(Context context, LogicalExpression leftExpr,
    LogicalExpression rightExpr, Comparison comparison) {
    Object left = leftExpr.accept(this, context);
    Object right = rightExpr.accept(this, context);
    return buildComparisonPredicate(context, left, right, comparison);
  }

  private Predicate buildComparisonPredicate(Context context, Object left, Object right, Comparison comparison) {
    if (left instanceof SchemaPath && right instanceof LiteralValue) {
      return buildPredicate(context, (SchemaPath) left, comparison, (LiteralValue) right);
    }
    if (right instanceof SchemaPath && left instanceof LiteralValue) {
      return buildPredicate(context, (SchemaPath) right, comparison.flip(), (LiteralValue) left);
    }
    return null;
  }

  private Predicate buildPredicate(Context context, SchemaPath path, Comparison comparison, LiteralValue literalValue) {
    int index = columnIndex(context, path);
    if (index < 0) {
      return null;
    }
    Object value = literalValue.value();
    if (value == null) {
      return null;
    }
    DataField field = context.rowType.getFields().get(index);
    Object internalValue;
    try {
      internalValue = PredicateBuilder.convertJavaObject(field.type(), value);
    } catch (RuntimeException e) {
      return null;
    }
    switch (comparison) {
      case EQ:
        return context.predicateBuilder.equal(index, internalValue);
      case NE:
        return context.predicateBuilder.notEqual(index, internalValue);
      case LT:
        return context.predicateBuilder.lessThan(index, internalValue);
      case LE:
        return context.predicateBuilder.lessOrEqual(index, internalValue);
      case GT:
        return context.predicateBuilder.greaterThan(index, internalValue);
      case GE:
        return context.predicateBuilder.greaterOrEqual(index, internalValue);
      default:
        return null;
    }
  }

  private Predicate buildIsNullPredicate(Context context, SchemaPath path, boolean isNull) {
    int index = columnIndex(context, path);
    if (index < 0) {
      return null;
    }
    return isNull
      ? context.predicateBuilder.isNull(index)
      : context.predicateBuilder.isNotNull(index);
  }

  private int columnIndex(Context context, SchemaPath path) {
    PathSegment segment = path.getRootSegment();
    if (segment == null || !segment.isNamed() || segment.getChild() != null) {
      return -1;
    }
    return context.predicateBuilder.indexOf(segment.getNameSegment().getPath());
  }

  private Predicate asPredicate(Object value) {
    return value instanceof Predicate ? (Predicate) value : null;
  }

  private enum Comparison {
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE;

    public Comparison flip() {
      switch (this) {
        case LT:
          return GT;
        case LE:
          return GE;
        case GT:
          return LT;
        case GE:
          return LE;
        case EQ:
        case NE:
        default:
          return this;
      }
    }
  }

  public static class Context {
    private final RowType rowType;
    private final PredicateBuilder predicateBuilder;

    public Context(RowType rowType) {
      this.rowType = rowType;
      this.predicateBuilder = new PredicateBuilder(rowType);
    }
  }

  private static class LiteralValue {
    private final Object value;

    private LiteralValue(Object value) {
      this.value = value;
    }

    private Object value() {
      return value;
    }
  }
}
