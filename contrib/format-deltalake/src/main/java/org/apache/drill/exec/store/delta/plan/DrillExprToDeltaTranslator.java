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
package org.apache.drill.exec.store.delta.plan;

import io.delta.standalone.expressions.And;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.expressions.GreaterThan;
import io.delta.standalone.expressions.GreaterThanOrEqual;
import io.delta.standalone.expressions.IsNotNull;
import io.delta.standalone.expressions.IsNull;
import io.delta.standalone.expressions.LessThan;
import io.delta.standalone.expressions.LessThanOrEqual;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.expressions.Not;
import io.delta.standalone.expressions.Or;
import io.delta.standalone.expressions.Predicate;
import io.delta.standalone.types.StructType;
import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

public class DrillExprToDeltaTranslator extends AbstractExprVisitor<Expression, Void, RuntimeException> {

  private final StructType structType;

  public DrillExprToDeltaTranslator(StructType structType) {
    this.structType = structType;
  }

  @Override
  public Expression visitFunctionCall(FunctionCall call, Void value) {
    try {
      return visitFunctionCall(call);
    } catch (Exception e) {
      return null;
    }
  }

  private Predicate visitFunctionCall(FunctionCall call) {
    switch (call.getName()) {
      case FunctionNames.AND: {
        Expression left = call.arg(0).accept(this, null);
        Expression right = call.arg(1).accept(this, null);
        if (left != null && right != null) {
          return new And(left, right);
        }
        return null;
      }
      case FunctionNames.OR: {
        Expression left = call.arg(0).accept(this, null);
        Expression right = call.arg(1).accept(this, null);
        if (left != null && right != null) {
          return new Or(left, right);
        }
        return null;
      }
      case FunctionNames.NOT: {
        Expression expression = call.arg(0).accept(this, null);
        if (expression != null) {
          return new Not(expression);
        }
        return null;
      }
      case FunctionNames.IS_NULL: {
        LogicalExpression arg = call.arg(0);
        if (arg instanceof SchemaPath) {
          String name = getPath((SchemaPath) arg);
          return new IsNull(structType.column(name));
        }
        return null;
      }
      case FunctionNames.IS_NOT_NULL: {
        LogicalExpression arg = call.arg(0);
        if (arg instanceof SchemaPath) {
          String name = getPath((SchemaPath) arg);
          return new IsNotNull(structType.column(name));
        }
        return null;
      }
      case FunctionNames.LT: {
        LogicalExpression nameRef = call.arg(0);
        Expression expression = call.arg(1).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new LessThan(structType.column(name), expression);
        }
        return null;
      }
      case FunctionNames.LE: {
        LogicalExpression nameRef = call.arg(0);
        Expression expression = call.arg(1).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new LessThanOrEqual(structType.column(name), expression);
        }
        return null;
      }
      case FunctionNames.GT: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new GreaterThan(structType.column(name), expression);
        }
        return null;
      }
      case FunctionNames.GE: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(0).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new GreaterThanOrEqual(structType.column(name), expression);
        }
        return null;
      }
      case FunctionNames.EQ: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new EqualTo(structType.column(name), expression);
        }
        return null;
      }
      case FunctionNames.NE: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath) {
          String name = getPath((SchemaPath) nameRef);
          return new Not(new EqualTo(structType.column(name), expression));
        }
        return null;
      }
    }
    return null;
  }

  @Override
  public Expression visitFloatConstant(ValueExpressions.FloatExpression fExpr, Void value) {
    return Literal.of(fExpr.getFloat());
  }

  @Override
  public Expression visitIntConstant(ValueExpressions.IntExpression intExpr, Void value) {
    return Literal.of(intExpr.getInt());
  }

  @Override
  public Expression visitLongConstant(ValueExpressions.LongExpression longExpr, Void value) {
    return Literal.of(longExpr.getLong());
  }

  @Override
  public Expression visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr, Void value) {
    return Literal.of(decExpr.getIntFromDecimal());
  }

  @Override
  public Expression visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr, Void value) {
    return Literal.of(decExpr.getLongFromDecimal());
  }

  @Override
  public Expression visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr, Void value) {
    return Literal.of(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr, Void value) {
    return Literal.of(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Void value) {
    return Literal.of(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitDateConstant(ValueExpressions.DateExpression dateExpr, Void value) {
    return Literal.of(dateExpr.getDate());
  }

  @Override
  public Expression visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Void value) {
    return Literal.of(timeExpr.getTime());
  }

  @Override
  public Expression visitTimeStampConstant(ValueExpressions.TimeStampExpression timestampExpr, Void value) {
    return Literal.of(timestampExpr.getTimeStamp());
  }

  @Override
  public Expression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Void value) {
    return Literal.of(dExpr.getDouble());
  }

  @Override
  public Expression visitBooleanConstant(ValueExpressions.BooleanExpression e, Void value) {
    return Literal.of(e.getBoolean());
  }

  @Override
  public Expression visitQuotedStringConstant(ValueExpressions.QuotedString e, Void value) {
    return Literal.of(e.getString());
  }

  @Override
  public Expression visitUnknown(LogicalExpression e, Void value) {
    return null;
  }

  private static String getPath(SchemaPath schemaPath) {
    StringBuilder sb = new StringBuilder();
    PathSegment segment = schemaPath.getRootSegment();
    sb.append(segment.getNameSegment().getPath());

    while ((segment = segment.getChild()) != null) {
      sb.append('.')
        .append(segment.isNamed()
          ? segment.getNameSegment().getPath()
          : "element");
    }
    return sb.toString();
  }
}
