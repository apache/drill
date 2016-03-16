/**
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
package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.TypedNullConstant;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DateExpression;
import org.apache.drill.common.expression.ValueExpressions.Decimal18Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal28Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal38Expression;
import org.apache.drill.common.expression.ValueExpressions.Decimal9Expression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalDayExpression;
import org.apache.drill.common.expression.ValueExpressions.IntervalYearExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.ValueExpressions.TimeExpression;
import org.apache.drill.common.expression.ValueExpressions.TimeStampExpression;

final class ConstantChecker implements ExprVisitor<Boolean, ErrorCollector, RuntimeException> {


  private final static ConstantChecker INSTANCE = new ConstantChecker();

  private ConstantChecker() {}

  public static void checkConstants(LogicalExpression e, ErrorCollector errors) {
    e.accept(INSTANCE, errors);
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, ErrorCollector errors) {
    throw new UnsupportedOperationException("FunctionCall is not expected here. "
        + "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, ErrorCollector errors) {
    boolean allArgsAreConstant = true;
    for (int i = 0; i < holder.args.size(); i++) {
      boolean thisArgIsConstant = holder.args.get(i).accept(this, errors);
      if (!thisArgIsConstant) {
        allArgsAreConstant = false;
        if (holder.argConstantOnly(i)) {
          errors.addGeneralError( //
              holder.args.get(i).getPosition(), //
              String.format("Function %s expects constant input for argument number %d", holder.getName(), i));
        }
      }
    }
    return allArgsAreConstant;
  }

  @Override
  public Boolean visitBooleanOperator(BooleanOperator op, ErrorCollector errors) {
    for (LogicalExpression e : op.args) {
      if (!e.accept(this, errors)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, ErrorCollector errors) {
    IfCondition c = ifExpr.ifCondition;
    if (!c.condition.accept(this, errors) || !c.expression.accept(this, errors)) {
      return false;
    }

    if (!ifExpr.elseExpression.accept(this, errors)) {
      return false;
    }
    return true;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitIntConstant(IntExpression intExpr, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitFloatConstant(FloatExpression fExpr, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitLongConstant(LongExpression intExpr, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitDateConstant(DateExpression intExpr, ErrorCollector errors) {
      return true;
  }

  @Override
  public Boolean visitTimeConstant(TimeExpression intExpr, ErrorCollector errors) {
      return true;
  }

  @Override
  public Boolean visitTimeStampConstant(TimeStampExpression intExpr, ErrorCollector errors) {
      return true;
  }

  @Override
  public Boolean visitIntervalYearConstant(IntervalYearExpression intExpr, ErrorCollector errors) {
      return true;
  }

  @Override
  public Boolean visitIntervalDayConstant(IntervalDayExpression intExpr, ErrorCollector errors) {
      return true;
  }

  @Override
  public Boolean visitDecimal9Constant(Decimal9Expression decExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDecimal18Constant(Decimal18Expression decExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDecimal28Constant(Decimal28Expression decExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDecimal38Constant(Decimal38Expression decExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression e, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, ErrorCollector errors) {
    return true;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, ErrorCollector value) throws RuntimeException {
    return e.getInput().accept(this, value);
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, ErrorCollector value) throws RuntimeException {
    return e.getInput().accept(this, value);
  }

  @Override
  public Boolean visitNullConstant(TypedNullConstant e, ErrorCollector value) throws RuntimeException {
    return true;
  }

  @Override
  public Boolean visitNullExpression(NullExpression e, ErrorCollector value) throws RuntimeException {
    return true;
  }

}
