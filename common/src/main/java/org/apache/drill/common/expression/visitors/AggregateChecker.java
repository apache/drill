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

import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.IfExpression.IfCondition;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;

public final class AggregateChecker implements ExprVisitor<Boolean, ErrorCollector, RuntimeException>{

  public static final AggregateChecker INSTANCE = new AggregateChecker();

  public static boolean isAggregating(LogicalExpression e, ErrorCollector errors){
    return e.accept(INSTANCE, errors);
  }

  @Override
  public Boolean visitFunctionCall(FunctionCall call, ErrorCollector errors) {
    throw new UnsupportedOperationException("FunctionCall is not expected here. "+
      "It should have been converted to FunctionHolderExpression in materialization");
  }

  @Override
  public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, ErrorCollector errors) {
    if(holder.isAggregating()){
      for (int i = 0; i < holder.args.size(); i++) {
        LogicalExpression e = holder.args.get(i);
        if(e.accept(this, errors)){
          errors.addGeneralError(e.getPosition(),
            String.format("Aggregating function call %s includes nested aggregations at arguments number %d. " +
              "This isn't allowed.", holder.getName(), i));
        }
      }
      return true;
    }else{
      for(LogicalExpression e : holder.args){
        if(e.accept(this, errors)) return true;
      }
      return false;
    }
  }

  @Override
  public Boolean visitIfExpression(IfExpression ifExpr, ErrorCollector errors) {
    for(IfCondition c : ifExpr.conditions){
      if(c.condition.accept(this, errors) || c.expression.accept(this, errors)) return true;
    }
    return ifExpr.elseExpression.accept(this, errors);
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitIntConstant(IntExpression intExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitFloatConstant(FloatExpression fExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitLongConstant(LongExpression intExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitDoubleConstant(DoubleExpression dExpr, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitBooleanConstant(BooleanExpression e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitQuotedStringConstant(QuotedString e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitUnknown(LogicalExpression e, ErrorCollector errors) {
    return false;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, ErrorCollector errors) {
    return e.getInput().accept(this, errors);
  }
}
