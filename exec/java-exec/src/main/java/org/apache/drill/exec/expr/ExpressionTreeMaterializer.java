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
package org.apache.drill.exec.expr;

import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ExpressionValidator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.SimpleExprVisitor;
import org.apache.drill.exec.record.NullExpression;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;

import com.google.common.collect.Lists;
import org.apache.drill.exec.record.VectorAccessible;

public class ExpressionTreeMaterializer {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionTreeMaterializer.class);
  
  
  
  private ExpressionTreeMaterializer() {
  };

  public static LogicalExpression materialize(LogicalExpression expr, VectorAccessible batch, ErrorCollector errorCollector) {
    return expr.accept(new MaterializeVisitor(batch, errorCollector), null);
  }

  private static class MaterializeVisitor extends SimpleExprVisitor<LogicalExpression> {
    private final ErrorCollector errorCollector;
    private final VectorAccessible batch;
    private ExpressionValidator validator = new ExpressionValidator();

    public MaterializeVisitor(VectorAccessible batch, ErrorCollector errorCollector) {
      this.batch = batch;
      this.errorCollector = errorCollector;
    }

    private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
      newExpr.accept(validator, errorCollector);
      return newExpr;
    }

    @Override
    public LogicalExpression visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      throw new UnsupportedOperationException(String.format("Expression tree materializer does not currently support materializing nodes of type %s.", e.getClass().getCanonicalName()));
    }

    @Override
    public LogicalExpression visitFunctionCall(FunctionCall call) {
      List<LogicalExpression> args = Lists.newArrayList();
      for (int i = 0; i < call.args.size(); ++i) {
        LogicalExpression newExpr = call.args.get(i).accept(this, null);
        args.add(newExpr);
      }

      return validateNewExpr(new FunctionCall(call.getDefinition(), args, call.getPosition()));
    }

    @Override
    public LogicalExpression visitIfExpression(IfExpression ifExpr) {
      List<IfExpression.IfCondition> conditions = Lists.newArrayList(ifExpr.conditions);
      LogicalExpression newElseExpr = ifExpr.elseExpression.accept(this, null);

      for (int i = 0; i < conditions.size(); ++i) {
        IfExpression.IfCondition condition = conditions.get(i);

        LogicalExpression newCondition = condition.condition.accept(this, null);
        LogicalExpression newExpr = condition.expression.accept(this, null);
        conditions.set(i, new IfExpression.IfCondition(newCondition, newExpr));
      }

      return validateNewExpr(IfExpression.newBuilder().setElse(newElseExpr).addConditions(conditions).build());
    }

    @Override
    public LogicalExpression visitSchemaPath(SchemaPath path) {
//      logger.debug("Visiting schema path {}", path);
      TypedFieldId tfId = batch.getValueVectorId(path);
      if (tfId == null) {
        logger.warn("Unable to find value vector of path {}, returning null instance.", path);
        return NullExpression.INSTANCE;
      } else {
        return new ValueVectorReadExpression(tfId);
      }
    }

    @Override
    public LogicalExpression visitLongConstant(ValueExpressions.LongExpression intExpr) {
      return intExpr;
    }

    @Override
    public LogicalExpression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr) {
      return dExpr;
    }

    @Override
    public LogicalExpression visitBooleanConstant(ValueExpressions.BooleanExpression e) {
      return e;
    }

    @Override
    public LogicalExpression visitQuotedStringConstant(ValueExpressions.QuotedString e) {
      return e;
    }
  }
}
