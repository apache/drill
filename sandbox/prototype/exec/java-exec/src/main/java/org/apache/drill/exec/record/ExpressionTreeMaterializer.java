/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.record;

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.expression.visitors.ExprVisitor;

import java.util.List;

public class ExpressionTreeMaterializer {
    public LogicalExpression Materialize(LogicalExpression expr, BatchSchema schema, ErrorCollector errorCollector) {
        return expr.accept(new MaterializeVisitor(schema, errorCollector));
    }

    private class MaterializeVisitor implements ExprVisitor<LogicalExpression> {
        private final ErrorCollector errorCollector;
        private final BatchSchema schema;
        private boolean isModified; // Flag to track if children is changed

        public MaterializeVisitor(BatchSchema schema, ErrorCollector errorCollector) {
            this.schema = schema;
            this.errorCollector = errorCollector;
            isModified = false;
        }

        private LogicalExpression validateNewExpr(LogicalExpression newExpr) {
            StringBuilder stringBuilder = new StringBuilder();
            newExpr.addToString(stringBuilder);
            newExpr.resolveAndValidate(stringBuilder.toString(), errorCollector);
            return newExpr;
        }

        @Override
        public LogicalExpression visitFunctionCall(FunctionCall call) {
            List<LogicalExpression> args = Lists.newArrayList(call.iterator());
            boolean hasChanged = false;
            for (int i = 0; i < args.size(); ++i) {
                LogicalExpression newExpr = args.get(i).accept(this);
                if (isModified) {
                    hasChanged = true;
                    args.set(i, newExpr);
                    isModified = false;
                }
            }

            if(hasChanged) {
                isModified = true;
                return validateNewExpr(new FunctionCall(call.getDefinition(), args));
            }

            return call;
        }

        @Override
        public LogicalExpression visitIfExpression(IfExpression ifExpr) {
            List<IfExpression.IfCondition> conditions = Lists.newArrayList(ifExpr.iterator());
            boolean hasChanged = false;
            LogicalExpression newElseExpr = null;
            if(ifExpr.elseExpression != null) {
                newElseExpr = ifExpr.elseExpression.accept(this);
                hasChanged = isModified;
            }

            isModified = false;

            for(int i = 0; i < conditions.size(); ++i) {
                IfExpression.IfCondition condition = conditions.get(i);

                LogicalExpression newCondition = condition.condition.accept(this);
                boolean modified = isModified;
                isModified = false;
                LogicalExpression newExpr = condition.expression.accept(this);
                if(modified || isModified) {
                    conditions.set(i, new IfExpression.IfCondition(newCondition, newExpr));
                    hasChanged = true;
                    isModified = false;
                }
            }

            if(hasChanged) {
                isModified = true;
                return validateNewExpr(IfExpression.newBuilder().setElse(newElseExpr).addConditions(conditions).build());
            }

            return ifExpr;
        }

        @Override
        public LogicalExpression visitSchemaPath(SchemaPath path) {
            for (MaterializedField field : schema) {
                if (field.getType() != DataType.LATEBIND && field.matches(path)) {
                    isModified = true;
                    return validateNewExpr(new FieldReference(path.getPath().toString(), field.getType()));
                }
            }

            return path;
        }

        @Override
        public LogicalExpression visitLongExpression(ValueExpressions.LongExpression intExpr) {
            return intExpr;
        }

        @Override
        public LogicalExpression visitDoubleExpression(ValueExpressions.DoubleExpression dExpr) {
            return dExpr;
        }

        @Override
        public LogicalExpression visitBoolean(ValueExpressions.BooleanExpression e) {
            return e;
        }

        @Override
        public LogicalExpression visitQuotedString(ValueExpressions.QuotedString e) {
            return e;
        }
    }
}
