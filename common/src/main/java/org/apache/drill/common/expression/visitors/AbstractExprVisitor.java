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

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.FloatExpression;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;

public abstract class AbstractExprVisitor<T, VAL, EXCEP extends Exception> implements ExprVisitor<T, VAL, EXCEP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExprVisitor.class);

  @Override
  public T visitFunctionCall(FunctionCall call, VAL value) throws EXCEP {
    return null;
  }

  @Override
  public T visitIfExpression(IfExpression ifExpr, VAL value) throws EXCEP {
    return visitUnknown(ifExpr, value);
  }

  @Override
  public T visitSchemaPath(SchemaPath path, VAL value) throws EXCEP {
    return visitUnknown(path, value);
  }

  @Override
  public T visitFloatConstant(FloatExpression fExpr, VAL value) throws EXCEP {
    return visitUnknown(fExpr, value);
  }

  @Override
  public T visitIntConstant(IntExpression intExpr, VAL value) throws EXCEP {
    return visitUnknown(intExpr, value);
  }

  @Override
  public T visitLongConstant(LongExpression intExpr, VAL value) throws EXCEP {
    return visitUnknown(intExpr, value);
  }

  @Override
  public T visitDoubleConstant(DoubleExpression dExpr, VAL value) throws EXCEP {
    return visitUnknown(dExpr, value);
  }

  @Override
  public T visitBooleanConstant(BooleanExpression e, VAL value) throws EXCEP {
    return visitUnknown(e, value);
  }

  @Override
  public T visitQuotedStringConstant(QuotedString e, VAL value) throws EXCEP {
    return visitUnknown(e, value);
  }

  @Override
  public T visitUnknown(LogicalExpression e, VAL value) throws EXCEP {
    throw new UnsupportedOperationException(String.format("Expression of type %s not handled by visitor type %s.", e.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
  }
  
  
}
