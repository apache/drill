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
package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.IfExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.BooleanExpression;
import org.apache.drill.common.expression.ValueExpressions.DoubleExpression;
import org.apache.drill.common.expression.ValueExpressions.LongExpression;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;

public interface ExprVisitor<T, VAL, EXCEP extends Exception> {
	public T visitFunctionCall(FunctionCall call, VAL value) throws EXCEP;
	public T visitIfExpression(IfExpression ifExpr, VAL value) throws EXCEP;
	public T visitSchemaPath(SchemaPath path, VAL value) throws EXCEP;	
	public T visitLongConstant(LongExpression intExpr, VAL value) throws EXCEP;
	public T visitDoubleConstant(DoubleExpression dExpr, VAL value) throws EXCEP;
	public T visitBooleanConstant(BooleanExpression e, VAL value) throws EXCEP;
	public T visitQuotedStringConstant(QuotedString e, VAL value) throws EXCEP;	
	public T visitUnknown(LogicalExpression e, VAL value) throws EXCEP;
}
