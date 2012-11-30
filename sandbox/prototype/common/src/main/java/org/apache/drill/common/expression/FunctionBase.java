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
package org.apache.drill.common.expression;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.visitors.FunctionVisitor;
import org.apache.drill.common.logical.ValidationError;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;


public abstract class FunctionBase extends LogicalExpressionBase implements Iterable<LogicalExpression>{
	public final ImmutableList<LogicalExpression> expressions;
	
	
  protected void funcToString(StringBuilder sb, String functionName) {
    sb.append(" ");
    sb.append(functionName);
    sb.append("( ");
    for(int i= 0; i < expressions.size(); i++){
      if(i != 0) sb.append(", ");
      expressions.get(i).addToString(sb);
    }
    sb.append(" ) ");
  }
  
  protected void opToString(StringBuilder sb, String operator) {
    sb.append(" ( ");
    expressions.get(0).addToString(sb);
    for(int i =1; i < expressions.size(); i++){
      sb.append(operator);
      expressions.get(i).addToString(sb); 
    }
    sb.append(" ) ");
  }
	
	public FunctionBase(List<LogicalExpression> expressions){
		if( !(expressions instanceof ImmutableList)){
			expressions = ImmutableList.copyOf(expressions);
		}
		this.expressions = (ImmutableList<LogicalExpression>) expressions;
	}
	
	public static DataType getJointType(String parentName, LogicalExpression expr1, LogicalExpression expr2) throws ExpressionValidationError{
		DataType dt = DataType.getCombinedCast(expr1.getDataType(), expr2.getDataType());
		if(dt == null) throw new ExpressionValidationError();
		
		return dt;
	}
	
	public LogicalExpression wrapWithCastIfNecessary(DataType dt) throws ExpressionValidationError{
		if(this.getDataType() != dt) return new Cast(this, dt);
		return this;
	}	

	
	@Override
	public Void accept(FunctionVisitor visitor) {
		visitor.visit(this);
		return null;
	}

	@Override
	public Iterator<LogicalExpression> iterator() {
		return expressions.iterator();
	}

	@Override
	@JsonIgnore
	public DataType getDataType() {
		throw new UnsupportedOperationException();	
	}

	@Override
	public void resolveAndValidate(List<LogicalExpression> expressions,
			Collection<ValidationError> errors) {
		throw new UnsupportedOperationException();	
	}
	
	
	
	
	

}
