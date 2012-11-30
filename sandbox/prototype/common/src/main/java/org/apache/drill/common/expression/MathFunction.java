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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MathFunction extends LogicalExpressionBase{
	
	static final Logger logger = LoggerFactory.getLogger(MathFunction.class);
	
	public final Method method;
	public final LogicalExpression left;
	public final LogicalExpression right;

	public MathFunction(Method method, LogicalExpression left, LogicalExpression right) throws ExpressionValidationError {
		this.method = method;
		this.left = left;
		this.right = right;
	}
	
	public MathFunction(String methodString, LogicalExpression left, LogicalExpression right) throws ExpressionValidationError {
		logger.debug("{}|{} Generating new Math expression of type " + methodString, left, right);
		Method temp = null;
		for(Method m : Method.values()){
			if(m.expr.equals(methodString)){
				temp = m;
				break;
			}
		}		
		if(temp == null) throw new IllegalArgumentException("Unknown match operator: " + methodString);
		this.method = temp;
		this.left = left;
		this.right = right;
	}

  @Override
  public void addToString(StringBuilder sb) {
    sb.append(" ( ");
    left.addToString(sb);
    sb.append(" ");
    sb.append(method.expr);
    sb.append(" ");
    right.addToString(sb);
    sb.append(" ) ");
  }

  
	public static enum Method{
		ADD("+"), DIVIDE("/"), MULTIPLY("*"), SUBSTRACT("-"), POWER("^"), MOD("%");
		public final String expr;
		
		Method(String expr){
			this.expr = expr;
		}
	}

	
	public static LogicalExpression create(List<LogicalExpression> expressions, List<String> operators){
		
		if(expressions.size() == 1){
			return expressions.get(0);
		}
		
		if(expressions.size()-1 != operators.size()) throw new IllegalArgumentException("Must receive one more expression then the provided number of operators.");
		
		LogicalExpression first = expressions.get(0);
		LogicalExpression second;
		for(int i=0; i < operators.size(); i ++){
			second = expressions.get(i+1);
			first = new MathFunction(operators.get(i), first, second );
		}
		return first;
	}


	
}
