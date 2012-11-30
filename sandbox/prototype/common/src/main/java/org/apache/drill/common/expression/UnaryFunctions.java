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


public class UnaryFunctions {


  
  
	public static abstract class UnaryLogicalExpressionBase extends LogicalExpressionBase{
		protected final LogicalExpression input1;
		
		protected UnaryLogicalExpressionBase(LogicalExpression input1){
			this.input1 = input1;
		}
		
	  protected void unaryToString(StringBuilder sb, String expr) {
	    sb.append(" ");
	    sb.append(expr);
	    sb.append("( ");
      input1.addToString(sb);
	    sb.append(" ) ");
	  }
		
	}
	
	public static class Not extends UnaryLogicalExpressionBase{

		public Not(LogicalExpression input1) {
			super(input1);
		}

    @Override
    public void addToString(StringBuilder sb) {
      unaryToString(sb, "!");
    }

		
	}
	
	public static class Negative extends UnaryLogicalExpressionBase{

		public Negative(LogicalExpression input1) {
			super(input1);
		}

    @Override
    public void addToString(StringBuilder sb) {
      unaryToString(sb, "-");
    }

    
		@Override
		public DataType getDataType() {
			return input1.getDataType();
		}
	}
}
