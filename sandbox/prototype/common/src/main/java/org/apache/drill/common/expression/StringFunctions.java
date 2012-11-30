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

public class StringFunctions {

	public static final Class<?>[] SUB_TYPES = {Regex.class, StartsWith.class};
	
	@FunctionName("regex")
	public static class Regex extends FunctionBase{

		
		public Regex(List<LogicalExpression> expressions) {
			super(expressions);

		}

		@Override
		public DataType getDataType() {
			return DataType.NVARCHAR;
		}


    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "regex");
    }
    
	}

	@FunctionName("startsWith")
	public static class StartsWith extends FunctionBase{
		
		public StartsWith(List<LogicalExpression> expressions) {
			super(expressions);
		}

		@Override
		public DataType getDataType() {
			return DataType.BOOLEAN;
		}

    @Override
    public void addToString(StringBuilder sb) {
      this.funcToString(sb, "startsWith");
    }

		

	}
	
}
