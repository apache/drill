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
package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.OutputTypeDeterminer;



public class MathFunctions implements CallProvider{
	
  
  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
        FunctionDefinition.operator("add", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "+"),
        FunctionDefinition.operator("subtract", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "-"),
        FunctionDefinition.operator("divide", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "/"),
        FunctionDefinition.operator("multiply", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "*"),
        FunctionDefinition.operator("modulo", new ArgumentValidators.NumericTypeAllowed(1, Integer.MAX_VALUE, true), new OutputTypeDeterminer.SameAsAnySoft(), "%"),
        
    };

  }
  
	
}
