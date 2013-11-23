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
package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;

public class CastFunctionDefs implements CallProvider{
  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
        FunctionDefinition.simple("castBIGINT", new ArgumentValidators.AnyTypeAllowed(1,2), OutputTypeDeterminer.FIXED_BIGINT),
        FunctionDefinition.simple("castINT", new ArgumentValidators.AnyTypeAllowed(1,2), OutputTypeDeterminer.FIXED_INT),
        FunctionDefinition.simple("castFLOAT4", new ArgumentValidators.AnyTypeAllowed(1,2), OutputTypeDeterminer.FIXED_FLOAT4),
        FunctionDefinition.simple("castFLOAT8", new ArgumentValidators.AnyTypeAllowed(1,2), OutputTypeDeterminer.FIXED_FLOAT8),
        FunctionDefinition.simple("castVARCHAR", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_VARCHAR),
        FunctionDefinition.simple("castVARBINARY", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_VARBINARY),
        FunctionDefinition.simple("castVAR16CHAR", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_VAR16CHAR)
    };
  }
}
