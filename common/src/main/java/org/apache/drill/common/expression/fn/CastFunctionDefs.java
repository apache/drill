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
import org.apache.drill.common.types.TypeProtos.MinorType;

public class CastFunctionDefs implements CallProvider{
  
  private static final FunctionDefinition CAST_BIG_INT = FunctionDefinition.simple("castBIGINT", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_BIGINT);
  private static final FunctionDefinition CAST_INT = FunctionDefinition.simple("castINT", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_INT);
  private static final FunctionDefinition CAST_FLOAT4 = FunctionDefinition.simple("castFLOAT4", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_FLOAT4); 
  private static final FunctionDefinition CAST_FLOAT8 = FunctionDefinition.simple("castFLOAT8", new ArgumentValidators.AnyTypeAllowed(1,3), OutputTypeDeterminer.FIXED_FLOAT8);
  private static final FunctionDefinition CAST_VARCHAR = FunctionDefinition.simple("castVARCHAR", new ArgumentValidators.AnyTypeAllowed(1,4), OutputTypeDeterminer.FIXED_VARCHAR);
  private static final FunctionDefinition CAST_VARBINARY = FunctionDefinition.simple("castVARBINARY", new ArgumentValidators.AnyTypeAllowed(1,4), OutputTypeDeterminer.FIXED_VARBINARY);
  private static final FunctionDefinition CAST_VAR16CHAR = FunctionDefinition.simple("castVAR16CHAR", new ArgumentValidators.AnyTypeAllowed(1,4), OutputTypeDeterminer.FIXED_VAR16CHAR);

  
  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
        CAST_BIG_INT,
        CAST_INT,
        CAST_FLOAT4,
        CAST_FLOAT8,
        CAST_VARCHAR,
        CAST_VARBINARY,
        CAST_VAR16CHAR
    };
  }
  
  public static FunctionDefinition getCastFuncDef(MinorType targetMinorType) {
    switch (targetMinorType) {
      case BIGINT : 
        return CAST_BIG_INT;
      case INT :
        return CAST_INT;
      case FLOAT4: 
        return CAST_FLOAT4;
      case FLOAT8:
        return CAST_FLOAT8;
      case VARCHAR:
        return CAST_VARCHAR;
      case  VARBINARY:
        return CAST_VARBINARY;
      case VAR16CHAR:
        return CAST_VAR16CHAR;

      default:
        throw new RuntimeException(String.format("cast function for type %s is not defined", targetMinorType.name()));      
    }
    
  }
}
