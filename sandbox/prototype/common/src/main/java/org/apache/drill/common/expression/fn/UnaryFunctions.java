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
import org.apache.drill.common.expression.BasicArgumentValidator;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.Types;

public class UnaryFunctions implements CallProvider{

  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
        FunctionDefinition.operator("isNull", new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.FixedType(Types.REQUIRED_BOOLEAN)),
        FunctionDefinition.operator("not", new BasicArgumentValidator(Types.REQUIRED_BOOLEAN), new OutputTypeDeterminer.FixedType(Types.REQUIRED_BOOLEAN), "!"),
        FunctionDefinition.operator("negative", new ArgumentValidators.NumericTypeAllowed(1, true), new OutputTypeDeterminer.SameAsFirstInput(), "u-"),
    };

  }

}
