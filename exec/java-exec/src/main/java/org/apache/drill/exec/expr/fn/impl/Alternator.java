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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.NoArgValidator;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.record.RecordBatch;



public class Alternator {
  
  @FunctionTemplate(name = "alternate", scope = FunctionScope.SIMPLE)
  public static class Alternate2 implements DrillSimpleFunc{
    @Workspace int val;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
      val = 0;
    }


    public void eval() {
      out.value = val;
      if(val == 0){
        val = 1;
      }else{
        val = 0;
      }
    }
  }

  @FunctionTemplate(name = "alternate3", scope = FunctionScope.SIMPLE)
  public static class Alternate3 implements DrillSimpleFunc{
    @Workspace int val;
    @Output BigIntHolder out;
    
    public void setup(RecordBatch incoming) {
      val = 0;
    }


    public void eval() {
      out.value = val;
      if(val == 0){
        val = 1;
      }else if(val == 1){
        val = 2;
      }else{
        val = 0;
      }
    }
  }
  
  public static class Provider implements CallProvider{

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[]{
          FunctionDefinition.simpleRandom("alternate", NoArgValidator.VALIDATOR, new OutputTypeDeterminer.FixedType(Types.required(MinorType.BIGINT)), "alternate"),
          FunctionDefinition.simpleRandom("alternate3", NoArgValidator.VALIDATOR, new OutputTypeDeterminer.FixedType(Types.required(MinorType.BIGINT)), "alternate3")
      };
    }
    
  }
}
