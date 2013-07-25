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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

public interface OutputTypeDeterminer {

  public static OutputTypeDeterminer FIXED_BOOLEAN = new FixedType(MajorType.newBuilder().setMinorType(MinorType.BOOLEAN).setMode(DataMode.REQUIRED).build());
  
  public MajorType getOutputType(List<LogicalExpression> expressions);
  
  
  public static class FixedType implements OutputTypeDeterminer{
    public MajorType outputType;
    
    
    public FixedType(MajorType outputType) {
      super();
      this.outputType = outputType;
    }


    @Override
    public MajorType getOutputType(List<LogicalExpression> expressions) {
      return outputType;
    }
    
  }
  
  public static class SameAsFirstInput implements OutputTypeDeterminer{
    @Override
    public MajorType getOutputType(List<LogicalExpression> expressions) {
      return expressions.get(0).getMajorType();
    }
  }
  
  public static class SameAsAnySoft implements OutputTypeDeterminer{
    @Override
    public MajorType getOutputType(List<LogicalExpression> expressions) {
      for(LogicalExpression e : expressions){
        if(e.getMajorType().getMode() == DataMode.OPTIONAL){
          return e.getMajorType();
        }
      }
      return expressions.get(0).getMajorType();
    }
  }
  
}
