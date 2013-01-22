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

import org.apache.drill.common.expression.types.DataType;

public interface OutputTypeDeterminer {

  public static OutputTypeDeterminer FIXED_BOOLEAN = new FixedType(DataType.BOOLEAN);
  
  public DataType getOutputType(List<LogicalExpression> expressions);
  
  
  public static class FixedType implements OutputTypeDeterminer{
    public DataType outputType;
    
    
    public FixedType(DataType outputType) {
      super();
      this.outputType = outputType;
    }


    @Override
    public DataType getOutputType(List<LogicalExpression> expressions) {
      return null;
    }
    
  }
  
  public static class SameAsFirstInput implements OutputTypeDeterminer{

    @Override
    public DataType getOutputType(List<LogicalExpression> expressions) {
      return expressions.get(0).getDataType();
    }
  }
}
