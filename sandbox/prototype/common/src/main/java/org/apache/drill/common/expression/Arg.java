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

import org.apache.drill.common.expression.types.DataType;
import org.apache.drill.common.expression.visitors.ConstantChecker;


public class Arg {
  private final String name;
  private final DataType[] allowedTypes;
  private final boolean constantsOnly;
  
  public Arg(DataType... allowedTypes){
    this(false, null, allowedTypes);
  }
  
  public Arg(String name, DataType... allowedTypes) {
    this(false, name, allowedTypes);
  }

  public Arg(boolean constantsOnly, String name, DataType... allowedTypes) {
    this.name = name;
    this.allowedTypes = allowedTypes;
    this.constantsOnly = constantsOnly;
  }

  public String getName(){
    return name;
  }
  
  public void confirmDataType(String expr, int argIndex, LogicalExpression e, ErrorCollector errors){
    if(constantsOnly){
      if(ConstantChecker.onlyIncludesConstants(e)) errors.addExpectedConstantValue(expr, argIndex, name);
    }
    DataType dt = e.getDataType();
    if(dt.isLateBind()){
      
      // change among allowed types.
      for(DataType a : allowedTypes){
        if(dt == a) return;
      }
      
      // didn't find an allowed type.
      errors.addUnexpectedArgumentType(expr, name, dt, allowedTypes, argIndex);
      
    }
    
    
  }
}