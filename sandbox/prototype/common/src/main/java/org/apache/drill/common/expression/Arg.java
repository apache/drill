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

import org.apache.drill.common.expression.visitors.ConstantChecker;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;

import com.google.common.collect.Lists;


public class Arg {
  private final String name;
  private final MajorType[] allowedTypes;
  private final boolean constantsOnly;
  
  
  /**
   * Create an arg that supports any of the listed minor types using opt or req.  Does not allow repeated types.
   * @param name
   * @param constantsOnly
   * @param types
   */
  public Arg(boolean constantsOnly, boolean allowNulls, String name, MinorType... types){
    this(constantsOnly, name, getMajorTypes(allowNulls, types));
  }
  
  public Arg(MajorType... allowedTypes){
    this(false, null, allowedTypes);
  }
  
  public Arg(String name, MajorType... allowedTypes) {
    this(false, name, allowedTypes);
  }

  public Arg(boolean constantsOnly, String name, MajorType... allowedTypes) {
    this.name = name;
    this.allowedTypes = allowedTypes;
    this.constantsOnly = constantsOnly;
  }

  public String getName(){
    return name;
  }
  
  public void confirmDataType(ExpressionPosition expr, int argIndex, LogicalExpression e, ErrorCollector errors){
    if(constantsOnly){
      if(ConstantChecker.onlyIncludesConstants(e)) errors.addExpectedConstantValue(expr, argIndex, name);
    }
    MajorType dt = e.getMajorType();
    if(dt.getMinorType() == MinorType.LATE){
      
      // change among allowed types.
      for(MajorType a : allowedTypes){
        if(dt == a) return;
      }
      
      // didn't find an allowed type.
      errors.addUnexpectedArgumentType(expr, name, dt, allowedTypes, argIndex);
      
    }
    
    
  }
  
  private static MajorType[] getMajorTypes(boolean allowNulls, MinorType... types){
    List<MajorType> mts = Lists.newArrayList();
    for(MinorType t : types){
      if(allowNulls) mts.add(MajorType.newBuilder().setMinorType(t).setMode(DataMode.OPTIONAL).build());
      mts.add(MajorType.newBuilder().setMinorType(t).setMode(DataMode.REQUIRED).build());
    }
    return mts.toArray(new MajorType[mts.size()]);
  }
}