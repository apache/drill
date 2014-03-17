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
package org.apache.drill.common.expression;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos.MajorType;

public class FunctionDefinition {

  private final String name;
  private final String[] registeredNames;
  private final ArgumentValidator argumentValidator;
  private final OutputTypeDeterminer outputType;
  private final boolean aggregating;
  private final boolean isOperator;
  private final boolean isRandom;
  
  private FunctionDefinition(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType,
      boolean aggregating, boolean isOperator, String[] registeredNames) {
    this(name, argumentValidator, outputType, aggregating, isOperator, registeredNames, false);
  }

  private FunctionDefinition(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType,
      boolean aggregating, boolean isOperator, String[] registeredNames, boolean isRandom) {
    this.name = name;
    this.argumentValidator = argumentValidator;
    this.outputType = outputType;
    this.aggregating = aggregating;
    this.registeredNames = ArrayUtils.isEmpty(registeredNames) ? new String[]{name} : registeredNames;
    this.isOperator = isOperator;
    this.isRandom = isRandom;
  }
  
  public MajorType getDataType(List<LogicalExpression> args){
    return outputType.getOutputType(args);
  }
  
  public String[] getArgumentNames(){
    return argumentValidator.getArgumentNamesByPosition();
  }

  public ArgumentValidator getArgumentValidator() {
      return argumentValidator;
  }
  
  public OutputTypeDeterminer getOutputTypeDeterminer() {
    return outputType;
  }
  
  public static FunctionDefinition simple(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType, String... registeredNames){
    return new FunctionDefinition(name, argumentValidator, outputType, false,  false, registeredNames);
  }

  public static FunctionDefinition simpleRandom(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType, String... registeredNames){
    return new FunctionDefinition(name, argumentValidator, outputType, false,  false, registeredNames, true);
  }

  public static FunctionDefinition aggregator(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType, String... registeredNames){
    return new FunctionDefinition(name, argumentValidator, outputType, true,  false, registeredNames);
  }

  public static FunctionDefinition operator(String name, ArgumentValidator argumentValidator, OutputTypeDeterminer outputType, String... registeredNames){
    return new FunctionDefinition(name, argumentValidator, outputType, false,  true, registeredNames);
  }
  
  public boolean isOperator(){
    return isOperator;
  }
  
  public boolean isAggregating(){
    return aggregating;
  }
  
  public String[] getRegisteredNames(){
    return this.registeredNames;
  }
  
  public String getName(){
    return this.name;
  }
  
  public boolean isRandom() {
    return this.isRandom;
  }
  
  public FunctionCall newCall(List<LogicalExpression> args, ExpressionPosition pos){
    return new FunctionCall(this, args, pos);
  }
  
  public void addRegisteredName(StringBuilder sb){
    sb.append(registeredNames[0]);
  }

  @Override
  public String toString() {
    return "FunctionDefinition [name=" + name + ", registeredNames=" + Arrays.toString(registeredNames)
        + ", aggregating=" + aggregating + ", isOperator=" + isOperator + "]";
  }

  
  
}
