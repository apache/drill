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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.common.util.PathScanner;

import com.google.common.collect.Lists;

public class FunctionRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionRegistry.class);
  
  private final Map<String, FunctionDefinition> funcMap;

  public FunctionRegistry(DrillConfig config){
    try{
      Set<Class<? extends CallProvider>> providerClasses = PathScanner.scanForImplementations(CallProvider.class, config.getStringList(CommonConstants.LOGICAL_FUNCTION_SCAN_PACKAGES));
      Map<String, FunctionDefinition> funcs = new HashMap<String, FunctionDefinition>();
      for (Class<? extends CallProvider> c : providerClasses) {
        CallProvider p = c.newInstance();
        FunctionDefinition[] defs = p.getFunctionDefintions();
        for(FunctionDefinition d : defs){
          for(String rn : d.getRegisteredNames()){
            
            FunctionDefinition d2 = funcs.put(rn, d);
            logger.debug("Registering function {}", d);
            if(d2 != null){
              throw new ExceptionInInitializerError(String.format("Failure while registering functions.  The function %s tried to register with the name %s but the function %s already registered with that name.", d.getName(), rn, d2.getName()) );
            }
          }
        }
      }
      funcMap = funcs;
    }catch(Exception e){
      throw new RuntimeException("Failure while setting up FunctionRegistry.", e);
    }
  }
  
  
  public LogicalExpression createExpression(String functionName, ExpressionPosition ep, List<LogicalExpression> args){
    FunctionDefinition d = funcMap.get(functionName);
    if(d == null) throw new ExpressionParsingException(String.format("Unable to find function definition for function named '%s'", functionName));
    return d.newCall(args, ep);
  }
  
  public LogicalExpression createExpression(String unaryName, ExpressionPosition ep, LogicalExpression... e){
    return funcMap.get(unaryName).newCall(Lists.newArrayList(e), ep);
  }
  
  public LogicalExpression createByOp(List<LogicalExpression> args, ExpressionPosition ep, List<String> opTypes) {
    // logger.debug("Generating new comparison expressions.");
    if (args.size() == 1) {
      return args.get(0);
    }

    if (args.size() - 1 != opTypes.size())
      throw new DrillRuntimeException("Must receive one more expression then the provided number of operators.");

    LogicalExpression first = args.get(0);
    for (int i = 0; i < opTypes.size(); i++) {
      List<LogicalExpression> l2 = new ArrayList<LogicalExpression>();
      l2.add(first);
      l2.add(args.get(i + 1));
      first = createExpression(opTypes.get(i), ep, args);
    }
    return first;
  }
}
