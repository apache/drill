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
package org.apache.drill.exec.expr.fn;

import java.util.List;
import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ArrayListMultimap;

public class FunctionImplementationRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  
  private ArrayListMultimap<String, DrillFuncHolder> methods = ArrayListMultimap.create();
  
  public FunctionImplementationRegistry(DrillConfig config){
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(clazz);
      if(holder != null){
        methods.put(holder.getFunctionName(), holder);
//        logger.debug("Registering function {}", holder);
      }else{
        logger.warn("Unable to initialize function for class {}", clazz.getName());
      }
    }
  }
  
  public ArrayListMultimap<String, DrillFuncHolder> getMethods() {
    return this.methods;
  }
  
  public DrillFuncHolder getFunction(FunctionCall call){	  
    for(DrillFuncHolder h : methods.get(call.getDefinition().getName())){
      if(h.matches(call)){
        return h;
      }
    }
    
    List<MajorType> types = Lists.newArrayList();
    for(LogicalExpression e : call.args){
      types.add(e.getMajorType());
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Missing function implementation: ");
    sb.append("[");
    sb.append(call.getDefinition().getName());
    sb.append("(");
    boolean first = true;
    for(MajorType mt : types){
      if(first){
        first = false;
      }else{
        sb.append(", ");
      }
      sb.append(mt.getMinorType().name());
      sb.append("-");
      sb.append(mt.getMode().name());
    }
    sb.append(")");
    sb.append("]");
    throw new UnsupportedOperationException(sb.toString());
  }

  

}
