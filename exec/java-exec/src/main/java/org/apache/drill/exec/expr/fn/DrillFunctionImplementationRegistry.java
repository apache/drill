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

import java.util.Set;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.DrillFunc;

import com.google.common.collect.ArrayListMultimap;

public class DrillFunctionImplementationRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionImplementationRegistry.class);

  private ArrayListMultimap<String, DrillFuncHolder> methods = ArrayListMultimap.create();

  public DrillFunctionImplementationRegistry(DrillConfig config){
    FunctionConverter converter = new FunctionConverter();
    Set<Class<? extends DrillFunc>> providerClasses = PathScanner.scanForImplementations(DrillFunc.class, config.getStringList(ExecConstants.FUNCTION_PACKAGES));
    for (Class<? extends DrillFunc> clazz : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(clazz);
      if(holder != null){
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();
        for(String name : names) methods.put(name, holder);
      }else{
        logger.warn("Unable to initialize function for class {}", clazz.getName());
      }
    }
  }

  public ArrayListMultimap<String, DrillFuncHolder> getMethods() {
    return this.methods;
  }
}
