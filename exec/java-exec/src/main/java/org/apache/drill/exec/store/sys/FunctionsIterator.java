/*
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
package org.apache.drill.exec.store.sys;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.registry.FunctionHolder;
import org.apache.drill.exec.expr.fn.registry.LocalFunctionRegistry;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.store.pojo.NonNullable;

/**
 * List functions as a System Table
 */
public class FunctionsIterator implements Iterator<Object> {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionsIterator.class);

  private Iterator<FunctionInfo> sortedIterator;

  public FunctionsIterator(ExecutorFragmentContext context) {
    Map<String, FunctionInfo> functionMap = new HashMap<String, FunctionInfo>();
    //Access Registry for function list
    FunctionImplementationRegistry funcImplRegistry = (FunctionImplementationRegistry) context.getFunctionRegistry();
    Map<String, List<FunctionHolder>>  jarFunctionListMap = funcImplRegistry.getAllFunctionsHoldersByJar();

    //Step 1: Load Built-In
    for (FunctionHolder builtInEntry : jarFunctionListMap.get(LocalFunctionRegistry.BUILT_IN)) {
      populateFunctionMap(functionMap, LocalFunctionRegistry.BUILT_IN, builtInEntry.getHolder());
    }
    //Step 2: Load UDFs
    for (String jarName : new ArrayList<>(jarFunctionListMap.keySet())) {
      if (!jarName.equals(LocalFunctionRegistry.BUILT_IN)) {
        for (FunctionHolder udfEntry : jarFunctionListMap.get(jarName)) {
          populateFunctionMap(functionMap, jarName, udfEntry.getHolder());
        }
      }
    }

    List<FunctionInfo> functionList = new ArrayList<FunctionsIterator.FunctionInfo>(functionMap.values());
    functionList.sort(new Comparator<FunctionInfo>() {
      @Override
      public int compare(FunctionInfo o1, FunctionInfo o2) {
        int result = o1.name.compareTo(o2.name);
        if (result == 0) {
          result = o1.signature.compareTo(o2.signature);
        }
        if (result == 0) {
          return o1.returnType.compareTo(o2.returnType);
        }
        return result;
      }
    });

    sortedIterator = functionList.iterator();
  }

  //Populate for give Jar-FunctionHolder Entry
  private void populateFunctionMap(Map<String, FunctionInfo> functionMap, String jarName, DrillFuncHolder dfh) {
    String registeredNames[] = dfh.getRegisteredNames();
    String signature = dfh.getInputParameters();
    String returnType = dfh.getReturnType().getMinorType().toString();
    for (String name : registeredNames) {
      //Generate a uniqueKey to allow for fnName#fnSignature
      String funcSignatureKey = new StringBuilder(64).append(name).append('#').append(signature).toString();
      functionMap.put(funcSignatureKey, new FunctionInfo(name, signature, returnType, jarName));
    }
  }

  @Override
  public boolean hasNext() {
    return sortedIterator.hasNext();
  }

  @Override
  public FunctionInfo next() {
    return sortedIterator.next();
  }

  public static class FunctionInfo {
    @NonNullable
    public final String name;
    @NonNullable
    public final String signature;
    public final String returnType;
    public final String source;

    public FunctionInfo(String funcName, String funcSignature, String funcReturnType, String jarName) {
      this.name = funcName;
      this.signature = funcSignature;
      this.returnType = funcReturnType;
      this.source = jarName;
    }
  }
}
