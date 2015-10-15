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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.server.options.OptionManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class FunctionImplementationRegistry implements FunctionLookupContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);

  private DrillFunctionRegistry drillFuncRegistry;
  private List<PluggableFunctionRegistry> pluggableFuncRegistries = Lists.newArrayList();
  private OptionManager optionManager = null;

  @Deprecated @VisibleForTesting
  public FunctionImplementationRegistry(DrillConfig config){
    this(config, ClassPathScanner.fromPrescan(config));
  }

  public FunctionImplementationRegistry(DrillConfig config, ScanResult classpathScan){
    Stopwatch w = new Stopwatch().start();

    logger.debug("Generating function registry.");
    drillFuncRegistry = new DrillFunctionRegistry(classpathScan);

    Set<Class<? extends PluggableFunctionRegistry>> registryClasses =
        classpathScan.getImplementations(PluggableFunctionRegistry.class);

    for (Class<? extends PluggableFunctionRegistry> clazz : registryClasses) {
      for (Constructor<?> c : clazz.getConstructors()) {
        Class<?>[] params = c.getParameterTypes();
        if (params.length != 1 || params[0] != DrillConfig.class) {
          logger.warn("Skipping PluggableFunctionRegistry constructor {} for class {} since it doesn't implement a " +
              "[constructor(DrillConfig)]", c, clazz);
          continue;
        }

        try {
          PluggableFunctionRegistry registry = (PluggableFunctionRegistry)c.newInstance(config);
          pluggableFuncRegistries.add(registry);
        } catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          logger.warn("Unable to instantiate PluggableFunctionRegistry class '{}'. Skipping it.", clazz, e);
        }

        break;
      }
    }
    logger.info("Function registry loaded.  {} functions loaded in {} ms.", drillFuncRegistry.size(), w.elapsed(TimeUnit.MILLISECONDS));
  }

  public FunctionImplementationRegistry(DrillConfig config, ScanResult classpathScan, OptionManager optionManager) {
    this(config, classpathScan);
    this.optionManager = optionManager;
  }

  /**
   * Register functions in given operator table.
   * @param operatorTable
   */
  public void register(DrillOperatorTable operatorTable) {
    // Register Drill functions first and move to pluggable function registries.
    drillFuncRegistry.register(operatorTable);

    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      registry.register(operatorTable);
    }
  }

  /**
   * Using the given <code>functionResolver</code> find Drill function implementation for given
   * <code>functionCall</code>
   *
   * @param functionResolver
   * @param functionCall
   * @return
   */
  @Override
  public DrillFuncHolder findDrillFunction(FunctionResolver functionResolver, FunctionCall functionCall) {
    return functionResolver.getBestMatch(drillFuncRegistry.getMethods(functionReplacement(functionCall)), functionCall);
  }

  // Check if this Function Replacement is needed; if yes, return a new name. otherwise, return the original name
  private String functionReplacement(FunctionCall functionCall) {
    String funcName = functionCall.getName();
    if (optionManager != null
        && optionManager.getOption(ExecConstants.CAST_TO_NULLABLE_NUMERIC).bool_val
        && functionCall.args.size() > 0
        && CastFunctions.isReplacementNeeded(functionCall.args.get(0).getMajorType().getMinorType(),
                                             funcName)) {
      org.apache.drill.common.types.TypeProtos.DataMode dataMode =
          functionCall.args.get(0).getMajorType().getMode();
      funcName = CastFunctions.getReplacingCastFunction(funcName, dataMode);
    }

    return funcName;
  }

  /**
   * Find the Drill function implementation that matches the name, arg types and return type.
   * @param name
   * @param argTypes
   * @param returnType
   * @return
   */
  public DrillFuncHolder findExactMatchingDrillFunction(String name, List<MajorType> argTypes, MajorType returnType) {
    for (DrillFuncHolder h : drillFuncRegistry.getMethods(name)) {
      if (h.matches(returnType, argTypes)) {
        return h;
      }
    }

    return null;
  }

  /**
   * Find function implementation for given <code>functionCall</code> in non-Drill function registries such as Hive UDF
   * registry.
   *
   * Note: Order of searching is same as order of {@link org.apache.drill.exec.expr.fn.PluggableFunctionRegistry}
   * implementations found on classpath.
   *
   * @param functionCall
   * @return
   */
  @Override
  public AbstractFuncHolder findNonDrillFunction(FunctionCall functionCall) {
    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      AbstractFuncHolder h = registry.getFunction(functionCall);
      if (h != null) {
        return h;
      }
    }

    return null;
  }

  // Method to find if the output type of a drill function if of complex type
  public boolean isFunctionComplexOutput(String name) {
    List<DrillFuncHolder> methods = drillFuncRegistry.getMethods(name);
    for (DrillFuncHolder holder : methods) {
      if (holder.getReturnValue().isComplexWriter()) {
        return true;
      }
    }
    return false;
  }
}
