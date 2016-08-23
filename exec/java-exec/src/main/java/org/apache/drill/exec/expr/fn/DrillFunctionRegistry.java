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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.concurrent.AutoCloseableLock;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.FunctionValidationException;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperatorWithoutInference;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import com.google.common.collect.ArrayListMultimap;
import org.apache.drill.exec.planner.sql.DrillSqlOperatorWithoutInference;
import org.apache.drill.exec.proto.UserBitShared.Func;

/**
 * Registry of Drill functions.
 */
public class DrillFunctionRegistry {

  public static final String BUILT_IN = "built-in";

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionRegistry.class);

  // function registry holder
  // Jar names, function names and signatures are stored as String, function holders as DrillFuncHolder.
  private final GenericRegistryHolder<String, DrillFuncHolder> registryHolder = new GenericRegistryHolder<>();

  private static final ImmutableMap<String, Pair<Integer, Integer>> registeredFuncNameToArgRange = ImmutableMap.<String, Pair<Integer, Integer>> builder()
      // CONCAT is allowed to take [1, infinity) number of arguments.
      // Currently, this flexibility is offered by DrillOptiq to rewrite it as
      // a nested structure
      .put("CONCAT", Pair.of(1, Integer.MAX_VALUE))

      // When LENGTH is given two arguments, this function relies on DrillOptiq to rewrite it as
      // another function based on the second argument (encodingType)
      .put("LENGTH", Pair.of(1, 2))

      // Dummy functions
      .put("CONVERT_TO", Pair.of(2, 2))
      .put("CONVERT_FROM", Pair.of(2, 2))
      .put("FLATTEN", Pair.of(1, 1)).build();

  /** Registers all functions present in Drill classpath on start-up. All functions will be marked as built-in.*/
  public DrillFunctionRegistry(ScanResult classpathScan) {
    validate(BUILT_IN, classpathScan);
    register(BUILT_IN, classpathScan, this.getClass().getClassLoader());
    if (logger.isTraceEnabled()) {
      StringBuilder allFunctions = new StringBuilder();
      for (DrillFuncHolder method: registryHolder.getAllFunctionsWithHolders().values()) {
        allFunctions.append(method.toString()).append("\n");
      }
      logger.trace("Registered functions: [\n{}]", allFunctions);
    }
  }

  /**
   * Validates all functions, present in jars.
   * Will throw {@link FunctionValidationException} if:
   * 1. Jar with the same name has been already registered.
   * 2. Conflicting function with the similar signature is found.
   * 3. Aggregating function is not deterministic.
   *
   * @return list of validated functions
   */
  public List<Func> validate(String jarName, ScanResult classpathScan) {
    List<Func> functions = Lists.newArrayList();
    FunctionConverter converter = new FunctionConverter();
    List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();

    if (registryHolder.containsJar(jarName)) {
      throw new FunctionValidationException(String.format("Jar %s is already registered", jarName));
    }

    final ListMultimap<String, String> allFuncWithSignatures = registryHolder.getAllFunctionsWithSignatures();

    for (AnnotatedClassDescriptor func : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(func, ClassLoader.getSystemClassLoader());
      if (holder != null) {

        String functionInput = "";
        List<MajorType> types = Lists.newArrayList();
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
          types.add(ref.getType());
        }

        String[] names = holder.getRegisteredNames();
        for (String name : names) {
          String functionName = name.toLowerCase();
          String functionSignature = functionName + functionInput;

          if (allFuncWithSignatures.get(functionName).contains(functionSignature)) {
            throw new FunctionValidationException(
                String.format("Conflicting function with similar signature found. " +
                        "Function name: %s, class name: %s, input parameters : %s",
                    functionName, func.getClassName(), functionInput));
          } else if (holder.isAggregating() && !holder.isDeterministic()) {
            throw new FunctionValidationException(
                String.format("Aggregate functions must be deterministic: %s", func.getClassName()));
          } else {
            functions.add(Func.newBuilder().setName(functionName).addAllMajorType(types).build());
            allFuncWithSignatures.put(functionName, functionSignature);
          }
        }
      } else {
        logger.warn("Unable to initialize function for class {}", func.getClassName());
      }
    }
    return functions;
  }

  /**
   * Registers all functions present in jar.
   * If jar name is already registered, all jar related functions are overridden.
   */
  public void register(String jarName, ScanResult classpathScan, ClassLoader classloader) {
    FunctionConverter converter = new FunctionConverter();
    List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();
    Map<String, Pair<String, DrillFuncHolder>> functions = Maps.newHashMap();
    for (AnnotatedClassDescriptor func : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(func, classloader);
      if (holder != null) {
        String functionInput = "";
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
        }

        String[] names = holder.getRegisteredNames();
        for (String name : names) {
          String functionName = name.toLowerCase();
          String functionSignature = functionName + functionInput;
          functions.put(functionSignature, new ImmutablePair<>(functionName, holder));
        }
      }
    }
    registryHolder.addJar(jarName, functions);
  }

  /**
   * Removes all function associated with the given jar name.
   * Functions marked as built-in is not allowed to be unregistered.
   * Jar name is case-sensitive.
   */
  public void unregister(String jarName) {
    if (BUILT_IN.equals(jarName)) {
      logger.warn("Functions marked as built-in are not allowed to be unregistered.");
      return;
    }
    registryHolder.removeJar(jarName);
  }

  /**
   * @return list of jar names registered in function registry
   */
  public List<String> getAllJarNames() {
    return registryHolder.getAllJarNames();
  }

  /**
   * @return quantity of all registered functions
   */
  public int size(){
    return registryHolder.functionsSize();
  }

  /**
   *  @return all function holders associated with the function name. Function name is case insensitive.
   */
  public List<DrillFuncHolder> getMethods(String name) {
    return registryHolder.getHoldersByFunctionName(name.toLowerCase());
  }

  public void register(DrillOperatorTable operatorTable) {
    registerOperatorsWithInference(operatorTable);
    registerOperatorsWithoutInference(operatorTable);
  }

  private void registerOperatorsWithInference(DrillOperatorTable operatorTable) {
    final Map<String, DrillSqlOperator.DrillSqlOperatorBuilder> map = Maps.newHashMap();
    final Map<String, DrillSqlAggOperator.DrillSqlAggOperatorBuilder> mapAgg = Maps.newHashMap();
    for (Entry<String, Collection<DrillFuncHolder>> function : registryHolder.getAllFunctionsWithHolders().asMap().entrySet()) {
      final ArrayListMultimap<Pair<Integer, Integer>, DrillFuncHolder> functions = ArrayListMultimap.create();
      final ArrayListMultimap<Integer, DrillFuncHolder> aggregateFunctions = ArrayListMultimap.create();
      final String name = function.getKey().toUpperCase();
      boolean isDeterministic = true;
      for (DrillFuncHolder func : function.getValue()) {
        final int paramCount = func.getParamCount();
        if(func.isAggregating()) {
          aggregateFunctions.put(paramCount, func);
        } else {
          final Pair<Integer, Integer> argNumberRange;
          if(registeredFuncNameToArgRange.containsKey(name)) {
            argNumberRange = registeredFuncNameToArgRange.get(name);
          } else {
            argNumberRange = Pair.of(func.getParamCount(), func.getParamCount());
          }
          functions.put(argNumberRange, func);
        }

        if(!func.isDeterministic()) {
          isDeterministic = false;
        }
      }
      for (Entry<Pair<Integer, Integer>, Collection<DrillFuncHolder>> entry : functions.asMap().entrySet()) {
        final Pair<Integer, Integer> range = entry.getKey();
        final int max = range.getRight();
        final int min = range.getLeft();
        if(!map.containsKey(name)) {
          map.put(name, new DrillSqlOperator.DrillSqlOperatorBuilder()
              .setName(name));
        }

        final DrillSqlOperator.DrillSqlOperatorBuilder drillSqlOperatorBuilder = map.get(name);
        drillSqlOperatorBuilder
            .addFunctions(entry.getValue())
            .setArgumentCount(min, max)
            .setDeterministic(isDeterministic);
      }
      for (Entry<Integer, Collection<DrillFuncHolder>> entry : aggregateFunctions.asMap().entrySet()) {
        if(!mapAgg.containsKey(name)) {
          mapAgg.put(name, new DrillSqlAggOperator.DrillSqlAggOperatorBuilder().setName(name));
        }

        final DrillSqlAggOperator.DrillSqlAggOperatorBuilder drillSqlAggOperatorBuilder = mapAgg.get(name);
        drillSqlAggOperatorBuilder
            .addFunctions(entry.getValue())
            .setArgumentCount(entry.getKey(), entry.getKey());
      }
    }

    for(final Entry<String, DrillSqlOperator.DrillSqlOperatorBuilder> entry : map.entrySet()) {
      operatorTable.addOperatorWithInference(
          entry.getKey(),
          entry.getValue().build());
    }

    for(final Entry<String, DrillSqlAggOperator.DrillSqlAggOperatorBuilder> entry : mapAgg.entrySet()) {
      operatorTable.addOperatorWithInference(
          entry.getKey(),
          entry.getValue().build());
    }
  }

  private void registerOperatorsWithoutInference(DrillOperatorTable operatorTable) {
    SqlOperator op;
    for (Entry<String, Collection<DrillFuncHolder>> function : registryHolder.getAllFunctionsWithHolders().asMap().entrySet()) {
      Set<Integer> argCounts = Sets.newHashSet();
      String name = function.getKey().toUpperCase();
      for (DrillFuncHolder func : function.getValue()) {
        if (argCounts.add(func.getParamCount())) {
          if (func.isAggregating()) {
            op = new DrillSqlAggOperatorWithoutInference(name, func.getParamCount());
          } else {
            boolean isDeterministic;
            // prevent Drill from folding constant functions with types that cannot be materialized
            // into literals
            if (DrillConstExecutor.NON_REDUCIBLE_TYPES.contains(func.getReturnType().getMinorType())) {
              isDeterministic = false;
            } else {
              isDeterministic = func.isDeterministic();
            }
            op = new DrillSqlOperatorWithoutInference(name, func.getParamCount(), func.getReturnType(), isDeterministic);
          }
          operatorTable.addOperatorWithoutInference(function.getKey(), op);
        }
      }
    }
  }

  /**
   * Function registry holder. Stores function implementations by jar name, function name.
   */
  private class GenericRegistryHolder<T, U> {
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final AutoCloseableLock readLock = new AutoCloseableLock(readWriteLock.readLock());
    private final AutoCloseableLock writeLock = new AutoCloseableLock(readWriteLock.writeLock());

    // jar name, Map<function name, List<signature>
    private final Map<T, Map<T, List<T>>> jars;

    // function name, Map<signature, function holder>
    private final Map<T, Map<T, U>> functions;

    public GenericRegistryHolder() {
      this.functions = Maps.newHashMap();
      this.jars = Maps.newHashMap();
    }

    public void addJar(T jName, Map<T, Pair<T, U>> sNameMap) {
      try (AutoCloseableLock lock = writeLock.open()) {
        Map<T, List<T>> map = jars.get(jName);
        if (map != null) {
          removeAllByJar(jName);
        }
        map = Maps.newHashMap();
        jars.put(jName, map);

        for (Entry<T, Pair<T, U>> entry : sNameMap.entrySet()) {
          T sName = entry.getKey();
          Pair<T, U> pair = entry.getValue();
          addFunction(jName, pair.getKey(), sName, pair.getValue());
        }
      }
    }

    public void removeJar(T jName) {
      try (AutoCloseableLock lock = writeLock.open()) {
        removeAllByJar(jName);
      }
    }

    public List<T> getAllJarNames() {
      try (AutoCloseableLock lock = readLock.open()) {
        return Lists.newArrayList(jars.keySet());
      }
    }

    public List<T> getAllFunctionNames(T jName) {
      try  (AutoCloseableLock lock = readLock.open()){
        Map<T, List<T>> map = jars.get(jName);
        return map == null ? Lists.<T>newArrayList() : Lists.newArrayList(map.keySet());
      }
    }

    public ListMultimap<T, U> getAllFunctionsWithHolders() {
      try (AutoCloseableLock lock = readLock.open()) {
        ListMultimap<T, U> multimap = ArrayListMultimap.create();
        for (Entry<T, Map<T, U>> entry : functions.entrySet()) {
          multimap.putAll(entry.getKey(), Lists.newArrayList(entry.getValue().values()));
        }
        return multimap;
      }
    }

    public ListMultimap<T, T> getAllFunctionsWithSignatures() {
      try (AutoCloseableLock lock = readLock.open()) {
        ListMultimap<T, T> multimap = ArrayListMultimap.create();
        for (Entry<T, Map<T, U>> entry : functions.entrySet()) {
          multimap.putAll(entry.getKey(), Lists.newArrayList(entry.getValue().keySet()));
        }
        return multimap;
      }
    }

    public List<U> getHoldersByFunctionName(T fName) {
      try (AutoCloseableLock lock = readLock.open()) {
        Map<T, U> map = functions.get(fName);
        return map == null ? Lists.<U>newArrayList() : Lists.newArrayList(map.values());
      }
    }

    public boolean containsJar(T jName) {
      try (AutoCloseableLock lock = readLock.open()) {
        return jars.containsKey(jName);
      }
    }

    public int functionsSize() {
      try (AutoCloseableLock lock = readLock.open()) {
        return functions.size();
      }
    }

    private void addFunction(T jName, T fName, T sName, U fHolder) {
      Map<T, List<T>> map = jars.get(jName);

      List<T> list = map.get(fName);
      if (list == null) {
        list = Lists.newArrayList();
        map.put(fName, list);
      }

      if (!list.contains(sName)) {
        list.add(sName);

        Map<T, U> sigsMap = functions.get(fName);
        if (sigsMap == null) {
          sigsMap = Maps.newHashMap();
          functions.put(fName, sigsMap);
        }

        U u = sigsMap.get(sName);
        if (u == null) {
          sigsMap.put(sName, fHolder);
        }
      }
    }

    private void removeAllByJar(T jName) {
      Map<T, List<T>> removeMap = jars.remove(jName);
      if (removeMap == null) {
        return;
      }

      for (Map.Entry<T, List<T>> remEntry : removeMap.entrySet()) {
        Map<T, U> fNameMap = functions.get(remEntry.getKey());
        List<T> value = remEntry.getValue();
        fNameMap.keySet().removeAll(value);
        if (fNameMap.isEmpty()) {
          functions.remove(remEntry.getKey());
        }
      }
    }

  }
}
