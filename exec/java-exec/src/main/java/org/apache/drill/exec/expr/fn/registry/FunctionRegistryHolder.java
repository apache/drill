/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.registry;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.drill.common.concurrent.AutoCloseableLock;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Function registry holder stores function implementations by jar name, function name.
 * Contains two maps that hold data by jars and functions respectively.
 * Jars map contains each jar as a key and map of all its functions with collection of function signatures as value.
 * Functions map contains function name as key and map of its signatures and function holder as value.
 * All maps and collections used are concurrent to guarantee memory consistency effects.
 * Such structure is chosen to achieve maximum speed while retrieving data by jar or by function name,
 * since we expect infrequent registry changes.
 * Holder is designed to allow concurrent reads and single writes to keep data consistent.
 * This is achieved by {@link ReadWriteLock} implementation usage.
 * Holder has number version which indicates remote function registry version number it is in sync with.
 *
 * Structure example:
 *
 * JARS
 * built-in   -> upper          -> upper(VARCHAR-REQUIRED)
 *            -> lower          -> lower(VARCHAR-REQUIRED)
 *
 * First.jar  -> upper          -> upper(VARCHAR-OPTIONAL)
 *            -> custom_upper   -> custom_upper(VARCHAR-REQUIRED)
 *                              -> custom_upper(VARCHAR-OPTIONAL)
 *
 * Second.jar -> lower          -> lower(VARCHAR-OPTIONAL)
 *            -> custom_upper   -> custom_upper(VARCHAR-REQUIRED)
 *                              -> custom_upper(VARCHAR-OPTIONAL)
 *
 * FUNCTIONS
 * upper        -> upper(VARCHAR-REQUIRED)        -> function holder for upper(VARCHAR-REQUIRED)
 *              -> upper(VARCHAR-OPTIONAL)        -> function holder for upper(VARCHAR-OPTIONAL)
 *
 * lower        -> lower(VARCHAR-REQUIRED)        -> function holder for lower(VARCHAR-REQUIRED)
 *              -> lower(VARCHAR-OPTIONAL)        -> function holder for lower(VARCHAR-OPTIONAL)
 *
 * custom_upper -> custom_upper(VARCHAR-REQUIRED) -> function holder for custom_upper(VARCHAR-REQUIRED)
 *              -> custom_upper(VARCHAR-OPTIONAL) -> function holder for custom_upper(VARCHAR-OPTIONAL)
 *
 * custom_lower -> custom_lower(VARCHAR-REQUIRED) -> function holder for custom_lower(VARCHAR-REQUIRED)
 *              -> custom_lower(VARCHAR-OPTIONAL) -> function holder for custom_lower(VARCHAR-OPTIONAL)
 *
 * where
 * First.jar is jar name represented by String
 * upper is function name represented by String
 * upper(VARCHAR-REQUIRED) is signature name represented by String which consist of function name, list of input parameters
 * function holder for upper(VARCHAR-REQUIRED) is {@link DrillFuncHolder} initiated for each function.
 *
 */
public class FunctionRegistryHolder {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionRegistryHolder.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final AutoCloseableLock readLock = new AutoCloseableLock(readWriteLock.readLock());
  private final AutoCloseableLock writeLock = new AutoCloseableLock(readWriteLock.writeLock());
  // remote function registry number, it is in sync with
  private long version;

  // jar name, Map<function name, Queue<function signature>
  private final Map<String, Map<String, Queue<String>>> jars;

  // function name, Map<function signature, function holder>
  private final Map<String, Map<String, DrillFuncHolder>> functions;

  public FunctionRegistryHolder() {
    this.functions = Maps.newConcurrentMap();
    this.jars = Maps.newConcurrentMap();
  }

  /**
   * This is read operation, so several users at a time can get this data.
   * @return local function registry version number
   */
  public long getVersion() {
    try (AutoCloseableLock lock = readLock.open()) {
      return version;
    }
  }

  /**
   * Adds jars to the function registry.
   * If jar with the same name already exists, it and its functions will be removed.
   * Then jar will be added to {@link #jars}
   * and each function will be added using {@link #addFunctions(Map, List)}.
   * Registry version is updated with passed version if all jars were added successfully.
   * This is write operation, so one user at a time can call perform such action,
   * others will wait till first user completes his action.
   *
   * @param newJars jars and list of their function holders, each contains function name, signature and holder
   */
  public void addJars(Map<String, List<FunctionHolder>> newJars, long version) {
    try (AutoCloseableLock lock = writeLock.open()) {
      for (Map.Entry<String, List<FunctionHolder>> newJar : newJars.entrySet()) {
        String jarName = newJar.getKey();
        removeAllByJar(jarName);
        Map<String, Queue<String>> jar = Maps.newConcurrentMap();
        jars.put(jarName, jar);
        addFunctions(jar, newJar.getValue());
      }
      this.version = version;
    }
  }

  /**
   * Removes jar from {@link #jars} and all associated with jar functions from {@link #functions}
   * This is write operation, so one user at a time can call perform such action,
   * others will wait till first user completes his action.
   *
   * @param jarName jar name to be removed
   */
  public void removeJar(String jarName) {
    try (AutoCloseableLock lock = writeLock.open()) {
      removeAllByJar(jarName);
    }
  }

  /**
   * Retrieves list of all jars name present in {@link #jars}
   * This is read operation, so several users can get this data.
   *
   * @return list of all jar names
   */
  public List<String> getAllJarNames() {
    try (AutoCloseableLock lock = readLock.open()) {
      return Lists.newArrayList(jars.keySet());
    }
  }

  /**
   * Retrieves all function names associated with the jar from {@link #jars}.
   * Returns empty list if jar is not registered.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param jarName jar name
   * @return list of functions names associated from the jar
   */
  public List<String> getFunctionNamesByJar(String jarName) {
    try  (AutoCloseableLock lock = readLock.open()){
      Map<String, Queue<String>> functions = jars.get(jarName);
      return functions == null ? Lists.<String>newArrayList() : Lists.newArrayList(functions.keySet());
    }
  }

  /**
   * Returns list of functions with list of function holders for each functions.
   * Uses guava {@link ListMultimap} structure to return data.
   * If no functions present, will return empty {@link ListMultimap}.
   * If version holder is not null, updates it with current registry version number.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param version version holder
   * @return all functions which their holders
   */
  public ListMultimap<String, DrillFuncHolder> getAllFunctionsWithHolders(AtomicLong version) {
    try (AutoCloseableLock lock = readLock.open()) {
      if (version != null) {
        version.set(this.version);
      }
      ListMultimap<String, DrillFuncHolder> functionsWithHolders = ArrayListMultimap.create();
      for (Map.Entry<String, Map<String, DrillFuncHolder>> function : functions.entrySet()) {
        functionsWithHolders.putAll(function.getKey(), Lists.newArrayList(function.getValue().values()));
      }
      return functionsWithHolders;
    }
  }

  /**
   * Returns list of functions with list of function holders for each functions without version number.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @return all functions which their holders
   */
  public ListMultimap<String, DrillFuncHolder> getAllFunctionsWithHolders() {
    return getAllFunctionsWithHolders(null);
  }

  /**
   * Returns list of functions with list of function signatures for each functions.
   * Uses guava {@link ListMultimap} structure to return data.
   * If no functions present, will return empty {@link ListMultimap}.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @return all functions which their signatures
   */
  public ListMultimap<String, String> getAllFunctionsWithSignatures() {
    try (AutoCloseableLock lock = readLock.open()) {
      ListMultimap<String, String> functionsWithSignatures = ArrayListMultimap.create();
      for (Map.Entry<String, Map<String, DrillFuncHolder>> function : functions.entrySet()) {
        functionsWithSignatures.putAll(function.getKey(), Lists.newArrayList(function.getValue().keySet()));
      }
      return functionsWithSignatures;
    }
  }

  /**
   * Returns all function holders associated with function name.
   * If function is not present, will return empty list.
   * If version holder is not null, updates it with current registry version number.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param functionName function name
   * @param version version holder
   * @return list of function holders
   */
  public List<DrillFuncHolder> getHoldersByFunctionName(String functionName, AtomicLong version) {
    try (AutoCloseableLock lock = readLock.open()) {
      if (version != null) {
        version.set(this.version);
      }
      Map<String, DrillFuncHolder> holders = functions.get(functionName);
      return holders == null ? Lists.<DrillFuncHolder>newArrayList() : Lists.newArrayList(holders.values());
    }
  }

  /**
   * Returns all function holders associated with function name without version number.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param functionName function name
   * @return list of function holders
   */
  public List<DrillFuncHolder> getHoldersByFunctionName(String functionName) {
    return getHoldersByFunctionName(functionName, null);
  }

  /**
   * Checks is jar is present in {@link #jars}.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param jarName jar name
   * @return true if jar exists, else false
   */
  public boolean containsJar(String jarName) {
    try (AutoCloseableLock lock = readLock.open()) {
      return jars.containsKey(jarName);
    }
  }

  /**
   * Returns quantity of functions stored in {@link #functions}.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @return quantity of functions
   */
  public int functionsSize() {
    try (AutoCloseableLock lock = readLock.open()) {
      return functions.size();
    }
  }

  /**
   * Looks which jar in {@link #jars} contains passed function signature.
   * First looks by function name and if found checks if such function has passed function signature.
   * Returns jar name if found matching function signature, else null.
   * This is read operation, so several users can perform this operation at the same time.
   *
   * @param functionName function name
   * @param functionSignature function signature
   * @return jar name
   */
  public String getJarNameByFunctionSignature(String functionName, String functionSignature) {
    try (AutoCloseableLock lock = readLock.open()) {
      for (Map.Entry<String, Map<String, Queue<String>>> jar : jars.entrySet()) {
        Queue<String> functionSignatures = jar.getValue().get(functionName);
        if (functionSignatures != null && functionSignatures.contains(functionSignature)) {
          return jar.getKey();
        }
      }
    }
    return null;
  }

  /**
   * Adds all function names and signatures to passed jar,
   * adds all function names, their signatures and holders to {@link #functions}.
   *
   * @param jar jar where function to be added
   * @param newFunctions collection of function holders, each contains function name, signature and holder.
   */
  private void addFunctions(Map<String, Queue<String>> jar, List<FunctionHolder> newFunctions) {
    for (FunctionHolder function : newFunctions) {
      final String functionName = function.getName();
      Queue<String> jarFunctions = jar.get(functionName);
      if (jarFunctions == null) {
        jarFunctions = Queues.newConcurrentLinkedQueue();;
        jar.put(functionName, jarFunctions);
      }
      final String functionSignature = function.getSignature();
      jarFunctions.add(functionSignature);

      Map<String, DrillFuncHolder> signatures = functions.get(functionName);
      if (signatures == null) {
        signatures = Maps.newConcurrentMap();
        functions.put(functionName, signatures);
      }
      signatures.put(functionSignature, function.getHolder());
    }
  }

  /**
   * Removes jar from {@link #jars} and all associated with jars functions from {@link #functions}
   * Since each jar is loaded with separate class loader before
   * removing we need to close class loader to release opened connection to jar.
   * All jar functions have the same class loader, so we need to close only one time.
   *
   * @param jarName jar name to be removed
   */
  private void removeAllByJar(String jarName) {
    Map<String, Queue<String>> jar = jars.remove(jarName);
    if (jar == null) {
      return;
    }

    for (Map.Entry<String, Queue<String>> functionEntry : jar.entrySet()) {
      final String function = functionEntry.getKey();
      Map<String, DrillFuncHolder> functionHolders = functions.get(function);
      Queue<String> functionSignatures = functionEntry.getValue();
      for (Map.Entry<String, DrillFuncHolder> entry : functionHolders.entrySet()) {
        if (functionSignatures.contains(entry.getKey())) {
          ClassLoader classLoader = entry.getValue().getClassLoader();
          if (classLoader instanceof AutoCloseable) {
            try {
              ((AutoCloseable) classLoader).close();
            } catch (Exception e) {
              logger.warn("Problem during closing class loader", e);
            }
          }
          break;
        }
      }
      functionHolders.keySet().removeAll(functionSignatures);

      if (functionHolders.isEmpty()) {
        functions.remove(function);
      }
    }
  }
}
