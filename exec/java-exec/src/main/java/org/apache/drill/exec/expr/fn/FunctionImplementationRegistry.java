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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.fn.CastFunctions;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.RunTimeScan;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.store.TransientStoreEvent;
import org.apache.drill.exec.coord.store.TransientStoreEventType;
import org.apache.drill.exec.coord.store.TransientStoreListener;
import org.apache.drill.exec.exception.FunctionValidationException;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.UserBitShared.Jar;
import org.apache.drill.exec.proto.UserBitShared.Registry;
import org.apache.drill.exec.proto.UserBitShared.Func;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.server.options.OptionManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.drill.exec.util.JarUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class offers the registry for functions. Notably, in addition to Drill its functions
 * (in {@link DrillFunctionRegistry}), other PluggableFunctionRegistry (e.g., {@link org.apache.drill.exec.expr.fn.HiveFunctionRegistry})
 * is also registered in this class
 */
public class FunctionImplementationRegistry implements FunctionLookupContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);

  private final DrillFunctionRegistry drillFuncRegistry;
  private final RemoteFunctionRegistry remoteFunctionRegistry;
  private List<PluggableFunctionRegistry> pluggableFuncRegistries = Lists.newArrayList();
  private OptionManager optionManager = null;

  @Deprecated @VisibleForTesting
  public FunctionImplementationRegistry(DrillConfig config){
    this(config, ClassPathScanner.fromPrescan(config));
  }

  public FunctionImplementationRegistry(DrillConfig config, ScanResult classpathScan){
    Stopwatch w = Stopwatch.createStarted();

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
    this.remoteFunctionRegistry = new RemoteFunctionRegistry(new UnregistrationListener());
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
   * If function implementation was not found and in case if Dynamic UDF Support is enabled
   * loads all missing remote functions and tries to find Drill implementation one more time.
   */
  @Override
  public DrillFuncHolder findDrillFunction(FunctionResolver functionResolver, FunctionCall functionCall) {
    return findDrillFunction(functionResolver, functionCall, true);
  }

  private DrillFuncHolder findDrillFunction(FunctionResolver functionResolver, FunctionCall functionCall, boolean retry) {
    DrillFuncHolder holder = functionResolver.getBestMatch(drillFuncRegistry.getMethods(functionReplacement(functionCall)), functionCall);
    if (holder == null && retry) {
      if (optionManager != null && optionManager.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {
        if (loadRemoteFunctions()) {
          findDrillFunction(functionResolver, functionCall, false);
        }
      }
    }
    return holder;
  }

  // Check if this Function Replacement is needed; if yes, return a new name. otherwise, return the original name
  private String functionReplacement(FunctionCall functionCall) {
    String funcName = functionCall.getName();
      if (functionCall.args.size() > 0) {
          MajorType majorType =  functionCall.args.get(0).getMajorType();
          DataMode dataMode = majorType.getMode();
          MinorType minorType = majorType.getMinorType();
          if (optionManager != null
              && optionManager.getOption(ExecConstants.CAST_TO_NULLABLE_NUMERIC).bool_val
              && CastFunctions.isReplacementNeeded(funcName, minorType)) {
              funcName = CastFunctions.getReplacingCastFunction(funcName, dataMode, minorType);
          }
      }

    return funcName;
  }

  /**
   * Find the Drill function implementation that matches the name, arg types and return type.
   * If exact function implementation was not found and in case if Dynamic UDF Support is enabled
   * loads all missing remote functions and tries to find Drill implementation one more time.
   */
  public DrillFuncHolder findExactMatchingDrillFunction(String name, List<MajorType> argTypes, MajorType returnType) {
    return findExactMatchingDrillFunction(name, argTypes, returnType, true);
  }

  private DrillFuncHolder findExactMatchingDrillFunction(String name, List<MajorType> argTypes, MajorType returnType, boolean retry) {
    for (DrillFuncHolder h : drillFuncRegistry.getMethods(name)) {
      if (h.matches(returnType, argTypes)) {
        return h;
      }
    }

    if (retry && optionManager != null && optionManager.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {
      if (loadRemoteFunctions()) {
        findExactMatchingDrillFunction(name, argTypes, returnType, false);
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

  public RemoteFunctionRegistry getRemoteFunctionRegistry() {
    return remoteFunctionRegistry;
  }

  public List<Func> validate(Path path) throws IOException {
    URL url = path.toUri().toURL();
    URL[] urls = {url};
    ClassLoader classLoader = new URLClassLoader(urls);
    return drillFuncRegistry.validate(path.getName(), scan(classLoader, path, urls));
  }

  public void register(String jarName, ScanResult classpathScan, ClassLoader classLoader) {
    drillFuncRegistry.register(jarName, classpathScan, classLoader);
  }

  public void unregister(String jarName) {
    drillFuncRegistry.unregister(jarName);
  }

  /**
   * Loads all missing functions from remote registry.
   * Compares list of already registered jars and remote jars, loads missing jars.
   * Missing jars are stores in local DRILL_UDF_DIR.
   *
   * @return true if at least functions from one jar were loaded
   */
  public boolean loadRemoteFunctions() {
    List<String> missingJars = Lists.newArrayList();
    Registry registry = remoteFunctionRegistry.getRegistry();

    List<String> localJars = drillFuncRegistry.getAllJarNames();
    for (Jar jar : registry.getJarList()) {
      if (!localJars.contains(jar.getName())) {
        missingJars.add(jar.getName());
      }
    }

    for (String jarName : missingJars) {
      try {
        Path localUdfArea = new Path(new File(getUdfDir()).toURI());
        Path registryArea = remoteFunctionRegistry.getRegistryArea();
        FileSystem fs = remoteFunctionRegistry.getFs();

        String sourceName = JarUtil.getSourceName(jarName);

        Path remoteBinary = new Path(registryArea, jarName);
        Path remoteSource = new Path(registryArea, sourceName);

        Path binary = new Path(localUdfArea, jarName);
        Path source = new Path(localUdfArea, sourceName);

        fs.copyToLocalFile(remoteBinary, binary);
        fs.copyToLocalFile(remoteSource, source);

        URL[] urls = {binary.toUri().toURL(), source.toUri().toURL()};
        ClassLoader classLoader = new URLClassLoader(urls);
        ScanResult scanResult = scan(classLoader, binary, urls);
        drillFuncRegistry.register(jarName, scanResult, classLoader);
      } catch (IOException | FunctionValidationException e) {
        logger.error("Problem during remote functions load from {}", jarName, e);
      }
    }

    return missingJars.size() > 0;
  }

  private ScanResult scan(ClassLoader classLoader, Path path, URL[] urls) throws IOException {
    Enumeration<URL> e = classLoader.getResources(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME);
    while (e.hasMoreElements()) {
      URL res = e.nextElement();
      if (res.getPath().contains(path.toUri().getPath())) {
        DrillConfig drillConfig = DrillConfig.create(ConfigFactory.parseURL(res));
        return RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(urls));
      }
    }
    throw new FunctionValidationException(String.format("Marker file %s is missing in %s.",
        CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, path.getName()));
  }

  private static String getUdfDir() {
    return Preconditions.checkNotNull(System.getenv("DRILL_UDF_DIR"), "DRILL_UDF_DIR variable is not set");
  }

  /**
   * Fires when jar name is submitted for unregistration.
   * Will unregister all functions associated with the jar name
   * and delete binary and source associated with the jar from local DRILL_UDF_DIR.
   */
  public class UnregistrationListener implements TransientStoreListener {

    @Override
    public void onChange(TransientStoreEvent event) {
        String jarName = (String) event.getValue();
        String sourceName = JarUtil.getSourceName(jarName);
        String localDir = getUdfDir();
        FileUtils.deleteQuietly(new File(localDir, jarName));
        FileUtils.deleteQuietly(new File(localDir, sourceName));
        drillFuncRegistry.unregister(jarName);
    }
  }

}
