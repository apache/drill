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
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
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
import org.apache.drill.exec.coord.store.TransientStoreListener;
import org.apache.drill.exec.exception.FunctionValidationException;
import org.apache.drill.exec.exception.JarValidationException;
import org.apache.drill.exec.expr.fn.registry.LocalFunctionRegistry;
import org.apache.drill.exec.expr.fn.registry.JarScan;
import org.apache.drill.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.UserBitShared.Jar;
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
 * (in {@link LocalFunctionRegistry}), other PluggableFunctionRegistry (e.g., {@link org.apache.drill.exec.expr.fn.HiveFunctionRegistry})
 * is also registered in this class
 */
public class FunctionImplementationRegistry implements FunctionLookupContext {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);
  private static final String jar_suffix_pattern = "_(\\d+)\\.jar";
  private static final String generated_jar_name_pattern = "%s_%s.%s";

  private final LocalFunctionRegistry localFunctionRegistry;
  private final RemoteFunctionRegistry remoteFunctionRegistry;
  private final Path localUdfDir;
  private List<PluggableFunctionRegistry> pluggableFuncRegistries = Lists.newArrayList();
  private OptionManager optionManager = null;

  @Deprecated @VisibleForTesting
  public FunctionImplementationRegistry(DrillConfig config){
    this(config, ClassPathScanner.fromPrescan(config));
  }

  public FunctionImplementationRegistry(DrillConfig config, ScanResult classpathScan){
    Stopwatch w = Stopwatch.createStarted();

    logger.debug("Generating function registry.");
    localFunctionRegistry = new LocalFunctionRegistry(classpathScan);

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
    logger.info("Function registry loaded.  {} functions loaded in {} ms.", localFunctionRegistry.size(), w.elapsed(TimeUnit.MILLISECONDS));
    this.remoteFunctionRegistry = new RemoteFunctionRegistry(new UnregistrationListener());
    this.localUdfDir = getLocalUdfDir();
  }

  public FunctionImplementationRegistry(DrillConfig config, ScanResult classpathScan, OptionManager optionManager) {
    this(config, classpathScan);
    this.optionManager = optionManager;
  }

  /**
   * Register functions in given operator table.
   * @param operatorTable
   */
  public void register(DrillOperatorTable operatorTable, AtomicLong version) {
    // Register Drill functions first and move to pluggable function registries.
    localFunctionRegistry.register(operatorTable, version);

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
    AtomicLong version = new AtomicLong();
    DrillFuncHolder holder = functionResolver.getBestMatch(
        localFunctionRegistry.getMethods(functionReplacement(functionCall), version), functionCall);
    if (holder == null && retry) {
      if (optionManager != null && optionManager.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {
        if (loadRemoteFunctions(version.get())) {
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
    AtomicLong version = new AtomicLong();
    for (DrillFuncHolder h : localFunctionRegistry.getMethods(name, version)) {
      if (h.matches(returnType, argTypes)) {
        return h;
      }
    }

    if (retry && optionManager != null && optionManager.getOption(ExecConstants.DYNAMIC_UDF_SUPPORT_ENABLED).bool_val) {
      if (loadRemoteFunctions(version.get())) {
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
    List<DrillFuncHolder> methods = localFunctionRegistry.getMethods(name);
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

  /**
   * Using given local path to jar creates unique class loader for this jar.
   * Scan jar content to receive list of all scanned classes
   * and starts validation process against local function registry.
   * Checks if received list of validated function is not empty.
   *
   * @param path local path to jar we need to validate
   * @return list of validated function signatures
   */
  public List<String> validate(Path path) throws IOException {
    URL url = path.toUri().toURL();
    URL[] urls = {url};
    ClassLoader classLoader = new URLClassLoader(urls);
    ScanResult jarScanResult = scan(classLoader, path, urls);
    List<String> functions = localFunctionRegistry.validate(path.getName(), jarScanResult);
    if (functions.isEmpty()) {
      throw new FunctionValidationException(String.format("Jar %s does not contain functions", path.getName()));
    }
    return functions;
  }

  /**
   * Attempts to load and register functions from remote function registry.
   * First checks if there is no missing jars.
   * If yes, enters synchronized block to prevent other loading the same jars.
   * Again re-checks if there are no missing jars in case someone has already loaded them (double-check lock).
   * If there are still missing jars, first copies jars to local udf area and prepares {@link JarScan} for each jar.
   * Then registers all jars at the same time. Returns true when finished.
   * In case if any errors during jars coping or registration, logs errors and proceeds.
   *
   * If no missing jars are found, checks current local registry version.
   * Returns false if versions match, true otherwise.
   *
   * @param version local function registry version
   * @return true if new jars were registered or local function registry version is different, false otherwise
   */
  public boolean loadRemoteFunctions(long version) {
    List<String> missingJars = getMissingJars(remoteFunctionRegistry, localFunctionRegistry);
    if (!missingJars.isEmpty()) {
      synchronized (this) {
        missingJars = getMissingJars(remoteFunctionRegistry, localFunctionRegistry);
        List<JarScan> jars = Lists.newArrayList();
        for (String jarName : missingJars) {
          Path binary = null;
          Path source = null;
          try {
            long suffix = System.nanoTime();
            binary = copyJarToLocal(jarName, remoteFunctionRegistry, suffix);
            source = copyJarToLocal(JarUtil.getSourceName(jarName), remoteFunctionRegistry, suffix);
            URL[] urls = {binary.toUri().toURL(), source.toUri().toURL()};
            ClassLoader classLoader = new URLClassLoader(urls);
            ScanResult scanResult = scan(classLoader, binary, urls);
            localFunctionRegistry.validate(jarName, scanResult);
            jars.add(new JarScan(jarName, scanResult, classLoader));
          } catch (Exception e) {
            deleteQuietlyLocalJar(binary);
            deleteQuietlyLocalJar(source);
            logger.error("Problem during remote functions load from {}", jarName, e);
          }
        }
        if (!jars.isEmpty()) {
          localFunctionRegistry.register(jars);
          return true;
        }
      }
    }
    return version != localFunctionRegistry.getVersion();
  }

  /**
   * First finds path to marker file url, otherwise throws {@link JarValidationException}.
   * Then scans jar classes according to list indicated in marker files.
   *
   * @param classLoader unique class loader for jar
   * @param path local path to jar
   * @param urls urls associated with the jar (ex: binary and source)
   * @return scan result of packages, classes, annotations found in jar
   */
  private ScanResult scan(ClassLoader classLoader, Path path, URL[] urls) throws IOException {
    Enumeration<URL> e = classLoader.getResources(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME);
    while (e.hasMoreElements()) {
      URL res = e.nextElement();
      if (res.getPath().contains(path.toUri().getPath())) {
        DrillConfig drillConfig = DrillConfig.create(ConfigFactory.parseURL(res));
        return RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(urls));
      }
    }
    throw new JarValidationException(String.format("Marker file %s is missing in %s",
        CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, path.getName()));
  }

  /**
   * Return list of jars that are missing in local function registry
   * but present in remote function registry.
   *
   * @param remoteFunctionRegistry remote function registry
   * @param localFunctionRegistry local function registry
   * @return list of missing jars
   */
  private List<String> getMissingJars(RemoteFunctionRegistry remoteFunctionRegistry,
                                      LocalFunctionRegistry localFunctionRegistry) {
    List<Jar> remoteJars = remoteFunctionRegistry.getRegistry().getJarList();
    List<String> localJars = localFunctionRegistry.getAllJarNames();
    List<String> missingJars = Lists.newArrayList();
    for (Jar jar : remoteJars) {
      if (!localJars.contains(jar.getName())) {
        missingJars.add(jar.getName());
      }
    }
    return missingJars;
  }

  /**
   * Creates local udf directory, if it doesn't exist.
   * Checks if local is a directory and if current application has write rights on it.
   * Attempts to clean up local idf directory in case jars were left after previous drillbit run.
   *
   * @return path to local udf directory
   */
  private Path getLocalUdfDir() {
    String confDir = getConfDir();
    File udfDir = new File(confDir, "udf");
    String udfPath = udfDir.getPath();
    udfDir.mkdirs();
    Preconditions.checkState(udfDir.exists(), "Local udf directory [%s] must exist", udfPath);
    Preconditions.checkState(udfDir.isDirectory(), "Local udf directory [%s] must be a directory", udfPath);
    Preconditions.checkState(udfDir.canWrite(), "Local udf directory [%s] must be writable for application user", udfPath);
    try {
      FileUtils.cleanDirectory(udfDir);
    } catch (IOException e) {
      throw new DrillRuntimeException("Error during local udf directory clean up", e);
    }
    return new Path(udfDir.toURI());
  }

  /**
   * First tries to get drill conf directory value from system properties,
   * if value is missing, checks environment properties.
   * Throws exception is value is null.
   * @return drill conf dir path
   */
  private String getConfDir() {
    String drillConfDir = "DRILL_CONF_DIR";
    String value = System.getProperty(drillConfDir);
    if (value == null) {
      value = Preconditions.checkNotNull(System.getenv(drillConfDir), "%s variable is not set", drillConfDir);
    }
    return value;
  }

  /**
   * Copies jar from remote udf area to local udf area with numeric suffix,
   * in order to achieve uniqueness for each locally copied jar.
   * Ex: DrillUDF-1.0.jar -> DrillUDF-1.0_12200255588.jar
   *
   * @param jarName jar name to be copied
   * @param remoteFunctionRegistry remote function registry
   * @param suffix numeric suffix
   * @return local path to jar that was copied
   * @throws IOException in case of problems during jar coping process
   */
  private Path copyJarToLocal(String jarName, RemoteFunctionRegistry remoteFunctionRegistry, long suffix) throws IOException {
    String generatedName = String.format(generated_jar_name_pattern,
        Files.getNameWithoutExtension(jarName), suffix, Files.getFileExtension(jarName));
    Path registryArea = remoteFunctionRegistry.getRegistryArea();
    FileSystem fs = remoteFunctionRegistry.getFs();
    Path remoteJar = new Path(registryArea, jarName);
    Path localJar = new Path(localUdfDir, generatedName);
    try {
      fs.copyToLocalFile(remoteJar, localJar);
    } catch (IOException e) {
      String message = String.format("Error during jar [%s] coping from [%s] to [%s]",
          jarName, registryArea.toUri().getPath(), localUdfDir.toUri().getPath());
      throw new IOException(message, e);
    }
    return localJar;
  }

  /**
   * Deletes quietly local jar but first checks if path to jar is not null.
   *
   * @param jar path to jar
   */
  private void deleteQuietlyLocalJar(Path jar) {
    if (jar != null) {
      FileUtils.deleteQuietly(new File(jar.toUri().getPath()));
    }
  }

  /**
   * Fires when jar name is submitted for unregistration.
   * Will unregister all functions associated with the jar name
   * and delete binary and source associated with the jar from local udf directory
   * according to pattern jar name + {@link #jar_suffix_pattern}.
   */
  public class UnregistrationListener implements TransientStoreListener {

    @Override
    public void onChange(TransientStoreEvent event) {
      String jarName = (String) event.getValue();
      localFunctionRegistry.unregister(jarName);

      Pattern binaryPattern = Pattern.compile(Files.getNameWithoutExtension(jarName) + jar_suffix_pattern);
      Pattern sourcePattern = Pattern.compile(Files.getNameWithoutExtension(JarUtil.getSourceName(jarName)) + jar_suffix_pattern);

      File[] files = new File(localUdfDir.toUri().getPath()).listFiles();
      if (files != null) {
        for (File file : files) {
          if (binaryPattern.matcher(file.getName()).matches() || sourcePattern.matcher(file.getName()).matches()) {
            FileUtils.deleteQuietly(file);
          }
        }
      }
    }
  }

}
