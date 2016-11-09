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
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
public class FunctionImplementationRegistry implements FunctionLookupContext, AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);

  private final LocalFunctionRegistry localFunctionRegistry;
  private final RemoteFunctionRegistry remoteFunctionRegistry;
  private final Path localUdfDir;
  private boolean deleteTmpDir = false;
  private File tmpDir;
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
    this.localUdfDir = getLocalUdfDir(config);
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
    localFunctionRegistry.register(operatorTable);

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
          return findDrillFunction(functionResolver, functionCall, false);
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
        return findExactMatchingDrillFunction(name, argTypes, returnType, false);
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
   * Class loader is closed to release opened connection to jar when validation is finished.
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
    try (URLClassLoader classLoader = new URLClassLoader(urls)) {
      ScanResult jarScanResult = scan(classLoader, path, urls);
      List<String> functions = localFunctionRegistry.validate(path.getName(), jarScanResult);
      if (functions.isEmpty()) {
        throw new FunctionValidationException(String.format("Jar %s does not contain functions", path.getName()));
      }
      return functions;
    }
  }

  /**
   * Attempts to load and register functions from remote function registry.
   * First checks if there is no missing jars.
   * If yes, enters synchronized block to prevent other loading the same jars.
   * Again re-checks if there are no missing jars in case someone has already loaded them (double-check lock).
   * If there are still missing jars, first copies jars to local udf area and prepares {@link JarScan} for each jar.
   * Jar registration timestamp represented in milliseconds is used as suffix.
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
          URLClassLoader classLoader = null;
          try {
            binary = copyJarToLocal(jarName, remoteFunctionRegistry);
            source = copyJarToLocal(JarUtil.getSourceName(jarName), remoteFunctionRegistry);
            URL[] urls = {binary.toUri().toURL(), source.toUri().toURL()};
            classLoader = new URLClassLoader(urls);
            ScanResult scanResult = scan(classLoader, binary, urls);
            localFunctionRegistry.validate(jarName, scanResult);
            jars.add(new JarScan(jarName, scanResult, classLoader));
          } catch (Exception e) {
            deleteQuietlyLocalJar(binary);
            deleteQuietlyLocalJar(source);
            if (classLoader != null) {
              try {
                classLoader.close();
              } catch (Exception ex) {
                logger.warn("Problem during closing class loader for {}", jarName, e);
              }
            }
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
   * Additional logic is added to close {@link URL} after {@link ConfigFactory#parseURL(URL)}.
   * This is extremely important for Windows users where system doesn't allow to delete file if it's being used.
   *
   * @param classLoader unique class loader for jar
   * @param path local path to jar
   * @param urls urls associated with the jar (ex: binary and source)
   * @return scan result of packages, classes, annotations found in jar
   */
  private ScanResult scan(ClassLoader classLoader, Path path, URL[] urls) throws IOException {
    Enumeration<URL> markerFileEnumeration = classLoader.getResources(
        CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME);
    while (markerFileEnumeration.hasMoreElements()) {
      URL markerFile = markerFileEnumeration.nextElement();
      if (markerFile.getPath().contains(path.toUri().getPath())) {
        URLConnection markerFileConnection = null;
        try {
          markerFileConnection = markerFile.openConnection();
          DrillConfig drillConfig = DrillConfig.create(ConfigFactory.parseURL(markerFile));
          return RunTimeScan.dynamicPackageScan(drillConfig, Sets.newHashSet(urls));
        } finally {
          if (markerFileConnection instanceof JarURLConnection) {
            ((JarURLConnection) markerFile.openConnection()).getJarFile().close();
          }
        }
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
   * Checks if local udf directory is a directory and if current application has write rights on it.
   * Attempts to clean up local udf directory in case jars were left after previous drillbit run.
   * Local udf directory path is concatenated from drill temporary directory and ${drill.exec.udf.directory.local}.
   *
   * @param config drill config
   * @return path to local udf directory
   */
  private Path getLocalUdfDir(DrillConfig config) {
    tmpDir = getTmpDir(config);
    File udfDir = new File(tmpDir, config.getString(ExecConstants.UDF_DIRECTORY_LOCAL));
    udfDir.mkdirs();
    String udfPath = udfDir.getPath();
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
   * First tries to get drill temporary directory value from environmental variable $DRILL_TMP_DIR,
   * then from config ${drill.tmp-dir}.
   * If value is still missing, generates directory using {@link Files#createTempDir()}.
   * If temporary directory was generated, sets {@link #deleteTmpDir} to true
   * to delete directory on drillbit exit.
   * @return drill temporary directory path
   */
  private File getTmpDir(DrillConfig config) {
    String drillTempDir = System.getenv("DRILL_TMP_DIR");

    if (drillTempDir == null && config.hasPath(ExecConstants.DRILL_TMP_DIR)) {
      drillTempDir = config.getString(ExecConstants.DRILL_TMP_DIR);
    }

    if (drillTempDir == null) {
      deleteTmpDir = true;
      return Files.createTempDir();
    }

    return new File(drillTempDir);
  }

  /**
   * Copies jar from remote udf area to local udf area.
   *
   * @param jarName jar name to be copied
   * @param remoteFunctionRegistry remote function registry
   * @return local path to jar that was copied
   * @throws IOException in case of problems during jar coping process
   */
  private Path copyJarToLocal(String jarName, RemoteFunctionRegistry remoteFunctionRegistry) throws IOException {
    Path registryArea = remoteFunctionRegistry.getRegistryArea();
    FileSystem fs = remoteFunctionRegistry.getFs();
    Path remoteJar = new Path(registryArea, jarName);
    Path localJar = new Path(localUdfDir, jarName);
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
   * If {@link #deleteTmpDir} is set to true, deletes generated temporary directory.
   * Otherwise cleans up {@link #localUdfDir}.
   */
  @Override
  public void close() {
    if (deleteTmpDir) {
      FileUtils.deleteQuietly(tmpDir);
    } else {
      try {
        FileUtils.cleanDirectory(new File(localUdfDir.toUri().getPath()));
      } catch (IOException e) {
        logger.warn("Problems during local udf directory clean up", e);
      }
    }
  }

  /**
   * Fires when jar name is submitted for unregistration.
   * Will unregister all functions associated with the jar name
   * and delete binary and source associated with the jar from local udf directory
   */
  public class UnregistrationListener implements TransientStoreListener {

    @Override
    public void onChange(TransientStoreEvent event) {
      String jarName = (String) event.getValue();
      localFunctionRegistry.unregister(jarName);
      String localDir = localUdfDir.toUri().getPath();
      FileUtils.deleteQuietly(new File(localDir, jarName));
      FileUtils.deleteQuietly(new File(localDir, JarUtil.getSourceName(jarName)));
    }
  }

}
