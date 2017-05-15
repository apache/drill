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
package org.apache.drill.exec.server;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.StackTrace;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.ClusterCoordinator.RegistrationHandle;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.server.rest.WebServer;
import org.apache.drill.exec.service.ServiceEngine;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.store.provider.CachingPersistentStoreProvider;
import org.apache.drill.exec.store.sys.store.provider.InMemoryStoreProvider;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.store.sys.PersistentStoreRegistry;
import org.apache.drill.exec.store.sys.store.provider.LocalPersistentStoreProvider;
import org.apache.drill.exec.util.GuavaPatcher;
import org.apache.drill.exec.work.WorkManager;
import org.apache.zookeeper.Environment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

/**
 * Starts, tracks and stops all the required services for a Drillbit daemon to work.
 */
public class Drillbit implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);

  static {
    /*
     * HBase client uses older version of Guava's Stopwatch API,
     * while Drill ships with 18.x which has changes the scope of
     * these API to 'package', this code make them accessible.
     */
    GuavaPatcher.patch();
    Environment.logEnv("Drillbit environment: ", logger);
  }

  public final static String SYSTEM_OPTIONS_NAME = "org.apache.drill.exec.server.Drillbit.system_options";

  private boolean isClosed = false;

  private final ClusterCoordinator coord;
  private final ServiceEngine engine;
  private final PersistentStoreProvider storeProvider;
  private final WorkManager manager;
  private final BootStrapContext context;
  private final WebServer webServer;
  private RegistrationHandle registrationHandle;
  private volatile StoragePluginRegistry storageRegistry;
  private final PersistentStoreProvider profileStoreProvider;

  @VisibleForTesting
  public Drillbit(
      final DrillConfig config,
      final RemoteServiceSet serviceSet) throws Exception {
    this(config, serviceSet, ClassPathScanner.fromPrescan(config));
  }

  public Drillbit(
      final DrillConfig config,
      final RemoteServiceSet serviceSet,
      final ScanResult classpathScan) throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Construction started.");
    final boolean allowPortHunting = serviceSet != null;
    context = new BootStrapContext(config, classpathScan);
    manager = new WorkManager(context);

    webServer = new WebServer(context, manager);
    boolean isDistributedMode = false;
    if (serviceSet != null) {
      coord = serviceSet.getCoordinator();
      storeProvider = new CachingPersistentStoreProvider(new LocalPersistentStoreProvider(config));
    } else {
      coord = new ZKClusterCoordinator(config);
      storeProvider = new PersistentStoreRegistry(this.coord, config).newPStoreProvider();
      isDistributedMode = true;
    }

    //Check if InMemory Profile Store, else use Default Store Provider
    if (config.getBoolean(ExecConstants.PROFILES_STORE_INMEMORY)) {
      profileStoreProvider = new InMemoryStoreProvider(config.getInt(ExecConstants.PROFILES_STORE_CAPACITY));
      logger.info("Upto {} latest query profiles will be retained in-memory", config.getInt(ExecConstants.PROFILES_STORE_CAPACITY));
    } else {
      profileStoreProvider = storeProvider;
    }

    engine = new ServiceEngine(manager, context, allowPortHunting, isDistributedMode);

    logger.info("Construction completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
  }

  public void run() throws Exception {
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Startup begun.");
    coord.start(10000);
    storeProvider.start();
    if (profileStoreProvider != storeProvider) {
      profileStoreProvider.start();
    }
    final DrillbitEndpoint md = engine.start();
    manager.start(md, engine.getController(), engine.getDataConnectionCreator(), coord, storeProvider, profileStoreProvider);
    final DrillbitContext drillbitContext = manager.getContext();
    storageRegistry = drillbitContext.getStorage();
    storageRegistry.init();
    drillbitContext.getOptionManager().init();
    javaPropertiesToSystemOptions();
    manager.getContext().getRemoteFunctionRegistry().init(context.getConfig(), storeProvider, coord);
    registrationHandle = coord.register(md);
    webServer.start();

    Runtime.getRuntime().addShutdownHook(new ShutdownThread(this, new StackTrace()));
    logger.info("Startup completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
  }

  @Override
  public synchronized void close() {
    // avoid complaints about double closing
    if (isClosed) {
      return;
    }
    final Stopwatch w = Stopwatch.createStarted();
    logger.debug("Shutdown begun.");

    // wait for anything that is running to complete
    manager.waitToExit();

    if (coord != null && registrationHandle != null) {
      coord.unregister(registrationHandle);
    }
    try {
      Thread.sleep(context.getConfig().getInt(ExecConstants.ZK_REFRESH) * 2);
    } catch (final InterruptedException e) {
      logger.warn("Interrupted while sleeping during coordination deregistration.");

      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    }

    try {
      AutoCloseables.close(
          webServer,
          engine,
          storeProvider,
          coord,
          manager,
          storageRegistry,
          context);

      //Closing the profile store provider if distinct
      if (storeProvider != profileStoreProvider) {
        AutoCloseables.close(profileStoreProvider);
      }
    } catch(Exception e) {
      logger.warn("Failure on close()", e);
    }

    logger.info("Shutdown completed ({} ms).", w.elapsed(TimeUnit.MILLISECONDS));
    isClosed = true;
  }

  private void javaPropertiesToSystemOptions() {
    // get the system options property
    final String allSystemProps = System.getProperty(SYSTEM_OPTIONS_NAME);
    if ((allSystemProps == null) || allSystemProps.isEmpty()) {
      return;
    }

    final OptionManager optionManager = getContext().getOptionManager();

    // parse out the properties, validate, and then set them
    final String systemProps[] = allSystemProps.split(",");
    for (final String systemProp : systemProps) {
      final String keyValue[] = systemProp.split("=");
      if (keyValue.length != 2) {
        throwInvalidSystemOption(systemProp, "does not contain a key=value assignment");
      }

      final String optionName = keyValue[0].trim();
      if (optionName.isEmpty()) {
        throwInvalidSystemOption(systemProp, "does not contain a key before the assignment");
      }

      final String optionString = stripQuotes(keyValue[1].trim(), systemProp);
      if (optionString.isEmpty()) {
        throwInvalidSystemOption(systemProp, "does not contain a value after the assignment");
      }

      final OptionValue defaultValue = optionManager.getOption(optionName);
      if (defaultValue == null) {
        throwInvalidSystemOption(systemProp, "does not specify a valid option name");
      }
      if (defaultValue.type != OptionType.SYSTEM) {
        throwInvalidSystemOption(systemProp, "does not specify a SYSTEM option ");
      }

      final OptionValue optionValue = OptionValue.createOption(
          defaultValue.kind, OptionType.SYSTEM, optionName, optionString);
      optionManager.setOption(optionValue);
    }
  }

  /**
   * Shutdown hook for Drillbit. Closes the drillbit, and reports on errors that
   * occur during closure, as well as the location the drillbit was started from.
   */
  private static class ShutdownThread extends Thread {
    private final static AtomicInteger idCounter = new AtomicInteger(0);
    private final Drillbit drillbit;
    private final StackTrace stackTrace;

    /**
     * Constructor.
     *
     * @param drillbit the drillbit to close down
     * @param stackTrace the stack trace from where the Drillbit was started;
     *   use new StackTrace() to generate this
     */
    public ShutdownThread(final Drillbit drillbit, final StackTrace stackTrace) {
      this.drillbit = drillbit;
      this.stackTrace = stackTrace;
      /*
       * TODO should we try to determine a test class name?
       * See https://blogs.oracle.com/tor/entry/how_to_determine_the_junit
       */

      setName("Drillbit-ShutdownHook#" + idCounter.getAndIncrement());
    }

    @Override
    public void run() {
      logger.info("Received shutdown request.");
      try {
        /*
         * We can avoid metrics deregistration concurrency issues by only closing
         * one drillbit at a time. To enforce that, we synchronize on a convenient
         * singleton object.
         */
        synchronized(idCounter) {
          drillbit.close();
        }
      } catch(final Exception e) {
        throw new RuntimeException("Caught exception closing Drillbit started from\n" + stackTrace, e);
      }
    }
  }

  public DrillbitContext getContext() {
    return manager.getContext();
  }

  public static void main(final String[] cli) throws DrillbitStartupException {
    final StartupOptions options = StartupOptions.parse(cli);
    start(options);
  }

  public static Drillbit start(final StartupOptions options) throws DrillbitStartupException {
    return start(DrillConfig.create(options.getConfigLocation()), null);
  }

  public static Drillbit start(final DrillConfig config) throws DrillbitStartupException {
    return start(config, null);
  }

  public static Drillbit start(final DrillConfig config, final RemoteServiceSet remoteServiceSet)
      throws DrillbitStartupException {
    logger.debug("Starting new Drillbit.");
    // TODO: allow passing as a parameter
    ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
    Drillbit bit;
    try {
      bit = new Drillbit(config, remoteServiceSet, classpathScan);
    } catch (final Exception ex) {
      throw new DrillbitStartupException("Failure while initializing values in Drillbit.", ex);
    }

    try {
      bit.run();
    } catch (final Exception e) {
      logger.error("Failure during initial startup of Drillbit.", e);
      bit.close();
      throw new DrillbitStartupException("Failure during initial startup of Drillbit.", e);
    }
    logger.debug("Started new Drillbit.");
    return bit;
  }

  private static void throwInvalidSystemOption(final String systemProp, final String errorMessage) {
    throw new IllegalStateException("Property \"" + SYSTEM_OPTIONS_NAME + "\" part \"" + systemProp
        + "\" " + errorMessage + ".");
  }

  private static String stripQuotes(final String s, final String systemProp) {
    if (s.isEmpty()) {
      return s;
    }

    final char cFirst = s.charAt(0);
    final char cLast = s.charAt(s.length() - 1);
    if ((cFirst == '"') || (cFirst == '\'')) {
      if (cLast != cFirst) {
        throwInvalidSystemOption(systemProp, "quoted value does not have closing quote");
      }

      return s.substring(1, s.length() - 2); // strip the quotes
    }

    if ((cLast == '"') || (cLast == '\'')) {
      throwInvalidSystemOption(systemProp, "value has unbalanced closing quote");
    }

    // return as-is
    return s;
  }

}
