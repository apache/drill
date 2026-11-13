/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.security.ranger;

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * {@link AccessAuthorizer} SPI implementation backed by Ranger.
 *
 * <p>This class is a thin shim on the Drillbit's main classpath. It creates
 * the delegate {@code DrillAccessControl} (in the {@code drill-ranger-plugin}
 * module) once via reflection, casts it to {@link AccessAuthorizer} and then
 * invokes all checks as direct virtual calls through the SPI interface.
 * The cast is safe because the plugin classloader is child-first but falls
 * back to the Drillbit classloader for SPI types: both sides resolve
 * {@code AccessAuthorizer}/{@code UserIdentity} to the same Class objects
 * from drill-security-spi.jar on the main classpath.</p>
 *
 * <p>The classloader isolation is required because the Ranger plugin ships
 * Jersey 2.35 ({@code org.glassfish.jersey.*} + {@code javax.ws.rs.*}) for
 * {@code RangerAdminJersey2RESTClient}, while Drill's own REST server uses
 * Jersey 3.1.9 ({@code org.glassfish.jersey.*} + {@code jakarta.ws.rs.*}).
 * Both Jersey versions share the {@code org.glassfish.jersey.*} implementation
 * package name but bind to incompatible API namespaces, so they cannot coexist
 * in a single classloader. The {@link RangerPluginClassLoader} uses a
 * child-first strategy to load plugin classes from its private URL list,
 * falling back to the Drillbit classloader for shared types (Hadoop, SLF4J,
 * the Drill security SPI, etc.).</p>
 *
 * <p>Drill uses the subclass {@link DrillRangerPluginClassLoader} instead of
 * the base {@link RangerPluginClassLoader} to filter out the Jersey 3.1.9
 * {@code MultiPartFeatureAutodiscoverable} SPI entry that the base
 * {@code findResources} merge would otherwise leak from the Drillbit
 * classpath into Jersey 2.35's {@code ServiceFinder}. See
 * {@link DrillRangerPluginClassLoader} for the root-cause analysis.</p>
 *
 * <p>Every delegated call is wrapped in
 * {@code activateClassLoader()/deactivateClassLoader()} — the same
 * per-call TCCL contract used by Presto's {@code RangerSystemAccessControl}
 * and the other Ranger plugin shims.</p>
 */
public class RangerAccessAuthorizer implements AccessAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(RangerAccessAuthorizer.class);

  private static final String RANGER_PLUGIN_TYPE = "drill";
  private static final String DRILL_ACCESS_CONTROL_CLASS =
      "org.apache.ranger.authorization.drill.authorizer.DrillAccessControl";

  private final RangerPluginClassLoader pluginClassLoader;

  // Delegate created once in the constructor; all checks are direct virtual
  // calls through the SPI interface (no per-call reflection). Final: the
  // constructor either assigns it or throws (fail-closed), so an instance
  // that exists always has a working delegate.
  private final AccessAuthorizer delegate;

  /**
   * Creates the shim and completes all initialization (mirrors Presto's
   * RangerSystemAccessControl constructor): acquires the
   * {@link RangerPluginClassLoader}, reflectively instantiates
   * {@code DrillAccessControl(serviceName)} from the isolated
   * {@code ranger-drill-plugin-impl/} directory inside an
   * activated-classloader block, and casts it to {@link AccessAuthorizer}.
   *
   * @param serviceName the Ranger service instance name
   * @throws RuntimeException if initialization fails (fail-closed)
   */
  public RangerAccessAuthorizer(String serviceName) {
    this(createProductionClassLoader(), serviceName);
  }

  /**
   * Acquires the singleton production {@link DrillRangerPluginClassLoader}.
   * Initialization failures (including {@link ExceptionInInitializerError}
   * from the lazy holder) are wrapped in {@link RuntimeException} so the
   * factory always surfaces a consistent fail-closed exception type.
   */
  private static RangerPluginClassLoader createProductionClassLoader() {
    try {
      RangerPluginClassLoader cl = DrillRangerPluginClassLoaderHolder.INSTANCE;
      logger.info("DrillRangerPluginClassLoader initialized for plugin type: {}", RANGER_PLUGIN_TYPE);
      return cl;
    } catch (Throwable t) {
      logger.error("Failed to create DrillRangerPluginClassLoader", t);
      throw new RuntimeException(
          "Failed to create DrillRangerPluginClassLoader: " + t.getMessage(), t);
    }
  }

  /**
   * Package-private constructor for unit tests. Allows injecting a mock
   * {@link RangerPluginClassLoader} directly, bypassing
   * {@link RangerPluginClassLoader#getInstance} — which Mockito cannot mock
   * because it is a {@link ClassLoader} subclass (mocking class-loader
   * statics risks infinite recursion).
   *
   * <p>When a non-null classloader is supplied, the delegate is loaded and
   * instantiated through the injected classloader instead of the production
   * holder; the mock typically delegates {@code loadClass} to the test
   * classloader so the test stub class (same FQCN) is instantiated.</p>
   */
  RangerAccessAuthorizer(RangerPluginClassLoader pluginClassLoader, String serviceName) {
    try {
      this.pluginClassLoader = pluginClassLoader;
      activateClassLoader();
      try {
        Class<?> clazz = pluginClassLoader.loadClass(DRILL_ACCESS_CONTROL_CLASS);
        delegate = (AccessAuthorizer) clazz.getConstructor(String.class).newInstance(serviceName);
      } finally {
        deactivateClassLoader();
      }
    } catch (Exception e) {
      logger.error("Failed to initialize RangerAccessAuthorizer via PluginClassLoader", e);
      throw new RuntimeException("Failed to initialize RangerAccessAuthorizer: " + e.getMessage(), e);
    }
  }

  /**
   * Checks table-level access permission by delegating directly to the
   * {@code DrillAccessControl} instance through the SPI interface.
   * Fail-closed (returns {@code false}) on invocation error.
   *
   * @param user       the querying user identity
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param accessType the access type (e.g. {@code AccessType.SELECT})
   * @return {@code true} if access is allowed
   */
  @Override
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
                                  String table, AccessType accessType) {
    activateClassLoader();
    try {
      return delegate.checkTableAccess(user, dataSource, schema, table, accessType);
    } catch (Exception e) {
      logger.error("Failed to invoke DrillAccessControl.checkTableAccess()", e);
      return false; // fail-closed on error
    } finally {
      deactivateClassLoader();
    }
  }

  /**
   * Checks column-level access permission by delegating directly to the
   * {@code DrillAccessControl} instance through the SPI interface.
   * Fail-closed (returns {@code false}) on invocation error.
   *
   * @param user       the querying user identity
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param columns    the set of column names being accessed
   * @param accessType the access type (e.g. {@code AccessType.SELECT})
   * @return {@code true} if access is allowed for every column
   */
  @Override
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
                                   String table, Set<String> columns, AccessType accessType) {
    activateClassLoader();
    try {
      return delegate.checkColumnAccess(user, dataSource, schema, table, columns, accessType);
    } catch (Exception e) {
      logger.error("Failed to invoke DrillAccessControl.checkColumnAccess()", e);
      return false; // fail-closed on error
    } finally {
      deactivateClassLoader();
    }
  }

  private void activateClassLoader() {
    if (pluginClassLoader != null) {
      pluginClassLoader.activate();
    }
  }

  private void deactivateClassLoader() {
    if (pluginClassLoader != null) {
      pluginClassLoader.deactivate();
    }
  }

  /**
   * Releases the delegate's plugin resources (policy-refresh threads, policy
   * caches) through the same classloader-activated contract used by the
   * access checks. Forwarded by {@code AccessAuthorizerManager} when the
   * Drillbit shuts down. Idempotent; failures are logged, never thrown, so a
   * failing authorizer cannot abort Drill shutdown.
   */
  @Override
  public void close() {
    activateClassLoader();
    try {
      delegate.close();
    } catch (Exception e) {
      logger.warn("Failed to close DrillAccessControl", e);
    } finally {
      deactivateClassLoader();
    }
  }

  /**
   * Holder for the singleton {@link DrillRangerPluginClassLoader}. The
   * base {@code RangerPluginClassLoader.getInstance()} cannot return our
   * subclass, so Drill keeps its own single instance here. Initialized
   * lazily on first class-loading of the enclosing authorizer.
   */
  private static final class DrillRangerPluginClassLoaderHolder {
    static final RangerPluginClassLoader INSTANCE;

    static {
      try {
        INSTANCE = new DrillRangerPluginClassLoader(
            RANGER_PLUGIN_TYPE, RangerAccessAuthorizer.class);
      } catch (Exception e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }
}
