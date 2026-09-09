/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link AccessAuthorizer} SPI implementation backed by Ranger.
 *
 * <p>This class is a thin shim on the Drillbit's main classpath. It delegates
 * all calls to {@code DrillAccessControl} (in the {@code drill-ranger-plugin}
 * module) via reflection, using a {@link RangerPluginClassLoader} to load the
 * plugin classes from the isolated {@code ranger-drill-plugin-impl/} directory.</p>
 *
 * <p>The classloader isolation is required because the Ranger plugin ships
 * Jersey 2.35 ({@code org.glassfish.jersey.*} + {@code javax.ws.rs.*}) for
 * {@code RangerAdminJersey2RESTClient}, while Drill's own REST server uses
 * Jersey 3.1.9 ({@code org.glassfish.jersey.*} + {@code jakarta.ws.rs.*}).
 * Both Jersey versions share the {@code org.glassfish.jersey.*} implementation
 * package name but bind to incompatible API namespaces, so they cannot coexist
 * in a single classloader. The {@code RangerPluginClassLoader} uses a
 * child-first strategy to load plugin classes from its private URL list,
 * falling back to the Drillbit classloader for shared types (Hadoop, SLF4J,
 * etc.).</p>
 *
 * <p>Drill uses the subclass {@link DrillRangerPluginClassLoader} instead of
 * the base {@link RangerPluginClassLoader} to filter out the Jersey 3.1.9
 * {@code MultiPartFeatureAutodiscoverable} SPI entry that the base
 * {@code findResources} merge would otherwise leak from the Drillbit
 * classpath into Jersey 2.35's {@code ServiceFinder}. See
 * {@link DrillRangerPluginClassLoader} for the root-cause analysis.</p>
 */
public class RangerAccessAuthorizer implements AccessAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(RangerAccessAuthorizer.class);

  private static final String RANGER_PLUGIN_TYPE = "drill";
  private static final String DRILL_ACCESS_CONTROL_CLASS =
      "org.apache.ranger.authorization.drill.authorizer.DrillAccessControl";

  // DrillAccessControl static method names, resolved reflectively via the plugin classloader.
  private static final String METHOD_INIT = "init";
  private static final String METHOD_IS_ENABLED = "isEnabled";
  private static final String METHOD_CHECK_TABLE_ACCESS = "checkTableAccess";
  private static final String METHOD_CHECK_COLUMN_ACCESS = "checkColumnAccess";

  private RangerPluginClassLoader pluginClassLoader;
  private Class<?> drillAccessControlClass;

  // Cached Method handles for DrillAccessControl static methods
  private final Map<String, Method> methods = new HashMap<>();

  // Maps each DrillAccessControl static method name to its parameter types.
  // The String-operator overloads are used (not the DrillAccessType overloads)
  // because DrillAccessType is not visible on the main classpath.
  private static final Map<String, Class<?>[]> METHOD_SIGNATURES = new HashMap<>();
  static {
    Class<?>[] tableSig  = {String.class, String.class, String.class, String.class, String.class};
    Class<?>[] columnSig = {String.class, String.class, String.class, String.class, Set.class, String.class};
    Class<?>[] noArgSig  = {};
    METHOD_SIGNATURES.put(METHOD_CHECK_TABLE_ACCESS,  tableSig);
    METHOD_SIGNATURES.put(METHOD_CHECK_COLUMN_ACCESS, columnSig);
    METHOD_SIGNATURES.put(METHOD_IS_ENABLED,          noArgSig);
  }

  /**
   * Public no-arg constructor used by {@link AccessAuthorizerFactory} via
   * reflective instantiation. The {@link RangerPluginClassLoader} is created
   * lazily in {@link #init(String)}.
   */
  public RangerAccessAuthorizer() {
  }

  /**
   * Package-private constructor for unit tests. Allows injecting a mock
   * {@link RangerPluginClassLoader} directly, bypassing
   * {@link RangerPluginClassLoader#getInstance} — which Mockito cannot mock
   * because it is a {@link ClassLoader} subclass (mocking class-loader
   * statics risks infinite recursion).
   *
   * <p>When a non-null classloader is supplied, {@link #init(String)} skips
   * the {@code getInstance()} call and uses the injected classloader for the
   * reflective {@code DrillAccessControl} lookup.</p>
   */
  RangerAccessAuthorizer(RangerPluginClassLoader pluginClassLoader) {
    this.pluginClassLoader = pluginClassLoader;
  }

  @Override
  public void init(String serviceName) {
    try {
      if (pluginClassLoader == null) {
        // Use Drill's subclass to filter the Jersey 3.1.9 MultiPart SPI
        pluginClassLoader = DrillRangerPluginClassLoaderHolder.INSTANCE;
        logger.info("DrillRangerPluginClassLoader initialized for plugin type: {}", RANGER_PLUGIN_TYPE);
      }
      activateClassLoader();
      try {
        drillAccessControlClass = pluginClassLoader.loadClass(DRILL_ACCESS_CONTROL_CLASS);
        Method initMethod = drillAccessControlClass.getMethod(METHOD_INIT, String.class);
        initMethod.invoke(null, serviceName);

        for (Map.Entry<String, Class<?>[]> entry : METHOD_SIGNATURES.entrySet()) {
          resolveMethod(entry.getKey(), entry.getValue());
        }
      } finally {
        deactivateClassLoader();
      }
    } catch (Exception e) {
      logger.error("Failed to initialize RangerAccessAuthorizer via PluginClassLoader", e);
      throw new RuntimeException("Failed to initialize RangerAccessAuthorizer: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isEnabled() {
    Method isEnabledMethod = methods.get(METHOD_IS_ENABLED);
    if (isEnabledMethod == null) {
      return false;
    }
    activateClassLoader();
    try {
      return (boolean) isEnabledMethod.invoke(null);
    } catch (Exception e) {
      logger.error("Failed to invoke DrillAccessControl.isEnabled()", e);
      return false;
    } finally {
      deactivateClassLoader();
    }
  }

  /**
   * Checks table-level access permission by reflectively invoking
   * {@code DrillAccessControl.checkTableAccess(..., String operator)} via the
   * plugin classloader. Fail-open (returns {@code true}) when the authorizer
   * is not initialized; fail-closed (returns {@code false}) on invocation error.
   *
   * <p>The {@code accessType} string is forwarded as-is to
   * {@code DrillAccessControl}, which converts it to
   * {@code DrillAccessType} internally. An unsupported access type results in
   * access denied (fail-closed).</p>
   *
   * @param user       the querying user name
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param accessType the access type string (e.g. {@link AccessTypes#SELECT})
   * @return {@code true} if access is allowed
   */
  @Override
  public boolean checkTableAccess(String user, String dataSource, String schema,
                                  String table, String accessType) {
    return invokeTableAccess(methods.get(METHOD_CHECK_TABLE_ACCESS),
                             user, dataSource, schema, table, accessType);
  }

  /**
   * Checks column-level access permission by reflectively invoking
   * {@code DrillAccessControl.checkColumnAccess(..., String operator)} via the
   * plugin classloader. Fail-open (returns {@code true}) when the authorizer
   * is not initialized; fail-closed (returns {@code false}) on invocation error.
   *
   * @param user       the querying user name
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param columns    the set of column names being accessed
   * @param accessType the access type string (e.g. {@link AccessTypes#SELECT})
   * @return {@code true} if access is allowed for every column
   */
  @Override
  public boolean checkColumnAccess(String user, String dataSource, String schema,
                                   String table, Set<String> columns, String accessType) {
    Method method = methods.get(METHOD_CHECK_COLUMN_ACCESS);
    if (method == null) {
      return true; // fail-open when not initialized
    }
    activateClassLoader();
    try {
      return (boolean) method.invoke(null, user, dataSource, schema, table, columns, accessType);
    } catch (Exception e) {
      logger.error("Failed to invoke DrillAccessControl.checkColumnAccess()", e);
      return false; // fail-closed on error
    } finally {
      deactivateClassLoader();
    }
  }

  // ========================================================================
  // Reflection helpers
  // ========================================================================

  /**
   * Resolves a static method on {@code DrillAccessControl} by name and
   * parameter types and caches it in {@link #methods}. Called from
   * {@link #init} for each entry in {@link #METHOD_SIGNATURES}.
   */
  private void resolveMethod(String name, Class<?>... paramTypes) throws NoSuchMethodException {
    methods.put(name, drillAccessControlClass.getMethod(name, paramTypes));
  }

  /**
   * Invokes a cached 5-arg static table-access method on {@code DrillAccessControl}
   * with signature {@code (String, String, String, String, String) -> boolean}.
   * The {@code Method} is pre-resolved in {@link #init} to avoid repeated
   * {@code getMethod()} lookups on the hot path.
   */
  private boolean invokeTableAccess(Method method, String user, String dataSource,
                                    String schema, String table, String accessType) {
    if (method == null) {
      return true; // fail-open when not initialized
    }
    activateClassLoader();
    try {
      return (boolean) method.invoke(null, user, dataSource, schema, table, accessType);
    } catch (Exception e) {
      logger.error("Failed to invoke {}", method.getName(), e);
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
