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
package org.apache.drill.exec.security;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessAuthorizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Engine-side manager for the {@link AccessAuthorizer} SPI.
 * <ul>
 *   <li>reads {@code drill.exec.security.authorizer.enabled} — when {@code false}
 *       (the default) an {@link AllowAllAccessAuthorizer} is used (fail-open);</li>
 *   <li>flattens the {@code drill.exec.security.authorizer} configuration
 *       subtree into a plain {@code Map<String, String>} (engine configuration
 *       types never leak into the SPI);</li>
 *   <li>discovers {@link AccessAuthorizerFactory} implementations via
 *       {@link ServiceLoader} and selects the one whose {@code getName()}
 *       matches {@code drill.exec.security.authorizer.name} (default
 *       {@code "ranger"});</li>
 *   <li>fail-closed: enabled with no matching factory throws at startup
 *       rather than silently allowing everything.</li>
 * </ul>
 *
 * <p>The created instance is cached in a lazy double-checked-locking singleton;
 * mount points ({@code DrillCalciteCatalogReader}, {@code ColumnAccessChecker})
 * retrieve it via {@link #getAuthorizer(DrillConfig)}. The Drillbit triggers
 * initialization eagerly at startup for fail-fast behavior.</p>
 */
public final class AccessAuthorizerManager {

  private static final Logger logger = LoggerFactory.getLogger(AccessAuthorizerManager.class);

  static final String CONFIG_PREFIX = "drill.exec.security.authorizer";
  static final String DEFAULT_FACTORY_NAME = "ranger";

  private static volatile AccessAuthorizer instance;

  private AccessAuthorizerManager() {
  }

  /**
   * Returns the singleton {@link AccessAuthorizer} instance, initializing it
   * from the given configuration on first call.
   *
   * @param config the Drill configuration
   * @return the authorizer (never {@code null})
   */
  public static AccessAuthorizer getAuthorizer(DrillConfig config) {
    if (instance != null) {
      return instance;
    }
    synchronized (AccessAuthorizerManager.class) {
      if (instance == null) {
        instance = load(config);
      }
    }
    return instance;
  }

  /**
   * Resets the cached singleton. Package-private; used by tests.
   */
  static void reset() {
    instance = null;
  }

  /**
   * Closes the cached authorizer if any, then clears the singleton so a
   * future {@link #getAuthorizer(DrillConfig)} initializes a fresh instance.
   *
   * <p>Called by {@code Drillbit.close()} during shutdown, after all
   * in-flight queries have drained: the authorizer may hold plugin resources
   * (e.g. Ranger policy-refresh threads, policy-engine caches) that must be
   * released. Idempotent and safe when authorization is disabled or never
   * initialized — {@link AllowAllAccessAuthorizer} performs no work, and the
   * default SPI {@code close()} is a no-op. Close failures are logged and
   * ignored so a failing authorizer never aborts Drill shutdown.</p>
   */
  public static void close() {
    synchronized (AccessAuthorizerManager.class) {
      AccessAuthorizer authorizer = instance;
      // Drop the singleton reference first: even if close() throws, the next
      // getAuthorizer() must be able to build a fresh instance.
      instance = null;
      if (authorizer != null) {
        try {
          authorizer.close();
        } catch (Exception e) {
          logger.warn("Failure while closing access authorizer", e);
        }
      }
    }
  }

  private static AccessAuthorizer load(DrillConfig config) {
    if (!isEnabled(config)) {
      logger.info("Access authorizer disabled (drill.exec.security.authorizer.enabled=false); "
          + "using AllowAllAccessAuthorizer (fail-open)");
      return new AllowAllAccessAuthorizer();
    }

    // Read the selection key directly from the config: flattenConfig strips
    // the engine-managed keys ("enabled"/"name") from the plugin-visible map.
    String name = config.hasPath(CONFIG_PREFIX + ".name")
        ? config.getString(CONFIG_PREFIX + ".name") : null;
    if (name == null || name.isEmpty()) {
      name = DEFAULT_FACTORY_NAME;
    }
    Map<String, String> props = flattenConfig(config);

    // Discover factories via the Java SPI. The Ranger factory is shipped on
    // the Drillbit classpath (shim jar); Drill core has no compile-time
    // reference to any implementation.
    for (AccessAuthorizerFactory factory : ServiceLoader.load(AccessAuthorizerFactory.class)) {
      if (factory.getName().equals(name)) {
        AccessAuthorizer authorizer = factory.createAuthorizer(props);
        logger.info("Initialized access authorizer '{}' via {}", name,
            factory.getClass().getName());
        return authorizer;
      }
      logger.debug("Skipping AccessAuthorizerFactory '{}' (does not match configured name '{}')",
          factory.getName(), name);
    }
    throw new RuntimeException("Access authorizer is enabled but no AccessAuthorizerFactory "
        + "named '" + name + "' was found on the classpath. Ensure the shim jar registering "
        + "META-INF/services/org.apache.drill.exec.security.spi.AccessAuthorizerFactory "
        + "is present and initialized correctly.");
  }

  /**
   * Returns whether the access authorizer is enabled in the configuration
   * ({@code drill.exec.security.authorizer.enabled}, default {@code false}).
   *
   * <p>Mount points that pay measurable setup cost before the first SPI call
   * (e.g. the column-level check walks the whole RelNode tree) consult this
   * up front and skip the work entirely when authorization is disabled,
   * instead of relying on the {@link AllowAllAccessAuthorizer} sentinel
   * short-circuiting inside each check.</p>
   *
   * @param config the Drill configuration
   * @return {@code true} only when the enabled flag is explicitly set to true
   */
  public static boolean isEnabled(DrillConfig config) {
    return config.hasPath(CONFIG_PREFIX + ".enabled")
        && config.getBoolean(CONFIG_PREFIX + ".enabled");
  }

  /**
   * Flattens the {@code drill.exec.security.authorizer} configuration subtree
   * into a plain map of scalar leaf values. The {@code enabled} and {@code name}
   * keys are engine-managed selection knobs and are removed from the result.
   */
  private static Map<String, String> flattenConfig(DrillConfig config) {
    Map<String, String> props = new HashMap<>();
    if (config.hasPath(CONFIG_PREFIX)) {
      config.getConfig(CONFIG_PREFIX).entrySet().forEach(e -> {
        Object value = e.getValue().unwrapped();
        if (value != null && !(value instanceof Map) && !(value instanceof List)) {
          props.put(e.getKey(), String.valueOf(value));
        }
      });
    }
    props.remove("enabled");
    props.remove("name");
    return props;
  }
}
