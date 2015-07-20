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
package org.apache.drill.common.config;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.drill.common.exceptions.DrillConfigurationException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfigBase;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.common.logical.data.LogicalOperatorBase;
import org.apache.drill.common.util.PathScanner;
import org.reflections.util.ClasspathHelper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;


/**
 * Configuration for Drillbit or client.
 *
 * <p>
 *   DrillConfig loads Drill configuration information, primarily from
 *   configuration files.  It does this using a combination of classpath
 *   scanning and configuration fallbacks provided by the TypeSafe configuration
 *   library.
 *   The order of precedence is defined by the following loading order:
 * </p>
 * <ul>
 *   <li>
 *     Load the default configuration file:  Load a single classpath resource
 *     named "{@code drill-default.conf}".  If multiple such resources are on
 *     the classpath, which one is read is unspecified.
 *   </li>
 *   <li>
 *     Load each module's per-module configuration file:  Load all classpath
 *     resources named "{@code drill-module.conf}".  Loading order is
 *     unspecified.
 *   </li>
 *   <li>
 *     Load an optional overrides configuration file:  By default, try to load
 *     a single classpath resource named "{@code drill-override.conf}".  If a
 *     non-null value for an {@code overrideFileResourcePathname} parameter is
 *     provided, that value is used instead of the default resource pathname of
 *     "{@code drill-override.conf}".  If multiple resources are on the
 *     classpath, which copy is read is unspecified.
 *   </li>
 *   <li>
 *     Load any additional configuration overrides from JVM system properties.
 *   </li>
 * </ul>
 */
public final class DrillConfig extends NestedConfig{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConfig.class);

  private final ObjectMapper mapper;
  private final ImmutableList<String> startupArguments;

  public static final boolean ON_OSX = System.getProperty("os.name").contains("OS X");

  @SuppressWarnings("restriction")
  private static final long MAX_DIRECT_MEMORY = sun.misc.VM.maxDirectMemory();

  @VisibleForTesting
  public DrillConfig(Config config, boolean enableServerConfigs) {
    super(config);
    logger.debug("Setting up DrillConfig object.");
    logger.trace("Given Config object is:\n{}",
                 config.root().render(ConfigRenderOptions.defaults()));
    mapper = new ObjectMapper();

    if (enableServerConfigs) {
      SimpleModule deserModule = new SimpleModule("LogicalExpressionDeserializationModule")
        .addDeserializer(LogicalExpression.class, new LogicalExpression.De(this))
        .addDeserializer(SchemaPath.class, new SchemaPath.De());

      mapper.registerModule(deserModule);
      mapper.enable(SerializationFeature.INDENT_OUTPUT);
      mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      mapper.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, true);
      mapper.configure(Feature.ALLOW_COMMENTS, true);
      mapper.registerSubtypes(LogicalOperatorBase.getSubTypes(this));
      mapper.registerSubtypes(StoragePluginConfigBase.getSubTypes(this));
      mapper.registerSubtypes(FormatPluginConfigBase.getSubTypes(this));
    }

    RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
    this.startupArguments = ImmutableList.copyOf(bean.getInputArguments());
    logger.debug("DrillConfig object initialized.");
  };

  public List<String> getStartupArguments() {
    return startupArguments;
  }

  /**
   * Creates a {@link DrillConfig Drill configuration} using the default
   * resource pathname for the overrides configuration file and with
   * server-specific configuration options enabled.
   *
   * @return the new configuration object
   *
   * @see DrillConfig class description
   */
  public static DrillConfig create() {
    return create(null, true);
  }

  /**
   * Creates a {@link DrillConfig Drill configuration} with server-specific
   * configuration options disabled.  Uses the default resource pathname for the
   * overrides configuration file.
   *
   * @return the new configuration object
   *
   * @see DrillConfig class description
   */
  public static DrillConfig forClient() {
    return create(null, false);
  }

  /**
   * Creates a {@link DrillConfig Drill configuration}, allowing overriding
   * of the resource pathname of overrides configuration file.  Creates it with
   * server-specific configuration options enabled.
   *
   * @param overrideFileResourcePathname
   *          the classpath resource pathname to use for the overrides
   *          configuration file; {@code null} specifies to use the
   *          default pathname
   *          ({@link CommonConstants#CONFIG_OVERRIDE_RESOURCE_PATHNAME}) (null
   *          does <strong>not</strong> specify to suppress trying to load an
   *          overrides file)
   *
   * @return the new configuration object
   *
   * @see DrillConfig class description
   */
  public static DrillConfig create(String overrideFileResourcePathname) {
    return create(overrideFileResourcePathname, true);
  }

  /**
   * Creates a {@link DrillConfig Drill configuration}, allowing overriding
   * of the resource pathname of overrides configuration file and specifying
   * whether to enable or disable server-specific configuration options.
   *
   * @param  overrideFileResourcePathname
   *           see {@link #create(String)}
   * @param  enableServerConfigs
   *           whether to enable server-specific configuration options
   *
   *
   * @return the new configuration object
   *
   * @see #create(String)
   * @see DrillConfig class description
   */
  public static DrillConfig create(String overrideFileResourcePathname, boolean enableServerConfigs) {
    return create(overrideFileResourcePathname, null, enableServerConfigs);
  }

  /**
   * <b><u>Do not use this method outside of test code.</u></b>
   * <p>
   *   NOTE:  This is currently used outside of test code.
   * </p>
   *
   * @param  testConfigurations
   *           optional property map for further overriding (after any overrides
   *           configuration file's overrides are merged in)
   *
   * @return the new configuration object
   *
   * @see DrillConfig class description
   */
  @VisibleForTesting
  public static DrillConfig create(Properties testConfigurations) {
    return create(null, testConfigurations, true);
  }

  /**
   * Creates a {@link DrillConfig Drill configuration}.
   * @param  overrideFileResourcePathname  see {@link #create(String, boolean)}
   * @param  overriderProps  see {@link #create(Properties)}
   * @param  enableServerConfigs  see {@link #create(String, boolean)}
   *
   * @return the new configuration object
   */
  private static DrillConfig create(String overrideFileResourcePathname,
                                    final Properties overriderProps,
                                    final boolean enableServerConfigs) {
    overrideFileResourcePathname =
        overrideFileResourcePathname == null
            ? CommonConstants.CONFIG_OVERRIDE_RESOURCE_PATHNAME
            : overrideFileResourcePathname;

    // 1. Load defaults configuration file.
    Config fallback = null;
    final ClassLoader[] classLoaders = ClasspathHelper.classLoaders();
    for (ClassLoader classLoader : classLoaders) {
      final URL url =
          classLoader.getResource(CommonConstants.CONFIG_DEFAULT_RESOURCE_PATHNAME);
      if (null != url) {
        logger.info("Loading base configuration file at {}.", url);
        fallback =
            ConfigFactory.load(classLoader,
                               CommonConstants.CONFIG_DEFAULT_RESOURCE_PATHNAME);
        break;
      }
    }

    // 2. Load per-module configuration files.
    final Collection<URL> urls = PathScanner.getConfigURLs();
    final String lineBrokenList =
        urls.size() == 0 ? "" : "\n\t- " + Joiner.on("\n\t- ").join(urls);
    logger.info("Loading {} module configuration files at: {}.",
                urls.size(), lineBrokenList);
    for (URL url : urls) {
      fallback = ConfigFactory.parseURL(url).withFallback(fallback);
    }

    // 3. Load any specified overrides configuration file along with any
    //    overrides from JVM system properties (e.g., {-Dname=value").

    // (Per ConfigFactory.load(...)'s mention of using Thread.getContextClassLoader():)
    final URL overrideFileUrl =
        Thread.currentThread().getContextClassLoader().getResource(overrideFileResourcePathname);
    if (null != overrideFileUrl ) {
      logger.info("Loading override config. file at {}.", overrideFileUrl);
    }
    Config effectiveConfig =
        ConfigFactory.load(overrideFileResourcePathname).withFallback(fallback);

    // 4. Apply any overriding properties.
    if (overriderProps != null) {
      logger.info("Loading override Properties parameter {}.", overriderProps);
      effectiveConfig =
          ConfigFactory.parseProperties(overriderProps).withFallback(effectiveConfig);
    }

    // 5. Create DrillConfig object from Config object.
    return new DrillConfig(effectiveConfig.resolve(), enableServerConfigs);
  }

  public <T> Class<T> getClassAt(String location, Class<T> clazz) throws DrillConfigurationException {
    final String className = getString(location);
    if (className == null) {
      throw new DrillConfigurationException(String.format(
          "No class defined at location '%s'. Expected a definition of the class []",
          location, clazz.getCanonicalName()));
    }

    try {
      final Class<?> c = Class.forName(className);
      if (clazz.isAssignableFrom(c)) {
        @SuppressWarnings("unchecked")
        final Class<T> t = (Class<T>) c;
        return t;
      }

      throw new DrillConfigurationException(String.format("The class [%s] listed at location '%s' should be of type [%s].  It isn't.", className, location, clazz.getCanonicalName()));
    } catch (Exception ex) {
      if (ex instanceof DrillConfigurationException) {
        throw (DrillConfigurationException) ex;
      }
      throw new DrillConfigurationException(String.format("Failure while initializing class [%s] described at configuration value '%s'.", className, location), ex);
    }
  }

  public <T> T getInstanceOf(String location, Class<T> clazz) throws DrillConfigurationException{
    final Class<T> c = getClassAt(location, clazz);
    try {
      final T t = c.newInstance();
      return t;
    } catch (Exception ex) {
      throw new DrillConfigurationException(String.format("Failure while instantiating class [%s] located at '%s.", clazz.getCanonicalName(), location), ex);
    }
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  @Override
  public String toString() {
    return this.root().render();
  }

  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY;
  }
}
