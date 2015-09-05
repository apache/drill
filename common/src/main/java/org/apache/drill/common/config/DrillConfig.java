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
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

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

public final class DrillConfig extends NestedConfig{
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillConfig.class);

  private final ObjectMapper mapper;
  private final ImmutableList<String> startupArguments;

  public static final boolean ON_OSX = System.getProperty("os.name").contains("OS X");

  @SuppressWarnings("restriction")
  private static final long MAX_DIRECT_MEMORY = sun.misc.VM.maxDirectMemory();

  @SuppressWarnings("unchecked")
  private volatile List<Queue<Object>> sinkQueues = new CopyOnWriteArrayList<Queue<Object>>(new Queue[1]);

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
   * Creates a DrillConfig object using the default config file name
   * and with server-specific configuration options enabled.
   * @return The new DrillConfig object.
   */
  public static DrillConfig create() {
    return create(null, true);
  }

  /**
   * Creates a {@link DrillConfig configuration} using the default config file
   * name and with server-specific configuration options disabled.
   *
   * @return {@link DrillConfig} instance
   */
  public static DrillConfig forClient() {
    return create(null, false);
  }

  /**
   * <p>
   * DrillConfig loads up Drill configuration information. It does this utilizing a combination of classpath scanning
   * and Configuration fallbacks provided by the TypeSafe configuration library. The order of precedence is as
   * follows:
   * </p>
   * <p>
   * Configuration values are retrieved as follows:
   * <ul>
   * <li>Check a single copy of "drill-override.conf".  If multiple copies are
   *     on the classpath, which copy is read is indeterminate.
   *     If a non-null value for overrideFileResourcePathname is provided, this
   *     is used instead of "{@code drill-override.conf}".</li>
   * <li>Check all copies of "{@code drill-module.conf}".  Loading order is
   *     indeterminate.</li>
   * <li>Check a single copy of "{@code drill-default.conf}".  If multiple
   *     copies are on the classpath, which copy is read is indeterminate.</li>
   * </ul>
   *
   * </p>
   * @param overrideFileResourcePathname
   *          the classpath resource pathname of the file to use for
   *          configuration override purposes; {@code null} specifies to use the
   *          default pathname ({@link CommonConstants.CONFIG_OVERRIDE}) (does
   *          <strong>not</strong> specify to suppress trying to load an
   *          overrides file)
   *  @return A merged Config object.
   */
  public static DrillConfig create(String overrideFileResourcePathname) {
    return create(overrideFileResourcePathname, true);
  }

  /**
   * <b><u>Do not use this method outside of test code.</u></b>
   */
  @VisibleForTesting
  public static DrillConfig create(Properties testConfigurations) {
    return create(null, testConfigurations, true);
  }

  /**
   * @param overrideFileResourcePathname
   *          see {@link #create(String)}'s {@code overrideFileResourcePathname}
   */
  public static DrillConfig create(String overrideFileResourcePathname, boolean enableServerConfigs) {
    return create(overrideFileResourcePathname, null, enableServerConfigs);
  }

  /**
   * @param overrideFileResourcePathname
   *          see {@link #create(String)}'s {@code overrideFileResourcePathname}
   * @param overriderProps
   *          optional property map for further overriding (after override file
   *          is assimilated
   * @param enableServerConfigs
   *          whether to enable server-specific configuration options
   * @return
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

  public <T> Class<T> getClassAt(String location, Class<T> clazz) throws DrillConfigurationException{
    String className = this.getString(location);
    if (className == null) {
      throw new DrillConfigurationException(String.format("No class defined at location '%s'.  Expected a definition of the class []", location, clazz.getCanonicalName()));
    }
    try{
      Class<?> c = Class.forName(className);
      if (clazz.isAssignableFrom(c)) {
        @SuppressWarnings("unchecked") Class<T> t = (Class<T>) c;
        return t;
      } else {
        throw new DrillConfigurationException(String.format("The class [%s] listed at location '%s' should be of type [%s].  It isn't.", className, location, clazz.getCanonicalName()));
      }
    } catch (Exception ex) {
      if (ex instanceof DrillConfigurationException) {
        throw (DrillConfigurationException) ex;
      }
      throw new DrillConfigurationException(String.format("Failure while initializing class [%s] described at configuration value '%s'.", className, location), ex);
    }

  }

  public <T> T getInstanceOf(String location, Class<T> clazz) throws DrillConfigurationException{
    Class<T> c = getClassAt(location, clazz);
    try {
      T t = c.newInstance();
      return t;
    } catch (Exception ex) {
      throw new DrillConfigurationException(String.format("Failure while instantiating class [%s] located at '%s.", clazz.getCanonicalName(), location), ex);
    }
  }

  public void setSinkQueues(int number, Queue<Object> queue) {
    sinkQueues.set(number, queue);
  }

  public Queue<Object> getQueue(int number) {
    if (sinkQueues.size() <= number || number < 0 || sinkQueues == null) {
      throw new IllegalArgumentException(String.format("Queue %d is not available.", number));
    }
    return sinkQueues.get(number);
  }

  public ObjectMapper getMapper() {
    return mapper;
  }

  @Override
  public String toString() {
    return this.root().render();
  }

  public static void main(String[] args)  throws Exception {
    //"-XX:MaxDirectMemorySize"
    DrillConfig config = DrillConfig.create();
  }

  public static long getMaxDirectMemory() {
    return MAX_DIRECT_MEMORY;
  }
}
