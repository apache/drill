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
package org.apache.drill.exec.store.dfs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.util.ConstructorChecker;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

/**
 * Responsible for instantiating format plugins
 */
public class FormatCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatCreator.class);

  private static final ConstructorChecker FORMAT_BASED = new ConstructorChecker(String.class, DrillbitContext.class,
      Configuration.class, StoragePluginConfig.class, FormatPluginConfig.class);
  private static final ConstructorChecker DEFAULT_BASED = new ConstructorChecker(String.class, DrillbitContext.class,
      Configuration.class, StoragePluginConfig.class);

  /**
   * Returns a Map from the FormatPlugin Config class to the constructor of the format plugin that accepts it.
   * This is used to create a format plugin instance from its configuration.
   * @param pluginClasses the FormatPlugin classes to index on their config class
   * @return a map of type to constructor that taks the config
   */
  private static Map<Class<?>, Constructor<?>> initConfigConstructors(Collection<Class<? extends FormatPlugin>> pluginClasses) {
    Map<Class<?>, Constructor<?>> constructors = Maps.newHashMap();
    for (Class<? extends FormatPlugin> pluginClass: pluginClasses) {
      for (Constructor<?> c : pluginClass.getConstructors()) {
        try {
          if (!FORMAT_BASED.check(c)) {
            continue;
          }
          Class<?> configClass = c.getParameterTypes()[4];
          constructors.put(configClass, c);
        } catch (Exception e) {
          logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
        }
      }
    }
    return constructors;
  }


  private final DrillbitContext context;
  private final Configuration fsConf;
  private final FileSystemConfig storageConfig;

  /** format plugins initialized from the drill config, indexed by name */
  private final Map<String, FormatPlugin> pluginsByName;

  /** format plugins initialized from the drill config, indexed by {@link FormatPluginConfig} */
  private Map<FormatPluginConfig, FormatPlugin> pluginsByConfig;

  /** FormatMatchers for all configured plugins */
  private List<FormatMatcher> formatMatchers;

  /** The format plugin classes retrieved from classpath scanning */
  private final Collection<Class<? extends FormatPlugin>> pluginClasses;
  /** a Map from the FormatPlugin Config class to the constructor of the format plugin that accepts it.*/
  private final Map<Class<?>, Constructor<?>> configConstructors;

  FormatCreator(
      DrillbitContext context,
      Configuration fsConf,
      FileSystemConfig storageConfig,
      ScanResult classpathScan) {
    this.context = context;
    this.fsConf = fsConf;
    this.storageConfig = storageConfig;
    this.pluginClasses = classpathScan.getImplementations(FormatPlugin.class);
    this.configConstructors = initConfigConstructors(pluginClasses);

    Map<String, FormatPlugin> pluginsByName = Maps.newHashMap();
    Map<FormatPluginConfig, FormatPlugin> pluginsByConfig = Maps.newHashMap();
    List<FormatMatcher> formatMatchers = Lists.newArrayList();
    final Map<String, FormatPluginConfig> formats = storageConfig.getFormats();
    if (formats == null || formats.isEmpty()) {
      for (Class<? extends FormatPlugin> pluginClass: pluginClasses) {
        for (Constructor<?> c : pluginClass.getConstructors()) {
          try {
            if (!DEFAULT_BASED.check(c)) {
              continue;
            }
            FormatPlugin plugin = (FormatPlugin) c.newInstance(null, context, fsConf, storageConfig);
            pluginsByName.put(plugin.getName(), plugin);
            pluginsByConfig.put(plugin.getConfig(), plugin);
            formatMatchers.add(plugin.getMatcher());
          } catch (Exception e) {
            logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
          }
        }
      }
    } else {
      for (Map.Entry<String, FormatPluginConfig> e : formats.entrySet()) {
        Constructor<?> c = configConstructors.get(e.getValue().getClass());
        if (c == null) {
          logger.warn("Unable to find constructor for storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName());
          continue;
        }
        try {
          FormatPlugin formatPlugin = (FormatPlugin) c.newInstance(e.getKey(), context, fsConf, storageConfig, e.getValue());
          pluginsByName.put(e.getKey(), formatPlugin);
          pluginsByConfig.put(formatPlugin.getConfig(), formatPlugin);
          formatMatchers.add(formatPlugin.getMatcher());
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
          logger.warn("Failure initializing storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName(), e1);
        }
      }
    }
    this.pluginsByName = Collections.unmodifiableMap(pluginsByName);
    this.pluginsByConfig = Collections.unmodifiableMap(pluginsByConfig);
    this.formatMatchers = Collections.unmodifiableList(formatMatchers);
  }

  /**
   * @param name the name of the formatplugin instance in the drill config
   * @return The configured FormatPlugin for this name
   */
  public FormatPlugin getFormatPluginByName(String name) {
    return pluginsByName.get(name);
  }

  /**
   * @param formatConfig {@link FormatPluginConfig} of the format plugin
   * @return The configured FormatPlugin for the given format config.
   */
  public FormatPlugin getFormatPluginByConfig(FormatPluginConfig formatConfig) {
    if (formatConfig instanceof NamedFormatPluginConfig) {
      return getFormatPluginByName(((NamedFormatPluginConfig) formatConfig).name);
    } else {
      return pluginsByConfig.get(formatConfig);
    }
  }

  /**
   * @return List of format matchers for all configured format plugins.
   */
  public List<FormatMatcher> getFormatMatchers() {
    return formatMatchers;
  }

  /**
   * @return all the format plugins from the Drill config
   */
  public Collection<FormatPlugin> getConfiguredFormatPlugins() {
    return pluginsByName.values();
  }

  /**
   * Instantiate a new format plugin instance from the provided config object
   * @param fpconfig the conf for the plugin
   * @return the newly created instance of a FormatPlugin based on provided config
   */
  public FormatPlugin newFormatPlugin(FormatPluginConfig fpconfig) {
    Constructor<?> c = configConstructors.get(fpconfig.getClass());
    if (c == null) {
      throw UserException.dataReadError()
        .message(
            "Unable to find constructor for storage config of type %s",
            fpconfig.getClass().getName())
        .build(logger);
    }
    try {
      return (FormatPlugin) c.newInstance(null, context, fsConf, storageConfig, fpconfig);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw UserException.dataReadError(e)
        .message(
            "Failure initializing storage config of type %s",
            fpconfig.getClass().getName())
        .build(logger);
    }
  }
}
