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
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.ConstructorChecker;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;

import com.google.common.collect.Maps;

public class FormatCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatCreator.class);

  static final ConstructorChecker FORMAT_BASED = new ConstructorChecker(String.class, DrillbitContext.class, DrillFileSystem.class, StoragePluginConfig.class, FormatPluginConfig.class);
  static final ConstructorChecker DEFAULT_BASED = new ConstructorChecker(String.class, DrillbitContext.class, DrillFileSystem.class, StoragePluginConfig.class);

  static Map<String, FormatPlugin> getFormatPlugins(DrillbitContext context, DrillFileSystem fileSystem, FileSystemConfig storageConfig) {
    final DrillConfig config = context.getConfig();
    Map<String, FormatPlugin> plugins = Maps.newHashMap();

    Collection<Class<? extends FormatPlugin>> pluginClasses = PathScanner.scanForImplementations(FormatPlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));


    if (storageConfig.formats == null || storageConfig.formats.isEmpty()) {

      for (Class<? extends FormatPlugin> pluginClass: pluginClasses) {
        for (Constructor<?> c : pluginClass.getConstructors()) {
          try {
            if (!DEFAULT_BASED.check(c)) {
              continue;
            }
            FormatPlugin plugin = (FormatPlugin) c.newInstance(null, context, fileSystem, storageConfig);
            plugins.put(plugin.getName(), plugin);
          } catch (Exception e) {
            logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
          }
        }
      }

    } else {
      Map<Class<?>, Constructor<?>> constructors = Maps.newHashMap();
      for (Class<? extends FormatPlugin> pluginClass: pluginClasses) {
        for (Constructor<?> c : pluginClass.getConstructors()) {
          try {
            if (!FORMAT_BASED.check(c)) {
              continue;
            }
            Class<? extends FormatPluginConfig> configClass = (Class<? extends FormatPluginConfig>) c.getParameterTypes()[4];
            constructors.put(configClass, c);
          } catch (Exception e) {
            logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
          }
        }
      }

      for (Map.Entry<String, FormatPluginConfig> e : storageConfig.formats.entrySet()) {
        Constructor<?> c = constructors.get(e.getValue().getClass());
        if (c == null) {
          logger.warn("Unable to find constructor for storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName());
          continue;
        }
        try {
          plugins.put(e.getKey(), (FormatPlugin) c.newInstance(e.getKey(), context, fileSystem, storageConfig, e.getValue()));
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
          logger.warn("Failure initializing storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName(), e1);
        }
      }

    }

    return plugins;
  }

}
