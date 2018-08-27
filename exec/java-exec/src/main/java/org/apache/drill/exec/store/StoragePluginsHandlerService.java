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
package org.apache.drill.exec.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;
import com.jasonclawson.jackson.dataformat.hocon.HoconFactory;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.util.ActionOnFile;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.drill.exec.store.StoragePluginRegistry.ACTION_ON_STORAGE_PLUGINS_OVERRIDE_FILE;

/**
 * Drill plugins handler, which allows to update storage plugins configs from the
 * {@link CommonConstants#STORAGE_PLUGINS_OVERRIDE_CONF} conf file
 *
 * TODO: DRILL-6564: It can be improved with configs versioning and service of creating
 * {@link CommonConstants#STORAGE_PLUGINS_OVERRIDE_CONF}
 */
public class StoragePluginsHandlerService implements StoragePluginsHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginsHandlerService.class);

  private final LogicalPlanPersistence lpPersistence;
  private final DrillbitContext context;
  private URL pluginsOverrideFileUrl;

  public StoragePluginsHandlerService(DrillbitContext context) {
    this.context = context;
    this.lpPersistence = new LogicalPlanPersistence(context.getConfig(), context.getClasspathScan(),
        new ObjectMapper(new HoconFactory()));
  }

  @Override
  public void loadPlugins(@NotNull PersistentStore<StoragePluginConfig> persistentStore,
                          @Nullable StoragePlugins bootstrapPlugins) {
    // if bootstrapPlugins is not null -- fresh Drill set up
    StoragePlugins pluginsForPersistentStore;

    StoragePlugins newPlugins = getNewStoragePlugins();

    if (newPlugins != null) {
      pluginsForPersistentStore = new StoragePlugins(new HashMap<>());
      Optional.ofNullable(bootstrapPlugins)
          .ifPresent(pluginsForPersistentStore::putAll);

      for (Map.Entry<String, StoragePluginConfig> newPlugin : newPlugins) {
        String pluginName = newPlugin.getKey();
        StoragePluginConfig oldPluginConfig = Optional.ofNullable(bootstrapPlugins)
            .map(plugins -> plugins.getConfig(pluginName))
            .orElse(persistentStore.get(pluginName));
        StoragePluginConfig updatedStatusPluginConfig = updatePluginStatus(oldPluginConfig, newPlugin.getValue());
        pluginsForPersistentStore.put(pluginName, updatedStatusPluginConfig);
      }
    } else {
      pluginsForPersistentStore = bootstrapPlugins;
    }

    // load pluginsForPersistentStore to Persistent Store
    Optional.ofNullable(pluginsForPersistentStore)
        .ifPresent(plugins -> plugins.forEach(plugin -> persistentStore.put(plugin.getKey(), plugin.getValue())));

    if (newPlugins != null) {
      String fileAction = context.getConfig().getString(ACTION_ON_STORAGE_PLUGINS_OVERRIDE_FILE);
      Optional<ActionOnFile> actionOnFile = Arrays.stream(ActionOnFile.values())
          .filter(action -> action.name().equalsIgnoreCase(fileAction))
          .findFirst();
      actionOnFile.ifPresent(action -> action.action(pluginsOverrideFileUrl));
      // TODO: replace with ifPresentOrElse() once the project will be on Java9
      if (!actionOnFile.isPresent()) {
        logger.error("Unknown value {} for {} boot option. Nothing will be done with file.",
            fileAction, ACTION_ON_STORAGE_PLUGINS_OVERRIDE_FILE);
      }
    }
  }

  /**
   * Helper method to identify the enabled status for new storage plugins config. If this status is absent in the updater
   * file, the status is kept from the configs, which are going to be updated
   *
   * @param oldPluginConfig current storage plugin config from Persistent Store or bootstrap config file
   * @param newPluginConfig new storage plugin config
   * @return new storage plugin config with updated enabled status
   */
  private StoragePluginConfig updatePluginStatus(@Nullable StoragePluginConfig oldPluginConfig,
                                                 @NotNull StoragePluginConfig newPluginConfig) {
    if (!newPluginConfig.isEnabledStatusPresent()) {
      boolean newStatus = oldPluginConfig != null && oldPluginConfig.isEnabled();
      newPluginConfig.setEnabled(newStatus);
    }
    return newPluginConfig;
  }

  /**
   * Get the new storage plugins from the {@link CommonConstants#STORAGE_PLUGINS_OVERRIDE_CONF} file if it exists,
   * null otherwise
   *
   * @return storage plugins
   */
  private StoragePlugins getNewStoragePlugins() {
    Set<URL> urlSet = ClassPathScanner.forResource(CommonConstants.STORAGE_PLUGINS_OVERRIDE_CONF, false);
    if (!urlSet.isEmpty()) {
      if (urlSet.size() != 1) {
        DrillRuntimeException.format("More than one %s file is placed in Drill's classpath: %s",
            CommonConstants.STORAGE_PLUGINS_OVERRIDE_CONF, urlSet);
      }
      pluginsOverrideFileUrl = urlSet.iterator().next();
      try {
        String newPluginsData = Resources.toString(pluginsOverrideFileUrl, Charsets.UTF_8);
        return lpPersistence.getMapper().readValue(newPluginsData, StoragePlugins.class);
      } catch (IOException e) {
        logger.error("Failures are obtained while loading %s file. Proceed without update",
            CommonConstants.STORAGE_PLUGINS_OVERRIDE_CONF, e);
      }
    }
    logger.trace("The {} file is absent. Proceed without updating of the storage plugins configs",
        CommonConstants.STORAGE_PLUGINS_OVERRIDE_CONF);
    return null;
  }
}
