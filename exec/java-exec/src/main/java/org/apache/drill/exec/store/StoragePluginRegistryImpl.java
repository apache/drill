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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.planner.sql.DynamicSchema;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.ischema.InfoSchemaConfig;
import org.apache.drill.exec.store.ischema.InfoSchemaStoragePlugin;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;
import org.apache.drill.exec.store.sys.SystemTablePlugin;
import org.apache.drill.exec.store.sys.SystemTablePluginConfig;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;

public class StoragePluginRegistryImpl implements StoragePluginRegistry {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistryImpl.class);

  private Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = Collections.emptyMap();
  private final StoragePluginMap plugins = new StoragePluginMap();

  private DrillbitContext context;
  private final DrillSchemaFactory schemaFactory = new DrillSchemaFactory();
  private final PersistentStore<StoragePluginConfig> pluginSystemTable;
  private final LogicalPlanPersistence lpPersistence;
  private final ScanResult classpathScan;
  private final LoadingCache<StoragePluginConfig, StoragePlugin> ephemeralPlugins;

  public StoragePluginRegistryImpl(DrillbitContext context) {
    this.context = checkNotNull(context);
    this.lpPersistence = checkNotNull(context.getLpPersistence());
    this.classpathScan = checkNotNull(context.getClasspathScan());
    try {
      this.pluginSystemTable = context //
          .getStoreProvider() //
          .getOrCreateStore(PersistentStoreConfig //
              .newJacksonBuilder(lpPersistence.getMapper(), StoragePluginConfig.class) //
              .name(PSTORE_NAME) //
              .build());
    } catch (StoreException | RuntimeException e) {
      logger.error("Failure while loading storage plugin registry.", e);
      throw new RuntimeException("Failure while reading and loading storage plugin configuration.", e);
    }

    ephemeralPlugins = CacheBuilder.newBuilder()
        .expireAfterAccess(24, TimeUnit.HOURS)
        .maximumSize(250)
        .removalListener(new RemovalListener<StoragePluginConfig, StoragePlugin>() {
          @Override
          public void onRemoval(RemovalNotification<StoragePluginConfig, StoragePlugin> notification) {
            closePlugin(notification.getValue());
          }
        })
        .build(new CacheLoader<StoragePluginConfig, StoragePlugin>() {
          @Override
          public StoragePlugin load(StoragePluginConfig config) throws Exception {
            return create(null, config);
          }
        });
  }

  @Override
  public PersistentStore<StoragePluginConfig> getStore() {
    return pluginSystemTable;
  }

  @Override
  public void init() throws DrillbitStartupException {
    availablePlugins = findAvailablePlugins(classpathScan);

    // create registered plugins defined in "storage-plugins.json"
    plugins.putAll(createPlugins());
  }

  @SuppressWarnings("resource")
  private Map<String, StoragePlugin> createPlugins() throws DrillbitStartupException {
    try {
      /*
       * Check if the storage plugins system table has any entries. If not, load the boostrap-storage-plugin file into
       * the system table.
       */
      if (!pluginSystemTable.getAll().hasNext()) {
        // bootstrap load the config since no plugins are stored.
        logger.info("No storage plugin instances configured in persistent store, loading bootstrap configuration.");
        Collection<URL> urls = ClassPathScanner.forResource(ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE, false);
        if (urls != null && !urls.isEmpty()) {
          logger.info("Loading the storage plugin configs from URLs {}.", urls);
          Map<String, URL> pluginURLMap = Maps.newHashMap();
          for (URL url : urls) {
            String pluginsData = Resources.toString(url, Charsets.UTF_8);
            StoragePlugins plugins = lpPersistence.getMapper().readValue(pluginsData, StoragePlugins.class);
            for (Map.Entry<String, StoragePluginConfig> config : plugins) {
              if (!definePluginConfig(config.getKey(), config.getValue())) {
                logger.warn("Duplicate plugin instance '{}' defined in [{}, {}], ignoring the later one.",
                    config.getKey(), pluginURLMap.get(config.getKey()), url);
                continue;
              }
              pluginURLMap.put(config.getKey(), url);
            }
          }
        } else {
          throw new IOException("Failure finding " + ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE);
        }
      }

      Map<String, StoragePlugin> activePlugins = new HashMap<String, StoragePlugin>();
      for (Map.Entry<String, StoragePluginConfig> entry : Lists.newArrayList(pluginSystemTable.getAll())) {
        String name = entry.getKey();
        StoragePluginConfig config = entry.getValue();
        if (config.isEnabled()) {
          try {
            StoragePlugin plugin = create(name, config);
            activePlugins.put(name, plugin);
          } catch (ExecutionSetupException e) {
            logger.error("Failure while setting up StoragePlugin with name: '{}', disabling.", name, e);
            config.setEnabled(false);
            pluginSystemTable.put(name, config);
          }
        }
      }

      activePlugins.put(INFORMATION_SCHEMA_PLUGIN, new InfoSchemaStoragePlugin(new InfoSchemaConfig(), context,
          INFORMATION_SCHEMA_PLUGIN));
      activePlugins.put(SYS_PLUGIN, new SystemTablePlugin(SystemTablePluginConfig.INSTANCE, context, SYS_PLUGIN));

      return activePlugins;
    } catch (IOException e) {
      logger.error("Failure setting up storage plugins.  Drillbit exiting.", e);
      throw new IllegalStateException(e);
    }
  }

  /**
   * Add a plugin and configuration. Assumes neither exists. Primarily
   * for testing.
   *
   * @param name plugin name
   * @param config plugin config
   * @param plugin plugin implementation
   */

  public void definePlugin(String name, StoragePluginConfig config, StoragePlugin plugin) {
    addPlugin(name, plugin);
    definePluginConfig(name, config);
  }

  private boolean definePluginConfig(String name, StoragePluginConfig config) {
    return pluginSystemTable.putIfAbsent(name, config);
  }

  @Override
  public void addPlugin(String name, StoragePlugin plugin) {
    plugins.put(name, plugin);
  }

  @Override
  public void deletePlugin(String name) {
    @SuppressWarnings("resource")
    StoragePlugin plugin = plugins.remove(name);
    closePlugin(plugin);
    pluginSystemTable.delete(name);
  }

  private void closePlugin(StoragePlugin plugin) {
    if (plugin == null) {
      return;
    }

    try {
      plugin.close();
    } catch (Exception e) {
      logger.warn("Exception while shutting down storage plugin.");
    }
  }

  @SuppressWarnings("resource")
  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, boolean persist)
      throws ExecutionSetupException {
    for (;;) {
      final StoragePlugin oldPlugin = plugins.get(name);
      final StoragePlugin newPlugin = create(name, config);
      boolean done = false;
      try {
        if (oldPlugin != null) {
          if (config.isEnabled()) {
            done = plugins.replace(name, oldPlugin, newPlugin);
          } else {
            done = plugins.remove(name, oldPlugin);
          }
          if (done) {
            closePlugin(oldPlugin);
          }
        } else if (config.isEnabled()) {
          done = (null == plugins.putIfAbsent(name, newPlugin));
        } else {
          done = true;
        }
      } finally {
        if (!done) {
          closePlugin(newPlugin);
        }
      }

      if (done) {
        if (persist) {
          pluginSystemTable.put(name, config);
        }

        return newPlugin;
      }
    }
  }

  @Override
  public StoragePlugin getPlugin(String name) throws ExecutionSetupException {
    StoragePlugin plugin = plugins.get(name);
    if (name.equals(SYS_PLUGIN) || name.equals(INFORMATION_SCHEMA_PLUGIN)) {
      return plugin;
    }

    // since we lazily manage the list of plugins per server, we need to update this once we know that it is time.
    StoragePluginConfig config = this.pluginSystemTable.get(name);
    if (config == null) {
      if (plugin != null) {
        plugins.remove(name);
      }
      return null;
    } else {
      if (plugin == null
          || !plugin.getConfig().equals(config)
          || plugin.getConfig().isEnabled() != config.isEnabled()) {
        plugin = createOrUpdate(name, config, false);
      }
      return plugin;
    }
  }


  @Override
  public StoragePlugin getPlugin(StoragePluginConfig config) throws ExecutionSetupException {
    if (config instanceof NamedStoragePluginConfig) {
      return getPlugin(((NamedStoragePluginConfig) config).name);
    } else {
      // try to lookup plugin by configuration
      StoragePlugin plugin = plugins.get(config);
      if (plugin != null) {
        return plugin;
      }

      // no named plugin matches the desired configuration, let's create an
      // ephemeral storage plugin (or get one from the cache)
      try {
        return ephemeralPlugins.get(config);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ExecutionSetupException) {
          throw (ExecutionSetupException) cause;
        } else {
          // this shouldn't happen. here for completeness.
          throw new ExecutionSetupException("Failure while trying to create ephemeral plugin.", cause);
        }
      }
    }
  }

  @SuppressWarnings("resource")
  @Override
  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig)
      throws ExecutionSetupException {
    StoragePlugin p = getPlugin(storageConfig);
    if (!(p instanceof FileSystemPlugin)) {
      throw new ExecutionSetupException(
          String.format("You tried to request a format plugin for a storage plugin that wasn't of type "
              + "FileSystemPlugin. The actual type of plugin was %s.", p.getClass().getName()));
    }
    FileSystemPlugin storage = (FileSystemPlugin) p;
    return storage.getFormatPlugin(formatConfig);
  }

  private StoragePlugin create(String name, StoragePluginConfig pluginConfig) throws ExecutionSetupException {
    StoragePlugin plugin = null;
    Constructor<? extends StoragePlugin> c = availablePlugins.get(pluginConfig.getClass());
    if (c == null) {
      throw new ExecutionSetupException(String.format("Failure finding StoragePlugin constructor for config %s",
          pluginConfig));
    }
    try {
      plugin = c.newInstance(pluginConfig, context, name);
      plugin.start();
      return plugin;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | IOException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException) {
        throw ((ExecutionSetupException) t);
      }
      throw new ExecutionSetupException(String.format(
          "Failure setting up new storage plugin configuration for config %s", pluginConfig), t);
    }
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return plugins.iterator();
  }

  @Override
  public SchemaFactory getSchemaFactory() {
    return schemaFactory;
  }

  public class DrillSchemaFactory implements SchemaFactory {

    @SuppressWarnings("resource")
    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
      Stopwatch watch = Stopwatch.createStarted();

      try {
        Set<String> currentPluginNames = Sets.newHashSet(plugins.names());
        // iterate through the plugin instances in the persistence store adding
        // any new ones and refreshing those whose configuration has changed
        for (Map.Entry<String, StoragePluginConfig> config : Lists.newArrayList(pluginSystemTable.getAll())) {
          if (config.getValue().isEnabled()) {
            getPlugin(config.getKey());
            currentPluginNames.remove(config.getKey());
          }
        }
        // remove those which are no longer in the registry
        for (String pluginName : currentPluginNames) {
          if (pluginName.equals(SYS_PLUGIN) || pluginName.equals(INFORMATION_SCHEMA_PLUGIN)) {
            continue;
          }
          plugins.remove(pluginName);
        }

        // finally register schemas with the refreshed plugins
        for (StoragePlugin plugin : plugins.plugins()) {
          plugin.registerSchemas(schemaConfig, parent);
        }
      } catch (ExecutionSetupException e) {
        throw new DrillRuntimeException("Failure while updating storage plugins", e);
      }

      // Add second level schema as top level schema with name qualified with parent schema name
      // Ex: "dfs" schema has "default" and "tmp" as sub schemas. Add following extra schemas "dfs.default" and
      // "dfs.tmp" under root schema.
      //
      // Before change, schema tree looks like below:
      // "root"
      // -- "dfs"
      // -- "default"
      // -- "tmp"
      // -- "hive"
      // -- "default"
      // -- "hivedb1"
      //
      // After the change, the schema tree looks like below:
      // "root"
      // -- "dfs"
      // -- "default"
      // -- "tmp"
      // -- "dfs.default"
      // -- "dfs.tmp"
      // -- "hive"
      // -- "default"
      // -- "hivedb1"
      // -- "hive.default"
      // -- "hive.hivedb1"
      List<SchemaPlus> secondLevelSchemas = Lists.newArrayList();
      for (String firstLevelSchemaName : parent.getSubSchemaNames()) {
        SchemaPlus firstLevelSchema = parent.getSubSchema(firstLevelSchemaName);
        for (String secondLevelSchemaName : firstLevelSchema.getSubSchemaNames()) {
          secondLevelSchemas.add(firstLevelSchema.getSubSchema(secondLevelSchemaName));
        }
      }

      for (SchemaPlus schema : secondLevelSchemas) {
        AbstractSchema drillSchema;
        try {
          drillSchema = schema.unwrap(AbstractSchema.class);
        } catch (ClassCastException e) {
          throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
        }
        SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
        parent.add(wrapper.getName(), wrapper);
      }

      logger.debug("Took {} ms to register schemas.", watch.elapsed(TimeUnit.MILLISECONDS));
    }

  }

  @Override
  public synchronized void close() throws Exception {
    ephemeralPlugins.invalidateAll();
    plugins.close();
    pluginSystemTable.close();
  }

  /**
   * Get a list of all available storage plugin class constructors.
   * @param classpathScan
   *          A classpath scan to use.
   * @return A Map of StoragePluginConfig => StoragePlugin.<init>() constructors.
   */
  @SuppressWarnings("unchecked")
  public static Map<Object, Constructor<? extends StoragePlugin>> findAvailablePlugins(final ScanResult classpathScan) {
    Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = new HashMap<Object, Constructor<? extends StoragePlugin>>();
    final Collection<Class<? extends StoragePlugin>> pluginClasses =
        classpathScan.getImplementations(StoragePlugin.class);
    final String lineBrokenList =
        pluginClasses.size() == 0
            ? "" : "\n\t- " + Joiner.on("\n\t- ").join(pluginClasses);
    logger.debug("Found {} storage plugin configuration classes: {}.",
        pluginClasses.size(), lineBrokenList);
    for (Class<? extends StoragePlugin> plugin : pluginClasses) {
      int i = 0;
      for (Constructor<?> c : plugin.getConstructors()) {
        Class<?>[] params = c.getParameterTypes();
        if (params.length != 3
            || params[1] != DrillbitContext.class
            || !StoragePluginConfig.class.isAssignableFrom(params[0])
            || params[2] != String.class) {
          logger.info("Skipping StoragePlugin constructor {} for plugin class {} since it doesn't implement a "
              + "[constructor(StoragePluginConfig, DrillbitContext, String)]", c, plugin);
          continue;
        }
        availablePlugins.put(params[0], (Constructor<? extends StoragePlugin>) c);
        i++;
      }
      if (i == 0) {
        logger.debug("Skipping registration of StoragePlugin {} as it doesn't have a constructor with the parameters "
            + "of (StorangePluginConfig, Config)", plugin.getCanonicalName());
      }
    }
    return availablePlugins;
  }



}
