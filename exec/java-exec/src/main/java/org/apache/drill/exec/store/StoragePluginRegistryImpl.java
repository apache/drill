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

import static org.apache.drill.shaded.guava.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.StoreException;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.sys.CaseInsensitivePersistentStore;
import org.apache.drill.exec.store.sys.PersistentStore;
import org.apache.drill.exec.store.sys.PersistentStoreConfig;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.base.Joiner;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.cache.RemovalListener;
import org.apache.drill.shaded.guava.com.google.common.io.Resources;

public class StoragePluginRegistryImpl implements StoragePluginRegistry {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistryImpl.class);

  private final StoragePluginMap enabledPlugins;
  private final DrillSchemaFactory schemaFactory;
  private final DrillbitContext context;
  private final LogicalPlanPersistence lpPersistence;
  private final ScanResult classpathScan;
  private final PersistentStore<StoragePluginConfig> pluginSystemTable;
  private final LoadingCache<StoragePluginConfig, StoragePlugin> ephemeralPlugins;

  private Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = Collections.emptyMap();
  private Map<String, StoragePlugin> systemPlugins = Collections.emptyMap();

  public StoragePluginRegistryImpl(DrillbitContext context) {
    this.enabledPlugins = new StoragePluginMap();
    this.schemaFactory = new DrillSchemaFactory(null);
    this.context = checkNotNull(context);
    this.lpPersistence = checkNotNull(context.getLpPersistence());
    this.classpathScan = checkNotNull(context.getClasspathScan());
    this.pluginSystemTable = initPluginsSystemTable(context, lpPersistence);
    this.ephemeralPlugins = CacheBuilder.newBuilder()
        .expireAfterAccess(24, TimeUnit.HOURS)
        .maximumSize(250)
        .removalListener(
            (RemovalListener<StoragePluginConfig, StoragePlugin>) notification -> closePlugin(notification.getValue()))
        .build(new CacheLoader<StoragePluginConfig, StoragePlugin>() {
          @Override
          public StoragePlugin load(StoragePluginConfig config) throws Exception {
            return create(null, config);
          }
        });
  }

  @Override
  public void init() {
    availablePlugins = findAvailablePlugins(classpathScan);
    systemPlugins = initSystemPlugins(classpathScan, context);
    try {
      StoragePlugins bootstrapPlugins = pluginSystemTable.getAll().hasNext() ? null : loadBootstrapPlugins(lpPersistence);

      StoragePluginsHandler storagePluginsHandler = new StoragePluginsHandlerService(context);
      storagePluginsHandler.loadPlugins(pluginSystemTable, bootstrapPlugins);

      defineEnabledPlugins();
    } catch (IOException e) {
      logger.error("Failure setting up storage enabledPlugins.  Drillbit exiting.", e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void deletePlugin(String name) {
    StoragePlugin plugin = enabledPlugins.remove(name);
    closePlugin(plugin);
    pluginSystemTable.delete(name);
  }

  @Override
  public StoragePlugin createOrUpdate(String name, StoragePluginConfig config, boolean persist) throws ExecutionSetupException {
    for (;;) {
      StoragePlugin oldPlugin = enabledPlugins.get(name);
      StoragePlugin newPlugin = create(name, config);
      boolean done = false;
      try {
        if (oldPlugin != null) {
          if (config.isEnabled()) {
            done = enabledPlugins.replace(name, oldPlugin, newPlugin);
          } else {
            done = enabledPlugins.remove(name, oldPlugin);
          }
          if (done) {
            closePlugin(oldPlugin);
          }
        } else if (config.isEnabled()) {
          done = (null == enabledPlugins.putIfAbsent(name, newPlugin));
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
    StoragePlugin plugin = enabledPlugins.get(name);
    if (systemPlugins.get(name) != null) {
      return plugin;
    }

    // since we lazily manage the list of plugins per server, we need to update this once we know that it is time.
    StoragePluginConfig config = pluginSystemTable.get(name);
    if (config == null) {
      if (plugin != null) {
        enabledPlugins.remove(name);
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
      return getPlugin(((NamedStoragePluginConfig) config).getName());
    } else {
      // try to lookup plugin by configuration
      StoragePlugin plugin = enabledPlugins.get(config);
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

  @Override
  public void addEnabledPlugin(String name, StoragePlugin plugin) {
    enabledPlugins.put(name, plugin);
  }

  @Override
  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig) throws ExecutionSetupException {
    StoragePlugin storagePlugin = getPlugin(storageConfig);
    return storagePlugin.getFormatPlugin(formatConfig);
  }

  @Override
  public PersistentStore<StoragePluginConfig> getStore() {
    return pluginSystemTable;
  }

  @Override
  public SchemaFactory getSchemaFactory() {
    return schemaFactory;
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return enabledPlugins.iterator();
  }

  @Override
  public synchronized void close() throws Exception {
    ephemeralPlugins.invalidateAll();
    enabledPlugins.close();
    pluginSystemTable.close();
  }

  /**
   * Add a plugin and configuration. Assumes neither exists. Primarily for testing.
   *
   * @param config plugin config
   * @param plugin plugin implementation
   */
  @VisibleForTesting
  public void addPluginToPersistentStoreIfAbsent(String name, StoragePluginConfig config, StoragePlugin plugin) {
    addEnabledPlugin(name, plugin);
    pluginSystemTable.putIfAbsent(name, config);
  }

  /**
   * <ol>
   *   <li>Initializes persistent store for storage plugins.</li>
   *   <li>Since storage plugins names are case-insensitive in Drill, to ensure backward compatibility,
   *   re-writes those not stored in lower case with lower case names, for duplicates issues warning. </li>
   *   <li>Wraps plugin system table into case insensitive wrapper.</li>
   * </ol>
   *
   * @param context drillbit context
   * @param lpPersistence deserialization mapper provider
   * @return persistent store for storage plugins
   */
  private PersistentStore<StoragePluginConfig> initPluginsSystemTable(DrillbitContext context, LogicalPlanPersistence lpPersistence) {

    try {
      PersistentStore<StoragePluginConfig> pluginSystemTable = context
          .getStoreProvider()
          .getOrCreateStore(PersistentStoreConfig
              .newJacksonBuilder(lpPersistence.getMapper(), StoragePluginConfig.class)
              .name(PSTORE_NAME)
              .build());

      Iterator<Entry<String, StoragePluginConfig>> storedPlugins = pluginSystemTable.getAll();
      while (storedPlugins.hasNext()) {
        Entry<String, StoragePluginConfig> entry = storedPlugins.next();
        String pluginName = entry.getKey();
        if (!pluginName.equals(pluginName.toLowerCase())) {
          logger.debug("Replacing plugin name {} with its lower case equivalent.", pluginName);
          pluginSystemTable.delete(pluginName);
          if (!pluginSystemTable.putIfAbsent(pluginName.toLowerCase(), entry.getValue())) {
            logger.warn("Duplicated storage plugin name [{}] is found. Duplicate is deleted from persistent storage.", pluginName);
          }
        }
      }

      return new CaseInsensitivePersistentStore<>(pluginSystemTable);
    } catch (StoreException e) {
      logger.error("Failure while loading storage plugin registry.", e);
      throw new DrillRuntimeException("Failure while reading and loading storage plugin configuration.", e);
    }
  }

  /**
   * Read bootstrap storage plugins {@link ExecConstants#BOOTSTRAP_STORAGE_PLUGINS_FILE} files for the first fresh
   * instantiating of Drill
   *
   * @param lpPersistence deserialization mapper provider
   * @return bootstrap storage plugins
   * @throws IOException if a read error occurs
   */
  private StoragePlugins loadBootstrapPlugins(LogicalPlanPersistence lpPersistence) throws IOException {
    // bootstrap load the config since no plugins are stored.
    logger.info("No storage plugin instances configured in persistent store, loading bootstrap configuration.");
    Set<URL> urls = ClassPathScanner.forResource(ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE, false);
    if (urls != null && !urls.isEmpty()) {
      logger.info("Loading the storage plugin configs from URLs {}.", urls);
      StoragePlugins bootstrapPlugins = new StoragePlugins(new HashMap<>());
      Map<String, URL> pluginURLMap = new HashMap<>();
      for (URL url : urls) {
        String pluginsData = Resources.toString(url, Charsets.UTF_8);
        StoragePlugins plugins = lpPersistence.getMapper().readValue(pluginsData, StoragePlugins.class);
        for (Entry<String, StoragePluginConfig> plugin : plugins) {
          StoragePluginConfig oldPluginConfig = bootstrapPlugins.putIfAbsent(plugin.getKey(), plugin.getValue());
          if (oldPluginConfig != null) {
            logger.warn("Duplicate plugin instance '{}' defined in [{}, {}], ignoring the later one.",
                plugin.getKey(), pluginURLMap.get(plugin.getKey()), url);
          } else {
            pluginURLMap.put(plugin.getKey(), url);
          }
        }
      }
      return bootstrapPlugins;
    } else {
      throw new IOException("Failure finding " + ExecConstants.BOOTSTRAP_STORAGE_PLUGINS_FILE);
    }
  }

  /**
   * Dynamically loads system plugins annotated with {@link SystemPlugin}.
   * Will skip plugin initialization if no matching constructor, incorrect class implementation, name absence are detected.
   *
   * @param classpathScan classpath scan result
   * @param context drillbit context
   * @return map with system plugins stored by name
   */
  private Map<String, StoragePlugin> initSystemPlugins(ScanResult classpathScan, DrillbitContext context) {
    Map<String, StoragePlugin> plugins = CaseInsensitiveMap.newHashMap();
    List<AnnotatedClassDescriptor> annotatedClasses = classpathScan.getAnnotatedClasses(SystemPlugin.class.getName());
    logger.trace("Found {} annotated classes with SystemPlugin annotation: {}.", annotatedClasses.size(), annotatedClasses);

    for (AnnotatedClassDescriptor annotatedClass : annotatedClasses) {
      try {
        Class<?> aClass = Class.forName(annotatedClass.getClassName());
        boolean isPluginInitialized = false;

        for (Constructor<?> constructor : aClass.getConstructors()) {
          Class<?>[] parameterTypes = constructor.getParameterTypes();

          if (parameterTypes.length != 1 || parameterTypes[0] != DrillbitContext.class) {
            logger.trace("Not matching constructor for {}. Expecting constructor with one parameter for DrillbitContext class.",
                annotatedClass.getClassName());
            continue;
          }

          Object instance = constructor.newInstance(context);
          if (!(instance instanceof StoragePlugin)) {
            logger.debug("Created instance of {} does not implement StoragePlugin interface.", annotatedClass.getClassName());
            continue;
          }

          StoragePlugin storagePlugin = (StoragePlugin) instance;
          String name = storagePlugin.getName();
          if (name == null) {
            logger.debug("Storage plugin name {} is not defined. Skipping plugin initialization.", annotatedClass.getClassName());
            continue;
          }
          storagePlugin.getConfig().setEnabled(true);
          plugins.put(name, storagePlugin);
          isPluginInitialized = true;

        }
        if (!isPluginInitialized) {
          logger.debug("Skipping plugin registration, did not find matching constructor or initialized object of wrong type.");
        }
      } catch (ReflectiveOperationException e) {
        logger.warn("Error during system plugin {} initialization. Plugin initialization will be skipped.", annotatedClass.getClassName(), e);
      }
    }
    logger.trace("The following system plugins have been initialized: {}.", plugins.keySet());
    return plugins;
  }

  /**
   * Get a list of all available storage plugin class constructors.
   * @param classpathScan A classpath scan to use.
   * @return A Map of StoragePluginConfig => StoragePlugin.<init>() constructors.
   */
  @SuppressWarnings("unchecked")
  private Map<Object, Constructor<? extends StoragePlugin>> findAvailablePlugins(final ScanResult classpathScan) {
    Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = new HashMap<>();
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
          logger.debug("Skipping StoragePlugin constructor {} for plugin class {} since it doesn't implement a "
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

  /**
   * It initializes {@link #enabledPlugins} with currently enabled plugins
   */
  private void defineEnabledPlugins() {
    Map<String, StoragePlugin> activePlugins = new HashMap<>();
    Iterator<Entry<String, StoragePluginConfig>> allPlugins = pluginSystemTable.getAll();
    while (allPlugins.hasNext()) {
      Entry<String, StoragePluginConfig> plugin = allPlugins.next();
      String name = plugin.getKey();
      StoragePluginConfig config = plugin.getValue();
      if (config.isEnabled()) {
        try {
          StoragePlugin storagePlugin = create(name, config);
          activePlugins.put(name, storagePlugin);
        } catch (ExecutionSetupException e) {
          logger.error("Failure while setting up StoragePlugin with name: '{}', disabling.", name, e);
          config.setEnabled(false);
          pluginSystemTable.put(name, config);
        }
      }
    }

    activePlugins.putAll(systemPlugins);
    enabledPlugins.putAll(activePlugins);
  }

  private StoragePlugin create(String name, StoragePluginConfig pluginConfig) throws ExecutionSetupException {
    // TODO: DRILL-6412: clients for storage plugins shouldn't be created, if storage plugin is disabled
    // Creating of the StoragePlugin leads to instantiating storage clients
    StoragePlugin plugin;
    Constructor<? extends StoragePlugin> c = availablePlugins.get(pluginConfig.getClass());
    if (c == null) {
      throw new ExecutionSetupException(String.format("Failure finding StoragePlugin constructor for config %s",
          pluginConfig));
    }
    try {
      plugin = c.newInstance(pluginConfig, context, name);
      plugin.start();
      return plugin;
    } catch (ReflectiveOperationException | IOException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException) {
        throw ((ExecutionSetupException) t);
      }
      throw new ExecutionSetupException(String.format("Failure setting up new storage plugin configuration for config %s", pluginConfig), t);
    }
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

  public class DrillSchemaFactory extends AbstractSchemaFactory {

    public DrillSchemaFactory(String name) {
      super(name);
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
      Stopwatch watch = Stopwatch.createStarted();

      try {
        Set<String> currentPluginNames = new HashSet<>(enabledPlugins.getNames());
        // iterate through the plugin instances in the persistent store adding
        // any new ones and refreshing those whose configuration has changed
        Iterator<Entry<String, StoragePluginConfig>> allPlugins = pluginSystemTable.getAll();
        while (allPlugins.hasNext()) {
          Entry<String, StoragePluginConfig> plugin = allPlugins.next();
          if (plugin.getValue().isEnabled()) {
            getPlugin(plugin.getKey());
            currentPluginNames.remove(plugin.getKey());
          }
        }
        // remove those which are no longer in the registry
        for (String pluginName : currentPluginNames) {
          if (systemPlugins.get(pluginName) != null) {
            continue;
          }
          enabledPlugins.remove(pluginName);
        }

        // finally register schemas with the refreshed plugins
        for (StoragePlugin plugin : enabledPlugins.plugins()) {
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
      List<SchemaPlus> secondLevelSchemas = new ArrayList<>();
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

}
