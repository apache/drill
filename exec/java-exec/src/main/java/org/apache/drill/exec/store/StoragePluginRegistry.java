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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.RuleSet;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.ischema.InfoSchemaConfig;
import org.apache.drill.exec.store.ischema.InfoSchemaStoragePlugin;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.SystemTablePlugin;
import org.apache.drill.exec.store.sys.SystemTablePluginConfig;
import org.eigenbase.relopt.RelOptRule;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.hive12.common.collect.Maps;


public class StoragePluginRegistry implements Iterable<Map.Entry<String, StoragePlugin>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistry.class);

  private Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = new HashMap<Object, Constructor<? extends StoragePlugin>>();
  private ConcurrentMap<String, StoragePlugin> plugins;

  private DrillbitContext context;
  private final DrillSchemaFactory schemaFactory = new DrillSchemaFactory();
  private final PStore<StoragePluginConfig> pluginSystemTable;

  private RuleSet storagePluginsRuleSet;

  public StoragePluginRegistry(DrillbitContext context) {
    try{
      this.context = context;
      this.pluginSystemTable = context //
          .getPersistentStoreProvider() //
          .getPStore(PStoreConfig //
              .newJacksonBuilder(context.getConfig().getMapper(), StoragePluginConfig.class) //
              .name("sys.storage_plugins") //
              .build());
    }catch(IOException | RuntimeException e){
      logger.error("Failure while loading storage plugin registry.", e);
      throw new RuntimeException("Faiure while reading and loading storage plugin configuration.", e);
    }
  }

  @SuppressWarnings("unchecked")
  public void init() throws DrillbitStartupException {
    DrillConfig config = context.getConfig();
    Collection<Class<? extends StoragePlugin>> plugins = PathScanner.scanForImplementations(StoragePlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading storage plugins {}", plugins);
    for(Class<? extends StoragePlugin> plugin: plugins){
      int i =0;
      for(Constructor<?> c : plugin.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 3
            || params[1] != DrillbitContext.class
            || !StoragePluginConfig.class.isAssignableFrom(params[0])
            || params[2] != String.class){
          logger.info("Skipping StoragePlugin constructor {} for plugin class {} since it doesn't implement a [constructor(StoragePluginConfig, DrillbitContext, String)]", c, plugin);
          continue;
        }
        availablePlugins.put(params[0], (Constructor<? extends StoragePlugin>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of StoragePlugin {} as it doesn't have a constructor with the parameters of (StorangePluginConfig, Config)", plugin.getCanonicalName());
      }
    }

    // create registered plugins defined in "storage-plugins.json"
    this.plugins = Maps.newConcurrentMap();
    this.plugins.putAll(createPlugins());

    // query registered engines for optimizer rules and build the storage plugin RuleSet
    Builder<RelOptRule> setBuilder = ImmutableSet.builder();
    for (StoragePlugin plugin : this.plugins.values()) {
      Set<StoragePluginOptimizerRule> rules = plugin.getOptimizerRules();
      if (rules != null && rules.size() > 0) {
        setBuilder.addAll(rules);
      }
    }
    this.storagePluginsRuleSet = DrillRuleSets.create(setBuilder.build());
  }

  private Map<String, StoragePlugin> createPlugins() throws DrillbitStartupException {
    /*
     * Check if the storage plugins system table has any entries.  If not, load the boostrap-storage-plugin file into the system table.
     */
    Map<String, StoragePlugin> activePlugins = new HashMap<String, StoragePlugin>();

    try{

      if(!pluginSystemTable.iterator().hasNext()){
        // bootstrap load the config since no plugins are stored.
        logger.info("Bootstrap loading the storage plugin configs.");
        URL url = Resources.class.getClassLoader().getResource("bootstrap-storage-plugins.json");
        if (url != null) {
          String pluginsData = Resources.toString(url, Charsets.UTF_8);
          StoragePlugins plugins = context.getConfig().getMapper().readValue(pluginsData, StoragePlugins.class);

          for(Map.Entry<String, StoragePluginConfig> config : plugins){
            pluginSystemTable.put(config.getKey(), config.getValue());
          }

        }else{
          throw new IOException("Failure finding bootstrap-storage-plugins.json");
        }
      }

      for(Map.Entry<String, StoragePluginConfig> config : pluginSystemTable){
        try{
          StoragePlugin plugin = create(config.getKey(), config.getValue());
          activePlugins.put(config.getKey(), plugin);
        }catch(ExecutionSetupException e){
          logger.error("Failure while setting up StoragePlugin with name: '{}'.", config.getKey(), e);
        }
      }

    }catch(IOException e){
      logger.error("Failure setting up storage plugins.  Drillbit exiting.", e);
      throw new IllegalStateException(e);
    }

    activePlugins.put("INFORMATION_SCHEMA", new InfoSchemaStoragePlugin(new InfoSchemaConfig(), context, "INFORMATION_SCHEMA"));
    activePlugins.put("sys", new SystemTablePlugin(SystemTablePluginConfig.INSTANCE, context, "sys"));

    return activePlugins;
  }

  public StoragePlugin getPlugin(String registeredStoragePluginName) throws ExecutionSetupException {
    StoragePlugin plugin = plugins.get(registeredStoragePluginName);

    if(plugin == null){
      StoragePluginConfig config = this.pluginSystemTable.get(registeredStoragePluginName);
      if(config != null){
        this.plugins.put(registeredStoragePluginName, create(registeredStoragePluginName, config));
        plugin = plugins.get(registeredStoragePluginName);
      }
    }

    return plugin;
  }

  public StoragePlugin getPlugin(StoragePluginConfig config) throws ExecutionSetupException {
    if(config instanceof NamedStoragePluginConfig){
      return plugins.get(((NamedStoragePluginConfig) config).name);
    }else{
      // TODO: for now, we'll throw away transient configs.  we really ought to clean these up.
      return create(null, config);
    }
  }

  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig) throws ExecutionSetupException{
    StoragePlugin p = getPlugin(storageConfig);
    if(!(p instanceof FileSystemPlugin)) throw new ExecutionSetupException(String.format("You tried to request a format plugin for a storage plugin that wasn't of type FileSystemPlugin.  The actual type of plugin was %s.", p.getClass().getName()));
    FileSystemPlugin storage = (FileSystemPlugin) p;
    return storage.getFormatPlugin(formatConfig);
  }

  private StoragePlugin create(String name, StoragePluginConfig pluginConfig) throws ExecutionSetupException {
    StoragePlugin plugin = null;
    Constructor<? extends StoragePlugin> c = availablePlugins.get(pluginConfig.getClass());
    if (c == null)
      throw new ExecutionSetupException(String.format("Failure finding StoragePlugin constructor for config %s",
          pluginConfig));
    try {
      plugin = c.newInstance(pluginConfig, context, name);
      return plugin;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException)
        throw ((ExecutionSetupException) t);
      throw new ExecutionSetupException(String.format(
          "Failure setting up new storage plugin configuration for config %s", pluginConfig), t);
    }
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return plugins.entrySet().iterator();
  }

  public RuleSet getStoragePluginRuleSet() {
    return storagePluginsRuleSet;
  }

  public DrillSchemaFactory getSchemaFactory(){
    return schemaFactory;
  }

  public class DrillSchemaFactory implements SchemaFactory{

    @Override
    public void registerSchemas(UserSession session, SchemaPlus parent) {
      for(Map.Entry<String, StoragePlugin> e : plugins.entrySet()){
        e.getValue().registerSchemas(session, parent);
      }

      // Add second level schema as top level schema with name qualified with parent schema name
      // Ex: "dfs" schema has "default" and "tmp" as sub schemas. Add following extra schemas "dfs.default" and
      // "dfs.tmp" under root schema.
      //
      // Before change, schema tree looks like below:
      // "root"
      //    -- "dfs"
      //          -- "default"
      //          -- "tmp"
      //    -- "hive"
      //          -- "default"
      //          -- "hivedb1"
      //
      // After the change, the schema tree looks like below:
      // "root"
      //    -- "dfs"
      //          -- "default"
      //          -- "tmp"
      //    -- "dfs.default"
      //    -- "dfs.tmp"
      //    -- "hive"
      //          -- "default"
      //          -- "hivedb1"
      //    -- "hive.default"
      //    -- "hive.hivedb1"
      List<SchemaPlus> secondLevelSchemas = Lists.newArrayList();
      for(String firstLevelSchemaName : parent.getSubSchemaNames()) {
        SchemaPlus firstLevelSchema = parent.getSubSchema(firstLevelSchemaName);
        for(String secondLevelSchemaName : firstLevelSchema.getSubSchemaNames()) {
          secondLevelSchemas.add(firstLevelSchema.getSubSchema(secondLevelSchemaName));
        }
      }

      for(SchemaPlus schema : secondLevelSchemas) {
        AbstractSchema drillSchema;
        try {
          drillSchema = schema.unwrap(AbstractSchema.class);
        } catch(ClassCastException e) {
          throw new RuntimeException(String.format("Schema '%s' is not expected under root schema", schema.getName()));
        }

        SubSchemaWrapper wrapper = new SubSchemaWrapper(drillSchema);
        parent.add(wrapper.getName(), wrapper);
      }
    }

  }

}
