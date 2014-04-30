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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.RuleSet;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.rpc.user.DrillUser;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.ischema.InfoSchemaConfig;
import org.apache.drill.exec.store.ischema.InfoSchemaStoragePlugin;
import org.apache.drill.exec.store.sys.SystemTablePlugin;
import org.apache.drill.exec.store.sys.SystemTablePluginConfig;
import org.eigenbase.relopt.RelOptRule;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.io.Resources;


public class StoragePluginRegistry implements Iterable<Map.Entry<String, StoragePlugin>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistry.class);

  private Map<Object, Constructor<? extends StoragePlugin>> availablePlugins = new HashMap<Object, Constructor<? extends StoragePlugin>>();
  private ImmutableMap<String, StoragePlugin> plugins;

  private DrillbitContext context;
  private final DrillSchemaFactory schemaFactory = new DrillSchemaFactory();

  private RuleSet storagePluginsRuleSet;

  private static final Expression EXPRESSION = new DefaultExpression(Object.class);

  public StoragePluginRegistry(DrillbitContext context) {
    try{
    this.context = context;
    }catch(RuntimeException e){
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
    this.plugins = ImmutableMap.copyOf(createPlugins());

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
     * Check if "storage-plugins.json" exists. Also check if "storage-plugins" object exists in Distributed Cache.
     * If both exist, check that they are the same. If they differ, throw exception. If "storage-plugins.json" exists, but
     * nothing found in cache, then add it to the cache. If neither are found, throw exception.
     */
    StoragePlugins plugins = null;
    StoragePlugins cachedPlugins = null;
    Map<String, StoragePlugin> activePlugins = new HashMap<String, StoragePlugin>();
    try{
      URL url = Resources.class.getClassLoader().getResource("storage-plugins.json");
      if (url != null) {
        String pluginsData = Resources.toString(url, Charsets.UTF_8);
        plugins = context.getConfig().getMapper().readValue(pluginsData, StoragePlugins.class);
      }
      DistributedMap<StoragePlugins> map = context.getCache().getMap(StoragePlugins.class);
      cachedPlugins = map.get("storage-plugins");
      if (cachedPlugins != null) {
        logger.debug("Found cached storage plugin config: {}", cachedPlugins);
      } else {
        Preconditions.checkNotNull(plugins,"No storage plugin configuration found");
        logger.debug("caching storage plugin config {}", plugins);
        map.put("storage-plugins", plugins);
        cachedPlugins = map.get("storage-plugins");
      }
      if(!(plugins == null || cachedPlugins.equals(plugins))) {
        logger.error("Storage plugin config mismatch. {}. {}", plugins, cachedPlugins);
        throw new DrillbitStartupException("Storage plugin config mismatch");
      }
      logger.debug("using plugin config: {}", cachedPlugins);
    }catch(IOException e){
      logger.error("Failure while reading storage plugins data.", e);
      throw new IllegalStateException("Failure while reading storage plugins data.", e);
    }

    for(Map.Entry<String, StoragePluginConfig> config : cachedPlugins){
      try{
        StoragePlugin plugin = create(config.getKey(), config.getValue());
        activePlugins.put(config.getKey(), plugin);
      }catch(ExecutionSetupException e){
        logger.error("Failure while setting up StoragePlugin with name: '{}'.", config.getKey(), e);
      }
    }
    activePlugins.put("INFORMATION_SCHEMA", new InfoSchemaStoragePlugin(new InfoSchemaConfig(), context, "INFORMATION_SCHEMA"));
    activePlugins.put("sys", new SystemTablePlugin(SystemTablePluginConfig.INSTANCE, context, "sys"));

    return activePlugins;
  }

  public StoragePlugin getPlugin(String registeredStoragePluginName) throws ExecutionSetupException {
    return plugins.get(registeredStoragePluginName);
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
    }

  }

}
