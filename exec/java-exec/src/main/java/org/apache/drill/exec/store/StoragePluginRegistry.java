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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.hydromatic.linq4j.expressions.DefaultExpression;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableFunction;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.logical.StorageEngines;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.ischema.InfoSchemaConfig;
import org.apache.drill.exec.store.ischema.InfoSchemaStoragePlugin;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;


public class StoragePluginRegistry implements Iterable<Map.Entry<String, StoragePlugin>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginRegistry.class);

  private Map<Object, Constructor<? extends StoragePlugin>> availableEngines = new HashMap<Object, Constructor<? extends StoragePlugin>>();
  private final ImmutableMap<String, StoragePlugin> engines;

  private DrillbitContext context;
  private final DrillSchemaFactory schemaFactory = new DrillSchemaFactory();
  
  public StoragePluginRegistry(DrillbitContext context) {
    try{
    this.context = context;
    init(context.getConfig());
    this.engines = ImmutableMap.copyOf(createEngines());
    }catch(RuntimeException e){
      logger.error("Failure while loading storage engine registry.", e);
      throw new RuntimeException("Faiure while reading and loading storage plugin configuration.", e);
    }
  }

  @SuppressWarnings("unchecked")
  public void init(DrillConfig config){
    Collection<Class<? extends StoragePlugin>> engines = PathScanner.scanForImplementations(StoragePlugin.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading storage engines {}", engines);
    for(Class<? extends StoragePlugin> engine: engines){
      int i =0;
      for(Constructor<?> c : engine.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 3 || params[1] != DrillbitContext.class || !StoragePluginConfig.class.isAssignableFrom(params[0]) || params[2] != String.class){
          logger.info("Skipping StorageEngine constructor {} for engine class {} since it doesn't implement a [constructor(StorageEngineConfig, DrillbitContext, String)]", c, engine);
          continue;
        }
        availableEngines.put(params[0], (Constructor<? extends StoragePlugin>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of StorageEngine {} as it doesn't have a constructor with the parameters of (StorangeEngineConfig, Config)", engine.getCanonicalName());
      }
    }
    
    
  }
  
  private Map<String, StoragePlugin> createEngines(){
    StorageEngines engines = null;
    Map<String, StoragePlugin> activeEngines = new HashMap<String, StoragePlugin>();
    try{
      String enginesData = Resources.toString(Resources.getResource("storage-engines.json"), Charsets.UTF_8);
      engines = context.getConfig().getMapper().readValue(enginesData, StorageEngines.class);
    }catch(IOException e){
      throw new IllegalStateException("Failure while reading storage engines data.", e);
    }
    
    for(Map.Entry<String, StoragePluginConfig> config : engines){
      try{
        StoragePlugin plugin = create(config.getKey(), config.getValue());
        activeEngines.put(config.getKey(), plugin);
      }catch(ExecutionSetupException e){
        logger.error("Failure while setting up StoragePlugin with name: '{}'.", config.getKey(), e);
      }
    }
    activeEngines.put("INFORMATION_SCHEMA", new InfoSchemaStoragePlugin(new InfoSchemaConfig(), context, "INFORMATION_SCHEMA"));
    
    return activeEngines;
  }

  public StoragePlugin getEngine(String registeredStorageEngineName) throws ExecutionSetupException {
    return engines.get(registeredStorageEngineName);
  }
  
  public StoragePlugin getEngine(StoragePluginConfig config) throws ExecutionSetupException {
    if(config instanceof NamedStoragePluginConfig){
      return engines.get(((NamedStoragePluginConfig) config).name);
    }else{
      // TODO: for now, we'll throw away transient configs.  we really ought to clean these up.
      return create(null, config);
    }
  }
  
  public FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig) throws ExecutionSetupException{
    StoragePlugin p = getEngine(storageConfig);
    if(!(p instanceof FileSystemPlugin)) throw new ExecutionSetupException(String.format("You tried to request a format plugin for a stroage engine that wasn't of type FileSystemPlugin.  The actual type of plugin was %s.", p.getClass().getName()));
    FileSystemPlugin storage = (FileSystemPlugin) p;
    return storage.getFormatPlugin(formatConfig);
  }

  private StoragePlugin create(String name, StoragePluginConfig engineConfig) throws ExecutionSetupException {
    StoragePlugin engine = null;
    Constructor<? extends StoragePlugin> c = availableEngines.get(engineConfig.getClass());
    if (c == null)
      throw new ExecutionSetupException(String.format("Failure finding StorageEngine constructor for config %s",
          engineConfig));
    try {
      engine = c.newInstance(engineConfig, context, name);
      return engine;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException) e).getTargetException() : e;
      if (t instanceof ExecutionSetupException)
        throw ((ExecutionSetupException) t);
      throw new ExecutionSetupException(String.format(
          "Failure setting up new storage engine configuration for config %s", engineConfig), t);
    }
  }

  @Override
  public Iterator<Entry<String, StoragePlugin>> iterator() {
    return engines.entrySet().iterator();
  }
  
  public DrillSchemaFactory getSchemaFactory(){
    return schemaFactory;
  }

  public class DrillSchemaFactory implements Function1<SchemaPlus, Schema>{

    @Override
    public Schema apply(SchemaPlus parent) {
      Schema defaultSchema = null;
      for(Map.Entry<String, StoragePlugin> e : engines.entrySet()){
        Schema s = e.getValue().createAndAddSchema(parent);
        if(defaultSchema == null) defaultSchema = s;
      }
      return defaultSchema;
    }
    
    /**
     * Used in situations where we want to get a schema without having to use in the context of an Optiq planner. 
     * @return Root schema of the storage engine hiearchy.
     */
    public SchemaPlus getOrphanedRootSchema(){
      SchemaPlus p = new OrphanPlus();
      apply(p);
      return p;
    }
    
  }
  

  private class OrphanPlusWrap extends OrphanPlus{
    private Schema inner;

    public OrphanPlusWrap(Schema inner) {
      super();
      this.inner = inner;
    }

    @Override
    public SchemaPlus getParentSchema() {
      return inner.getParentSchema();
    }

    @Override
    public String getName() {
      return inner.getName();
    }

    @Override
    public Table getTable(String name) {
      return inner.getTable(name);
    }

    @Override
    public Set<String> getTableNames() {
      return inner.getTableNames();
    }

    @Override
    public Collection<TableFunction> getTableFunctions(String name) {
      return inner.getTableFunctions(name);
    }

    @Override
    public Set<String> getTableFunctionNames() {
      return inner.getTableFunctionNames();
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return inner.getSubSchemaNames();
    }

    @Override
    public Expression getExpression() {
      return inner.getExpression();
    }

    @Override
    public SchemaPlus addRecursive(Schema schema) {
      return schema.getParentSchema().add(schema);
    }
    
    
    
  }

  private class OrphanPlus implements SchemaPlus{

    private HashMap<String, SchemaPlus> schemas = new HashMap();
    
    @Override
    public SchemaPlus getParentSchema() {
      return null;
    }

    @Override
    public String getName() {
      return "";
    }

    @Override
    public Table getTable(String name) {
      return null;
    }

    @Override
    public Set<String> getTableNames() {
      return Collections.emptySet();
    }

    @Override
    public Collection<TableFunction> getTableFunctions(String name) {
      return Collections.emptyList();
    }

    @Override
    public Set<String> getTableFunctionNames() {
      return Collections.emptySet();
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return schemas.keySet();
    }

    @Override
    public Expression getExpression() {
      return new DefaultExpression(Object.class);
    }

    @Override
    public SchemaPlus getSubSchema(String name) {
      return schemas.get(name);
    }

    @Override
    public SchemaPlus add(Schema schema) {
      OrphanPlusWrap plus = new OrphanPlusWrap(schema);
      schemas.put(schema.getName(), plus);
      return plus;
    }

    @Override
    public void add(String name, Table table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void add(String name, TableFunction table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMutable() {
      return false;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SchemaPlus addRecursive(Schema schema) {
      return schema.getParentSchema().add(schema);
    }
    
  }
  
}
