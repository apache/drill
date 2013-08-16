/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.store;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SetupException;

public class SchemaProviderRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SchemaProviderRegistry.class);
  
  private Map<Object, Constructor<? extends SchemaProvider>> allProviders = new HashMap<Object, Constructor<? extends SchemaProvider>>();
  private Map<StorageEngineConfig, SchemaProvider> activeEngines = new HashMap<StorageEngineConfig, SchemaProvider>();

  private final DrillConfig config;
  
  public SchemaProviderRegistry(DrillConfig config){
    init(config);
    this.config = config;
  }
  
  @SuppressWarnings("unchecked")
  public void init(DrillConfig config){
    Collection<Class<? extends SchemaProvider>> providers = PathScanner.scanForImplementations(SchemaProvider.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading schema providers {}", providers);
    for(Class<? extends SchemaProvider> schema: providers){
      int i =0;
      for(Constructor<?> c : schema.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 2 || params[1] != DrillConfig.class || !StorageEngineConfig.class.isAssignableFrom(params[0])){
          logger.info("Skipping SchemaProvider constructor {} for provider class {} since it doesn't implement a [constructor(StorageEngineConfig, DrillConfig)]", c, schema);
          continue;
        }
        allProviders.put(params[0], (Constructor<? extends SchemaProvider>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of StorageSchemaProvider {} as it doesn't have a constructor with the parameters of (StorangeEngineConfig, Config)", schema.getName());
      }
    }
  }
  
  public SchemaProvider getSchemaProvider(StorageEngineConfig engineConfig) throws SetupException{
    SchemaProvider engine = activeEngines.get(engineConfig);
    if(engine != null) return engine;
    Constructor<? extends SchemaProvider> c = allProviders.get(engineConfig.getClass());
    if(c == null) throw new SetupException(String.format("Failure finding StorageSchemaProvider constructor for config %s", engineConfig));
    try {
      return c.newInstance(engineConfig, config);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
      if(t instanceof SetupException) throw ((SetupException) t);
      throw new SetupException(String.format("Failure setting up new storage engine configuration for config %s", engineConfig), t);
    }
  }
  

  
}
