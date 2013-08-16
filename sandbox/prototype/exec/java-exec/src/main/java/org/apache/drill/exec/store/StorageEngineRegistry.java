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
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SetupException;
import org.apache.drill.exec.server.DrillbitContext;

public class StorageEngineRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageEngineRegistry.class);
  
  private Map<Object, Constructor<? extends StorageEngine>> availableEngines = new HashMap<Object, Constructor<? extends StorageEngine>>();
  private Map<StorageEngineConfig, StorageEngine> activeEngines = new HashMap<StorageEngineConfig, StorageEngine>();

  private DrillbitContext context;
  public StorageEngineRegistry(DrillbitContext context){
    init(context.getConfig());
    this.context = context;
  }
  
  @SuppressWarnings("unchecked")
  public void init(DrillConfig config){
    Collection<Class<? extends StorageEngine>> engines = PathScanner.scanForImplementations(StorageEngine.class, config.getStringList(ExecConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading storage engines {}", engines);
    for(Class<? extends StorageEngine> engine: engines){
      int i =0;
      for(Constructor<?> c : engine.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 2 || params[1] != DrillbitContext.class || !StorageEngineConfig.class.isAssignableFrom(params[0])){
          logger.info("Skipping StorageEngine constructor {} for engine class {} since it doesn't implement a [constructor(StorageEngineConfig, DrillbitContext)]", c, engine);
          continue;
        }
        availableEngines.put(params[0], (Constructor<? extends StorageEngine>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of StorageEngine {} as it doesn't have a constructor with the parameters of (StorangeEngineConfig, Config)", engine.getCanonicalName());
      }
    }
  }
  
  public StorageEngine getEngine(StorageEngineConfig engineConfig) throws ExecutionSetupException{
    StorageEngine engine = activeEngines.get(engineConfig);
    if(engine != null) return engine;
    Constructor<? extends StorageEngine> c = availableEngines.get(engineConfig.getClass());
    if(c == null) throw new ExecutionSetupException(String.format("Failure finding StorageEngine constructor for config %s", engineConfig));
    try {
      return c.newInstance(engineConfig, context);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
      if(t instanceof ExecutionSetupException) throw ((ExecutionSetupException) t);
      throw new ExecutionSetupException(String.format("Failure setting up new storage engine configuration for config %s", engineConfig), t);
    }
  }
  

  
}
