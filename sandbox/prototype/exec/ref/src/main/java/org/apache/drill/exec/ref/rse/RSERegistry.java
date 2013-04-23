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
package org.apache.drill.exec.ref.rse;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.ref.ExecRefConstants;
import org.apache.drill.exec.ref.exceptions.SetupException;

import com.typesafe.config.Config;

public class RSERegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RSERegistry.class);
  
  private Map<Object, Constructor<? extends ReferenceStorageEngine>> availableEngines = new HashMap<Object, Constructor<? extends ReferenceStorageEngine>>();
  private Map<StorageEngineConfig, ReferenceStorageEngine> activeEngines = new HashMap<StorageEngineConfig, ReferenceStorageEngine>();
  private DrillConfig config;
  
  public RSERegistry(DrillConfig config){
    this.config = config;
    setup(config);
  }
  
  @SuppressWarnings("unchecked")
  public void setup(DrillConfig config){
    Collection<Class<? extends ReferenceStorageEngine>> engines = PathScanner.scanForImplementations(ReferenceStorageEngine.class, config.getStringList(ExecRefConstants.STORAGE_ENGINE_SCAN_PACKAGES));
    logger.debug("Loading storage engines {}", engines);
    for(Class<? extends ReferenceStorageEngine> engine: engines){
      int i =0;
      for(Constructor<?> c : engine.getConstructors()){
        Class<?>[] params = c.getParameterTypes();
        if(params.length != 2 || params[1] == Config.class || !StorageEngineConfig.class.isAssignableFrom(params[0])){
          logger.debug("Skipping ReferenceStorageEngine constructor {} for engine class {} since it doesn't implement a [constructor(StorageEngineConfig, Config)]", c, engine);
          continue;
        }
        availableEngines.put(params[0], (Constructor<? extends ReferenceStorageEngine>) c);
        i++;
      }
      if(i == 0){
        logger.debug("Skipping registration of ReferenceStorageEngine {} as it doesn't have a constructor with the parameters of (StorangeEngineConfig, Config)", engine.getCanonicalName());
      }
    }
  }
  
  public ReferenceStorageEngine getEngine(StorageEngineConfig engineConfig) throws SetupException{
    ReferenceStorageEngine engine = activeEngines.get(engineConfig);
    if(engine != null) return engine;
    Constructor<? extends ReferenceStorageEngine> c = availableEngines.get(engineConfig.getClass());
    if(c == null) throw new SetupException(String.format("Failure finding StorageEngine constructor for config %s", engineConfig));
    try {
      engine = c.newInstance(engineConfig, config);
      activeEngines.put(engineConfig,engine);
      return engine;
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      Throwable t = e instanceof InvocationTargetException ? ((InvocationTargetException)e).getTargetException() : e;
      if(t instanceof SetupException) throw ((SetupException) t);
      throw new SetupException(String.format("Failure setting up new storage engine configuration for config %s", engineConfig), t);
    }
  }
  

  
}
