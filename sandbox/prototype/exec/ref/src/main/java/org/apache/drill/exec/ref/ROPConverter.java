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
package org.apache.drill.exec.ref;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Union;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rops.ScanROP;
import org.apache.drill.exec.ref.rops.StoreROP;
import org.apache.drill.exec.ref.rops.UnionROP;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine.ReadEntry;

import com.typesafe.config.Config;

class ROPConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ROPConverter.class);

  private LogicalPlan plan;
  private IteratorRegistry registry;
  private EvaluatorFactory builder;
  private RSERegistry engineRegistry;
  private Config config;

  public ROPConverter(LogicalPlan plan, IteratorRegistry registry, EvaluatorFactory builder, RSERegistry engineRegistry) {
    this.plan = plan;
    this.registry = registry;
    this.builder = builder;
    this.engineRegistry = engineRegistry;
  }

  public void convert(LogicalOperator o) throws SetupException {

    try {
      Method m = ROPConverter.class.getMethod("convertSpecific", o.getClass());
      m.invoke(this, o);
      return;
    } catch (NoSuchMethodException e) {
      // noop
      logger.debug("There is no convertSpecific method for type {}.  Looking for a generic option...", o.getClass());
    } catch (IllegalAccessException | IllegalArgumentException | SecurityException e) {
      logger.debug("Failure while attempting to run convertSpecific value for class {}", o.getClass().getSimpleName(),
          e);
    } catch (InvocationTargetException e) {
      Throwable c = e.getCause();
      if (c instanceof SetupException) {
        throw (SetupException) c;
      } else {
        throw new RuntimeException("Failure while trying to run convertSpecific conversion of operator type "
            + o.getClass().getSimpleName(), c);
      }
    }

    String name = ROP.class.getPackage().getName() + "." + o.getClass().getSimpleName() + "ROP";
    try {
      Class<?> c = Class.forName(name);
      if (!ROP.class.isAssignableFrom(c)) throw new IllegalArgumentException("Class does not derive from ROP.");
      Constructor<?> ctor = c.getConstructor(o.getClass());
      if (ctor == null)
        throw new IllegalArgumentException(
            "Class does not have an available constructor that supports a single argument of the class "
                + o.getClass().getSimpleName());
      ROP r = (ROP) ctor.newInstance(o);
      r.init(registry, builder);
      return;

    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      logger.debug("No {} class that accepts a single parameter or type {}.", name, o.getClass().getCanonicalName());
    } catch (InvocationTargetException e) {
      Throwable c = e.getCause();
      if (c instanceof SetupException) throw (SetupException) c;
      throw new SetupException("Failure while trying to run Convert node of type " + o.getClass().getSimpleName(), c);
    }

    throw new UnsupportedOperationException("Unable to convert Logical Operator of type "
        + o.getClass().getCanonicalName());
  }

  private ReferenceStorageEngine getEngine(String name){
    StorageEngineConfig config = plan.getStorageEngineConfig(name);
    if(config == null) throw new SetupException(String.format("Unable to find define storage engine of name [%s].", name));
    ReferenceStorageEngine engine = engineRegistry.getEngine(config);
    return engine;
  }
  
  public void convertSpecific(Store store) throws SetupException {
    StoreROP rop = new StoreROP(store, getEngine(store.getStorageEngine()));
    rop.init(registry, builder);
  }

  public void convertSpecific(Scan scan) throws SetupException {
    StorageEngineConfig engineConfig = plan.getStorageEngineConfig(scan.getStorageEngine());
    ReferenceStorageEngine engine = engineRegistry.getEngine(engineConfig);
    Collection<ReadEntry> readEntries;
    try {
      readEntries = engine.getReadEntries(scan);
    } catch (IOException e1) {
      throw new SetupException("Failure reading input entries.", e1);
    }
    
    switch(readEntries.size()){
    case 0:
      throw new SetupException(String.format("Scan provided did not correspond to any available data.", scan));
    case 1:
      ScanROP scanner = new ScanROP(scan, readEntries.iterator().next(), engine);
      scanner.init(registry, builder);
      return;
    default:
      Union logOp = new Union(null, false);

      ROP parentUnion = new UnionROP(logOp);
      ScanROP[] scanners = new ScanROP[readEntries.size()];
      int i = 0;
      for (ReadEntry e : readEntries) {
        scanners[i] = new ScanROP(scan, e, engine);
        scanners[i].init(registry, builder);
        i++;
      } 

      parentUnion.init(registry, builder);
      registry.swap(logOp, scan); // make it so future things point to the union as the original scans.
      return;
    }
  }

}
