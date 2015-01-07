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
package org.apache.drill.exec.store.sys;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;

import com.typesafe.config.ConfigException;

public class PStoreRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PStoreRegistry.class);

  private DrillConfig config;
  private ClusterCoordinator coord;

  public PStoreRegistry(ClusterCoordinator coord, DrillConfig config) {
    this.coord = coord;
    this.config = config;
  }

  public ClusterCoordinator getClusterCoordinator() {
    return this.coord;
  }

  public DrillConfig getConfig() {
    return this.config;
  }

  @SuppressWarnings("unchecked")
  public PStoreProvider newPStoreProvider() throws ExecutionSetupException {
    try {
      String storeProviderClassName = config.getString(ExecConstants.SYS_STORE_PROVIDER_CLASS);
      logger.info("Using the configured PStoreProvider class: '{}'.", storeProviderClassName);
      Class<? extends PStoreProvider> storeProviderClass = (Class<? extends PStoreProvider>) Class.forName(storeProviderClassName);
      Constructor<? extends PStoreProvider> c = storeProviderClass.getConstructor(PStoreRegistry.class);
      return new CachingStoreProvider(c.newInstance(this));
    } catch (ConfigException.Missing | ClassNotFoundException | NoSuchMethodException | SecurityException
        | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
      throw new ExecutionSetupException("A System Table provider was either not specified or could not be found or instantiated", e);
    }
  }

}
