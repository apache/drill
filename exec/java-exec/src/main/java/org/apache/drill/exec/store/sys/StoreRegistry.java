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

import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.store.sys.store.provider.CachingStoreProvider;

public class StoreRegistry<C extends ClusterCoordinator> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreRegistry.class);

  private final DrillConfig config;
  private final C coordinator;

  public StoreRegistry(C coordinator, DrillConfig config) {
    this.coordinator = Preconditions.checkNotNull(coordinator, "coordinator cannot be null");
    this.config = Preconditions.checkNotNull(config, "config cannot be null");
  }

  public C getCoordinator() {
    return this.coordinator;
  }

  public DrillConfig getConfig() {
    return this.config;
  }

  @SuppressWarnings("unchecked")
  public StoreProvider newPStoreProvider() throws ExecutionSetupException {
    try {
      String storeProviderClassName = config.getString(ExecConstants.SYS_STORE_PROVIDER_CLASS);
      logger.info("Using the configured PStoreProvider class: '{}'.", storeProviderClassName);
      Class<? extends StoreProvider> storeProviderClass = (Class<? extends StoreProvider>) Class.forName(storeProviderClassName);
      Constructor<? extends StoreProvider> c = storeProviderClass.getConstructor(StoreRegistry.class);
      return new CachingStoreProvider(c.newInstance(this));
    } catch (ConfigException.Missing | ClassNotFoundException | NoSuchMethodException | SecurityException
        | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
      throw new ExecutionSetupException("A System Table provider was either not specified or could not be found or instantiated", e);
    }
  }

}
