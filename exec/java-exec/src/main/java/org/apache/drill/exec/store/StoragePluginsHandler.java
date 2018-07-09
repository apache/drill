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

import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.store.sys.PersistentStore;


/**
 * Storage plugins handler is an additional service for updating storage plugins configs from the file
 */
public interface StoragePluginsHandler {

  /**
   * Update incoming storage plugins configs from persistence store if present, otherwise bootstrap plugins configs.
   *
   * @param persistentStore the last storage plugins configs from persistence store
   * @param bootstrapPlugins bootstrap storage plugins, which are used in case of first Drill start up
   * @return all storage plugins, which should be loaded into persistence store
   */
  void loadPlugins(PersistentStore<StoragePluginConfig> persistentStore, StoragePlugins bootstrapPlugins);

}
