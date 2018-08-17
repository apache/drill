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

import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.store.dfs.FormatPlugin;
import org.apache.drill.exec.store.sys.PersistentStore;

public interface StoragePluginRegistry extends Iterable<Map.Entry<String, StoragePlugin>>, AutoCloseable {

  String STORAGE_PLUGIN_REGISTRY_IMPL = "drill.exec.storage.registry";
  String ACTION_ON_STORAGE_PLUGINS_OVERRIDE_FILE = "drill.exec.storage.action_on_plugins_override_file";
  String PSTORE_NAME = "sys.storage_plugins";

  /**
   * Initialize the storage plugin registry. Must be called before the registry is used.
   *
   * @throws DrillbitStartupException if drillbit startup fails
   */
  void init() throws DrillbitStartupException;

  /**
   * Delete a plugin by name
   *
   * @param name The name of the storage plugin to delete.
   */
  void deletePlugin(String name);

  /**
   * Create a plugin by name and configuration. If the plugin already exists, update the plugin
   *
   * @param name The name of the plugin
   * @param config The plugin configuration
   * @param persist Whether to persist the plugin for later use or treat it as ephemeral.
   * @return The StoragePlugin instance.
   * @throws ExecutionSetupException if plugin cannot be created
   */
  StoragePlugin createOrUpdate(String name, StoragePluginConfig config, boolean persist) throws ExecutionSetupException;

  /**
   * Get a plugin by name. Create it based on the PStore saved definition if it doesn't exist.
   *
   * @param name The name of the plugin
   * @return The StoragePlugin instance.
   * @throws ExecutionSetupException if plugin cannot be obtained
   */
  StoragePlugin getPlugin(String name) throws ExecutionSetupException;

  /**
   * Get a plugin by configuration. If it doesn't exist, create it.
   *
   * @param config The configuration for the plugin.
   * @return The StoragePlugin instance.
   * @throws ExecutionSetupException if plugin cannot be obtained
   */
  StoragePlugin getPlugin(StoragePluginConfig config) throws ExecutionSetupException;

  /**
   * Add a plugin to the registry using the provided name.
   *
   * @param name The name of the plugin
   * @param plugin The StoragePlugin instance
   */
  void addEnabledPlugin(String name, StoragePlugin plugin);

  /**
   * Get the Format plugin for the FileSystemPlugin associated with the provided storage config and format config.
   *
   * @param storageConfig The storage config for the associated FileSystemPlugin
   * @param formatConfig The format config for the associated FormatPlugin
   * @return A FormatPlugin instance
   * @throws ExecutionSetupException if plugin cannot be obtained
   */
  FormatPlugin getFormatPlugin(StoragePluginConfig storageConfig, FormatPluginConfig formatConfig) throws ExecutionSetupException;

  /**
   * Get the PStore for this StoragePluginRegistry. (Used in the management layer.)
   *
   * @return PStore for StoragePlugin configuration objects.
   */
  PersistentStore<StoragePluginConfig> getStore();

  /**
   * Get the Schema factory associated with this storage plugin registry.
   *
   * @return A SchemaFactory that can register the schemas associated with this plugin registry.
   */
  SchemaFactory getSchemaFactory();

}
