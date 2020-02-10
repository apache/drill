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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds maps to storage plugins. Supports name => plugin and config => plugin
 * mappings. Name map is case insensitive. Assumes a unique config => plugin
 * mapping. This map holds only enabled plugins; those which are disabled appear
 * only in the persistent store.
 * <p>
 * The two maps are synchronized by this class, allowing the maps themselves
 * to not be of the concurrent variety.
 * <p>
 * This is inspired by ConcurrentMap but provides a secondary key mapping that allows an alternative lookup mechanism.
 * The class is responsible for internally managing consistency between the two maps. This class is threadsafe.
 * Name map is case insensitive.
 *
 * <h4>Concurrency</h4>
 *
 * All map access is protected to avoid race conditions across the two maps.
 * Callers should generally remove/replace methods which take the old value
 * as a form of optimistic concurrency: the change is made only if the value
 * found in the map is that which is expected.
 * <p>
 * Plugin open and close is <b>not</b> done in this map as both operations
 * could take considerable time and must not hold locks. The caller is
 * responsible for checking return statuses and performing the needed
 * close. (The one exception is final close, which is done here.)
 */
class StoragePluginMap implements Iterable<PluginHandle>, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(StoragePluginMap.class);

  private final Map<String, PluginHandle> nameMap = CaseInsensitiveMap.newHashMap();
  private final Map<StoragePluginConfig, PluginHandle> configMap = new HashMap<>();

  /**
   * Put a plugin. Replaces, and closes, any existing plugin. Safe for putting
   * the same plugin twice. Also safe for putting a different
   *
   * @return the replaced entry, if any, which the caller should close
   */
  public synchronized PluginHandle put(PluginHandle plugin) {
    PluginHandle oldPlugin = nameMap.put(plugin.name(), plugin);
    if (oldPlugin != null) {
      if (oldPlugin == plugin || oldPlugin.config().equals(plugin.config())) {
        return null;
      }
      if (oldPlugin.isIntrinsic()) {
        // Put the old one back
        nameMap.put(oldPlugin.name(), oldPlugin);
        throw UserException.permissionError()
          .message("Attempt to replace a system plugin.")
          .addContext("Plugin name", oldPlugin.name())
          .addContext("Intrinsic plugin class", oldPlugin.config().getClass().getName())
          .addContext("Attempted replacement", plugin.config().getClass().getName())
          .build(logger);
      }
      configMap.remove(oldPlugin.config());
    }
    configMap.put(plugin.config(), plugin);
    return oldPlugin;
  }

  /**
   * Put the given plugin, but only if no plugin already exists for the
   * name.
   * @param plugin the new plugin
   * @return the resulting entry, the old one that already existed,
   * or the new one
   */
  public synchronized PluginHandle putIfAbsent(PluginHandle plugin) {
    PluginHandle oldPlugin = nameMap.putIfAbsent(plugin.name(), plugin);
    if (oldPlugin != null) {
      return oldPlugin;
    } else {
      configMap.put(plugin.config(), plugin);
      return plugin;
    }
  }

  public synchronized PluginHandle get(String name) {
    return nameMap.get(name);
  }

  /**
   * Retrieve a plugin by config. Configs are compared by value: two instances
   * with the same values compare as identical (assuming the plugin config
   * implementation is correct.)
   */
  public synchronized PluginHandle get(StoragePluginConfig config) {
    return configMap.get(config);
  }

  /**
   * Replaces one plugin with another, but only if the map contains the old
   * one.
   *
   * @param oldPlugin the expected old plugin to be replaced
   * @param newPlugin the new plugin to insert
   * @return true if the new plugin was inserted, false if not because
   * the old plugin was not found in the map
   */
  public synchronized boolean replace(PluginHandle oldPlugin, PluginHandle newPlugin) {
    Preconditions.checkArgument(oldPlugin != null);
    Preconditions.checkArgument(newPlugin != null);
    Preconditions.checkArgument(oldPlugin.name().equalsIgnoreCase(newPlugin.name()));
    Preconditions.checkArgument(oldPlugin != newPlugin);
    if (oldPlugin.isIntrinsic()) {
      throw UserException.permissionError()
        .message("Attempt to replace a system plugin.")
        .addContext("Plugin name", oldPlugin.name())
        .addContext("Intrinsic plugin class", oldPlugin.config().getClass().getName())
        .addContext("Attempted replacement", newPlugin.config().getClass().getName())
        .build(logger);
    }
    boolean ok = nameMap.replace(oldPlugin.name(), oldPlugin, newPlugin);
    if (ok) {
      configMap.remove(oldPlugin.config(), oldPlugin);
      configMap.put(newPlugin.config(), newPlugin);
    }
    return ok;
  }

  /**
   * Removes and returns a plugin by name and closes it. This form is not
   * concurrency-safe: another user could have deleted and recreated the
   * plugin between the time the current user viewed the plugin and decided
   * to delete it.
   *
   * @return the doomed plugin if the plugin was removed, null if there was
   * no entry by the given name
   * @see {@link #remove(PluginHandle)
   */
  public synchronized PluginHandle remove(String name) {
    PluginHandle plugin = get(name);
    if (plugin == null) {
      return null;
    }
    if (plugin.isIntrinsic()) {
      throw UserException.permissionError()
        .message("Attempt to remove a system plugin.")
        .addContext("Plugin name", plugin.name())
        .addContext("Intrinsic plugin class", plugin.config().getClass().getName())
        .build(logger);
    }
    nameMap.remove(name);
    configMap.remove(plugin.config(), plugin);
    return plugin;
  }

  /**
   * Removes the plugin, but only if it is in the map. That is,
   * resolves the name and removes the plugin only if it resolves
   * to the given plugin.
   *
   * @return true if the plugin was removed and the caller
   * should close it, false otherwise
   */
  public synchronized boolean remove(PluginHandle oldPlugin) {
    if (oldPlugin.isIntrinsic()) {
      throw UserException.permissionError()
        .message("Attempt to remove a system plugin.")
        .addContext("Plugin name", oldPlugin.name())
        .addContext("Intrinsic plugin class", oldPlugin.config().getClass().getName())
        .build(logger);
    }
    boolean ok = nameMap.remove(oldPlugin.name(), oldPlugin);
    if (ok) {
      configMap.remove(oldPlugin.config(), oldPlugin);
    }
    return ok;
  }

  /**
   * Given a name and a config (which is presumed to have become disabled),
   * remove and return any existing plugin. Only matches if the name is found and the
   * named plugin has the same config as the one to remove to enforce
   * optimistic concurrency.
   *
   * @param name plugin name
   * @param oldConfig expected config of the doomed plugin
   * @return true if the plugin was removed and closed, false otherwise
   */
  public synchronized PluginHandle remove(String name, StoragePluginConfig oldConfig) {
    PluginHandle oldEntry = nameMap.get(name);
    if (oldEntry == null || !oldEntry.config().equals(oldConfig)) {
      return null;
    }
    if (oldEntry.isIntrinsic()) {
      throw UserException.permissionError()
        .message("Attempt to remove a system plugin.")
        .addContext("Plugin name", oldEntry.name())
        .addContext("Intrinsic plugin class", oldEntry.config().getClass().getName())
        .build(logger);
    }
    nameMap.remove(oldEntry.name());
    configMap.remove(oldEntry.config());
    return oldEntry;
  }

  @Override
  public synchronized Iterator<PluginHandle> iterator() {
    return nameMap.values().iterator();
  }

  /**
   * Returns set of plugin names of this {@link StoragePluginMap}
   *
   * @return plugin names
   */
  public synchronized Set<String> getNames() {
    return nameMap.keySet();
  }

  public synchronized Collection<PluginHandle> plugins() {
    return nameMap.values();
  }

  public synchronized Set<StoragePluginConfig> configs() {
    return configMap.keySet();
  }

  // Closes all plugins. Can take some time if plugins are slow to close
  // (Suffer network timeouts, for example.) Not synchronized as should
  // only be done during final Drillbit shutdown.
  @Override
  public void close() {
    // Plugin handles do not derive from AutoCloseable. Handles must handle
    // any errors on close so that things work when the loading cache decides
    // to evict a plugin. So, we just use a simple per-handle iteration here.
    plugins().stream()
      .forEach(e -> e.close());
    configMap.clear();
    nameMap.clear();
  }
}
