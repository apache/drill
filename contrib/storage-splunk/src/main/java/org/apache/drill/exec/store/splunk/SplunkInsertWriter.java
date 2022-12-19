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

package org.apache.drill.exec.store.splunk;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.List;

public class SplunkInsertWriter extends SplunkWriter {
  public static final String OPERATOR_TYPE = "SPLUNK_INSERT_WRITER";

  private final SplunkStoragePlugin plugin;
  private final List<String> tableIdentifier;

  @JsonCreator
  public SplunkInsertWriter(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("tableIdentifier") List<String> tableIdentifier,
      @JsonProperty("storage") SplunkPluginConfig storageConfig,
      @JacksonInject StoragePluginRegistry pluginRegistry) {
    super(child, tableIdentifier, pluginRegistry.resolve(storageConfig, SplunkStoragePlugin.class));
    this.plugin = pluginRegistry.resolve(storageConfig, SplunkStoragePlugin.class);
    this.tableIdentifier = tableIdentifier;
  }

  SplunkInsertWriter(PhysicalOperator child, List<String> tableIdentifier, SplunkStoragePlugin plugin) {
    super(child, tableIdentifier, plugin);
    this.tableIdentifier = tableIdentifier;
    this.plugin = plugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SplunkInsertWriter(child, tableIdentifier, plugin);
  }

  public List<String> getTableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @JsonIgnore
  public SplunkPluginConfig getPluginConfig() {
    return this.plugin.getConfig();
  }
}
