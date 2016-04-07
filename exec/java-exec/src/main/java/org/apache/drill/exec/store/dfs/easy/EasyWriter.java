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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.hadoop.conf.Configuration;

@JsonTypeName("fs-writer")
public class EasyWriter extends AbstractWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasyWriter.class);

  private final String location;
  private final List<String> partitionColumns;
  private final FileSystemPlugin plugin;
  private final String workspace;
  private final EasyFormatPlugin<?> formatPlugin;

  @JsonCreator
  public EasyWriter(
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("location") String location,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("workspace") String workspace,
      @JsonProperty("format") FormatPluginConfig formatConfig,
      @JacksonInject StoragePluginRegistry engineRegistry) throws IOException, ExecutionSetupException {

    super(child);
    this.plugin = (FileSystemPlugin) engineRegistry.getPlugin(storageConfig);
    this.workspace = workspace;
    this.formatPlugin = (EasyFormatPlugin<?>) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.location = location;
    this.partitionColumns = partitionColumns;
  }

  public EasyWriter(PhysicalOperator child, String location, List<String> partitionColumns,
      FileSystemPlugin plugin, String workspace, EasyFormatPlugin<?> formatPlugin) {
    super(child);
    this.plugin = plugin;
    this.workspace = workspace;
    this.formatPlugin = formatPlugin;
    this.location = location;
    this.partitionColumns = partitionColumns;
  }

  @JsonProperty("location")
  public String getLocation() {
    return location;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("workspace")
  public String getWorkspace() {
    return workspace;
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }

  @JsonIgnore
  public Configuration getFsConf() {
    return plugin.getFsConf(workspace);
  }

  @JsonIgnore
  public EasyFormatPlugin<?> getFormatPlugin(){
    return formatPlugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new EasyWriter(child, location, partitionColumns, plugin, workspace, formatPlugin);
  }

  @Override
  public int getOperatorType() {
    return formatPlugin.getReaderOperatorType();
  }
}
