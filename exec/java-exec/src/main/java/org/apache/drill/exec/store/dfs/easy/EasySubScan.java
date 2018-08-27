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
package org.apache.drill.exec.store.dfs.easy;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("fs-sub-scan")
public class EasySubScan extends AbstractSubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EasySubScan.class);

  private final List<FileWorkImpl> files;
  private final EasyFormatPlugin<?> formatPlugin;
  private final List<SchemaPath> columns;
  private String selectionRoot;

  @JsonCreator
  public EasySubScan(
      @JsonProperty("userName") String userName,
      @JsonProperty("files") List<FileWorkImpl> files, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, //
      @JsonProperty("columns") List<SchemaPath> columns, //
      @JsonProperty("selectionRoot") String selectionRoot
      ) throws IOException, ExecutionSetupException {
    super(userName);
    this.formatPlugin = (EasyFormatPlugin<?>) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(this.formatPlugin);
    this.files = files;
    this.columns = columns;
    this.selectionRoot = selectionRoot;
  }

  public EasySubScan(String userName, List<FileWorkImpl> files, EasyFormatPlugin<?> plugin, List<SchemaPath> columns,
      String selectionRoot){
    super(userName);
    this.formatPlugin = plugin;
    this.files = files;
    this.columns = columns;
    this.selectionRoot = selectionRoot;
  }

  @JsonProperty
  public String getSelectionRoot() {
    return selectionRoot;
  }

  @JsonIgnore
  public EasyFormatPlugin<?> getFormatPlugin(){
    return formatPlugin;
  }

  @JsonProperty("files")
  public List<FileWorkImpl> getWorkUnits() {
    return files;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns(){
    return columns;
  }

  @Override
  public int getOperatorType() {
    return formatPlugin.getReaderOperatorType();
  }

}
