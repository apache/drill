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

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

@JsonTypeName("fs-sub-scan")
public class EasySubScan extends AbstractSubScan {

  private final List<FileWorkImpl> files;
  private final EasyFormatPlugin<?> formatPlugin;
  private final List<SchemaPath> columns;
  private final Path selectionRoot;
  private final int partitionDepth;
  private final TupleMetadata schema;

  @JsonCreator
  public EasySubScan(
    @JsonProperty("userName") String userName,
    @JsonProperty("files") List<FileWorkImpl> files,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JsonProperty("format") FormatPluginConfig formatConfig,
    @JacksonInject StoragePluginRegistry engineRegistry,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("selectionRoot") Path selectionRoot,
    @JsonProperty("partitionDepth") int partitionDepth,
    @JsonProperty("schema") TupleMetadata schema
    ) throws ExecutionSetupException {
    super(userName);
    this.formatPlugin = (EasyFormatPlugin<?>) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(this.formatPlugin);
    this.files = files;
    this.columns = columns;
    this.selectionRoot = selectionRoot;
    this.partitionDepth = partitionDepth;
    this.schema = schema;
  }

  public EasySubScan(String userName, List<FileWorkImpl> files, EasyFormatPlugin<?> plugin,
      List<SchemaPath> columns, Path selectionRoot, int partitionDepth, TupleMetadata schema) {
    super(userName);
    this.formatPlugin = plugin;
    this.files = files;
    this.columns = columns;
    this.selectionRoot = selectionRoot;
    this.partitionDepth = partitionDepth;
    this.schema = schema;
  }

  @JsonProperty
  public Path getSelectionRoot() { return selectionRoot; }

  @JsonProperty
  public int getPartitionDepth() { return partitionDepth; }

  @JsonIgnore
  public EasyFormatPlugin<?> getFormatPlugin() { return formatPlugin; }

  @JsonProperty("files")
  public List<FileWorkImpl> getWorkUnits() { return files; }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() { return formatPlugin.getStorageConfig(); }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() { return formatPlugin.getConfig(); }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() { return columns; }

  @JsonProperty("schema")
  public TupleMetadata getSchema() { return schema; }

  @Override
  public int getOperatorType() { return formatPlugin.getReaderOperatorType(); }
}
