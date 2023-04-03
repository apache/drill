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
package org.apache.drill.exec.store.delta;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.delta.format.DeltaFormatPlugin;
import org.apache.drill.exec.store.delta.format.DeltaFormatPluginConfig;
import org.apache.drill.exec.store.parquet.AbstractParquetRowGroupScan;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonTypeName("delta-row-group-scan")
public class DeltaRowGroupScan extends AbstractParquetRowGroupScan {

  public static final String OPERATOR_TYPE = "DELTA_ROW_GROUP_SCAN";

  private final DeltaFormatPlugin formatPlugin;
  private final DeltaFormatPluginConfig formatPluginConfig;
  private final Map<Path, Map<String, String>> partitions;

  @JsonCreator
  public DeltaRowGroupScan(@JacksonInject StoragePluginRegistry registry,
    @JsonProperty("userName") String userName,
    @JsonProperty("storageConfig") StoragePluginConfig storageConfig,
    @JsonProperty("formatPluginConfig") FormatPluginConfig formatPluginConfig,
    @JsonProperty("rowGroupReadEntries") List<RowGroupReadEntry> rowGroupReadEntries,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("partitions") Map<Path, Map<String, String>> partitions,
    @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
    @JsonProperty("filter") LogicalExpression filter,
    @JsonProperty("schema") TupleMetadata schema) {
    this(userName,
      registry.resolveFormat(storageConfig, formatPluginConfig, DeltaFormatPlugin.class),
      rowGroupReadEntries,
      columns,
      partitions,
      readerConfig,
      filter,
      schema);
  }

  public DeltaRowGroupScan(String userName,
    DeltaFormatPlugin formatPlugin,
    List<RowGroupReadEntry> rowGroupReadEntries,
    List<SchemaPath> columns,
    Map<Path, Map<String, String>> partitions,
    ParquetReaderConfig readerConfig,
    LogicalExpression filter,
    TupleMetadata schema) {
    super(userName, rowGroupReadEntries, columns, readerConfig, filter,null, schema);
    this.formatPlugin = formatPlugin;
    this.formatPluginConfig = formatPlugin.getConfig();
    this.partitions = partitions;
  }

  @JsonProperty
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty
  public DeltaFormatPluginConfig getFormatPluginConfig() {
    return formatPluginConfig;
  }

  @JsonProperty
  public Map<Path, Map<String, String>> getPartitions() {
    return partitions;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new DeltaRowGroupScan(getUserName(), formatPlugin, rowGroupReadEntries, columns, partitions,
      readerConfig, filter, schema);
  }

  @Override
  public String getOperatorType() {
    return OPERATOR_TYPE;
  }

  @Override
  public AbstractParquetRowGroupScan copy(List<SchemaPath> columns) {
    return new DeltaRowGroupScan(getUserName(), formatPlugin, rowGroupReadEntries, columns, partitions,
       readerConfig, filter, schema);
  }

  @Override
  public Configuration getFsConf(RowGroupReadEntry rowGroupReadEntry) {
    return formatPlugin.getFsConf();
  }

  @Override
  public boolean supportsFileImplicitColumns() {
    return true;
  }

  @Override
  public List<String> getPartitionValues(RowGroupReadEntry rowGroupReadEntry) {
    return Collections.emptyList();
  }

  public Map<String, String> getPartitions(RowGroupReadEntry rowGroupReadEntry) {
    return partitions.get(rowGroupReadEntry.getPath());
  }

  @Override
  public boolean isImplicitColumn(SchemaPath path, String partitionColumnLabel) {
    return partitions.values().stream()
      .anyMatch(map -> map.containsKey(path.getAsUnescapedPath()));
  }
}

