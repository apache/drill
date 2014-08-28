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
package org.apache.drill.exec.store.parquet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single parquet row group form HDFS
@JsonTypeName("parquet-row-group-scan")
public class ParquetRowGroupScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRowGroupScan.class);

  public final ParquetFormatConfig formatConfig;
  private final ParquetFormatPlugin formatPlugin;
  private final List<RowGroupReadEntry> rowGroupReadEntries;
  private final List<SchemaPath> columns;
  private String selectionRoot;

  @JsonCreator
  public ParquetRowGroupScan( //
      @JacksonInject StoragePluginRegistry registry, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JsonProperty("entries") LinkedList<RowGroupReadEntry> rowGroupReadEntries, //
      @JsonProperty("columns") List<SchemaPath> columns, //
      @JsonProperty("selectionRoot") String selectionRoot //
  ) throws ExecutionSetupException {
    this((ParquetFormatPlugin) registry.getFormatPlugin(Preconditions.checkNotNull(storageConfig),
            formatConfig == null ? new ParquetFormatConfig() : formatConfig),
        rowGroupReadEntries, columns, selectionRoot);
  }

  public ParquetRowGroupScan( //
      ParquetFormatPlugin formatPlugin, //
      List<RowGroupReadEntry> rowGroupReadEntries, //
      List<SchemaPath> columns,
      String selectionRoot) {
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin);
    this.formatConfig = formatPlugin.getConfig();
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.columns = columns == null || columns.size() == 0 ? GroupScan.ALL_COLUMNS : columns;
    this.selectionRoot = selectionRoot;
  }

  @JsonProperty("entries")
  public List<RowGroupReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public ParquetFormatPlugin getStorageEngine() {
    return formatPlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetRowGroupScan(formatPlugin, rowGroupReadEntries, columns, selectionRoot);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

}
