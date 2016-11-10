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

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractWriter;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("parquet-writer")
public class ParquetWriter extends AbstractWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetWriter.class);

/** Version of Drill's Parquet writer. Increment this version (by 1) any time we make any format change to the file.
 * Format changes include:
 * <ul>
 * <li>Supporting new data types,
 * <li>Changes to the format of data fields,
 * <li>Adding new metadata to the file footer, etc.
 * </ul>
 * Newer readers must be able to read old files. The Writer version tells the Parquet reader how to interpret fields
 * or metadata when that data changes format from one writer version to another.
 */
  public static final int WRITER_VERSION = 2;

  private final String location;
  private final List<String> partitionColumns;
  private final ParquetFormatPlugin formatPlugin;

  @JsonCreator
  public ParquetWriter(
          @JsonProperty("child") PhysicalOperator child,
          @JsonProperty("location") String location,
          @JsonProperty("partitionColumns") List<String> partitionColumns,
          @JsonProperty("storage") StoragePluginConfig storageConfig,
          @JacksonInject StoragePluginRegistry engineRegistry) throws IOException, ExecutionSetupException {

    super(child);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, new ParquetFormatConfig());
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.location = location;
    this.partitionColumns = partitionColumns;
  }

  public ParquetWriter(PhysicalOperator child,
                       String location,
                       List<String> partitionColumns,
                       ParquetFormatPlugin formatPlugin) {

    super(child);
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

  @JsonProperty("partitionColumns")
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @JsonIgnore
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }

  @JsonIgnore
  public ParquetFormatPlugin getFormatPlugin(){
    return formatPlugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new ParquetWriter(child, location, partitionColumns, formatPlugin);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_WRITER_VALUE;
  }

}
