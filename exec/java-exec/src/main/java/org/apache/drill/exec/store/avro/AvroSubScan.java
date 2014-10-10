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
package org.apache.drill.exec.store.avro;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;

import com.google.common.collect.Iterators;
import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.hadoop.fs.FileSystem;

import java.util.Iterator;
import java.util.List;

/**
 * Contains information for reading a single Avro row group from HDFS.
 */
@JsonTypeName("avro-sub-scan")
public class AvroSubScan extends AbstractBase implements SubScan {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroSubScan.class);

  private final AvroFormatPlugin formatPlugin;
  private final AvroFormatConfig formatConfig;
  private final List<SchemaPath> columns;
  private final String selectionRoot;

  private ReadEntryWithPath entry;
  private FileSystem fs;

  @JsonCreator
  public AvroSubScan(@JacksonInject final StoragePluginRegistry registry,
                     @JsonProperty("storage") final StoragePluginConfig storageConfig,
                     @JsonProperty("format") final FormatPluginConfig formatConfig,
                     @JsonProperty("columns") final List<SchemaPath> columns,
                     @JsonProperty("selectionRoot") final String selectionRoot) throws ExecutionSetupException {
    this((AvroFormatPlugin) registry.getFormatPlugin(Preconditions.checkNotNull(storageConfig),
            formatConfig == null ? new AvroFormatConfig() : formatConfig), columns, selectionRoot);
  }

  public AvroSubScan(final AvroFormatPlugin formatPlugin, final List<SchemaPath> columns,
                     final String selectionRoot) {
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin);
    this.formatConfig = formatPlugin.getConfig();
    this.columns = columns;
    this.selectionRoot = selectionRoot;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public AvroFormatConfig getFormatConfig() {
    return formatConfig;
  }

  @Override
  public <T, X, E extends Throwable> T accept(final PhysicalVisitor<T, X, E> physicalVisitor, final X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new AvroSubScan(formatPlugin, columns, selectionRoot);
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.AVRO_ROW_GROUP_SCAN_VALUE;
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  @JsonIgnore
  public List<SchemaPath> getColumns() {
    return columns;
  }

  /*
  public static class AvroSubScanSpec {

  }
  */

  /** XXX - temp hacks **/

  @JsonIgnore
  public void setEntry(ReadEntryWithPath entry) {
    this.entry = entry;
  }

  @JsonIgnore
  public ReadEntryWithPath getEntry() {
    return entry;
  }

  @JsonIgnore
  public void setFileSystem(FileSystem fs) {
    this.fs = fs;
  }

  @JsonIgnore
  public FileSystem getFileSystem() {
    return fs;
  }
}
