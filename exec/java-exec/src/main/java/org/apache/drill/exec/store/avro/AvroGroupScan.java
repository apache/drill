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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
//import org.apache.drill.exec.store.avro.AvroSubScan.AvroSubScanSpec;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Group scan implementation for Avro data files.
 */
@JsonTypeName("avro-scan")
public class AvroGroupScan extends AbstractGroupScan {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AvroGroupScan.class);

  private final AvroFormatPlugin formatPlugin;
  private final AvroFormatConfig formatConfig;
  private final List<SchemaPath> columns;
  private final FileSystem fs;
  private final List<ReadEntryWithPath> entries;
  private final String selectionRoot;

  //private Map<Integer, List<AvroSubScanSpec>> endpointMappings;

  private List<EndpointAffinity> endpointAffinities;

  @JsonCreator
  public AvroGroupScan(@JsonProperty("entries") final List<ReadEntryWithPath> entries,
                       @JsonProperty("storage") final StoragePluginConfig storageConfig,
                       @JsonProperty("format") final FormatPluginConfig formatConfig,
                       @JacksonInject final StoragePluginRegistry engineRegistry,
                       @JsonProperty("columns") final List<SchemaPath> columns,
                       @JsonProperty("selectionRoot") final String selectionRoot) throws ExecutionSetupException {

    this.columns = columns;
    final AvroFormatConfig afc;
    if (formatConfig == null) {
      afc = new AvroFormatConfig();
    } else {
      afc = (AvroFormatConfig) formatConfig;
    }
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(afc);
    this.formatPlugin = (AvroFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, afc);
    Preconditions.checkNotNull(this.formatPlugin);
    this.fs = formatPlugin.getFileSystem().getUnderlying();
    this.formatConfig = formatPlugin.getConfig();
    this.entries = entries;
    this.selectionRoot = selectionRoot;
  }

  public AvroGroupScan(final List<FileStatus> files, final AvroFormatPlugin formatPlugin,
                       final String selectionRoot, final List<SchemaPath> columns) throws IOException {

    this.formatPlugin = formatPlugin;
    this.columns = columns;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = formatPlugin.getFileSystem().getUnderlying();
    this.selectionRoot = selectionRoot;

    this.entries = Lists.newArrayList();
    for (final FileStatus fs : files) {
      entries.add(new ReadEntryWithPath(fs.getPath().toString()));
    }
  }

  @JsonProperty("format")
  public AvroFormatConfig getFormatConfig() {
    return this.formatConfig;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return this.formatPlugin.getStorageConfig();
  }

  private AvroGroupScan(final AvroGroupScan that, final List<SchemaPath> columns) {
    this.columns = (columns == null) ? that.columns : columns;
    this.entries = that.entries;
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.fs = that.fs;
    this.selectionRoot = that.selectionRoot;

    // XXX - DON'T FORGET TO ADD THESE AFTER WE'VE IMPLEMENTED AFFINITY
    //this.endpointAffinities = that.endpointAffinities;
    //this.mappings = that.mappings;
    //this.rowCount = that.rowCount;
    //this.rowGroupInfos = that.rowGroupInfos;
    //this.columnValueCounts = that.columnValueCounts;
  }

  @Override
  public void applyAssignments(final List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    // XXX - Unimplemented
    logger.warn("AvroGroupScan.applyAssignments() is not implemented");
  }

  @Override
  public AvroSubScan getSpecificScan(final int minorFragmentId) throws ExecutionSetupException {

    final AvroSubScan sub = new AvroSubScan(formatPlugin, columns, selectionRoot);

    // XXX - This is a temporary hack just to get something working. Need to revisit sub-scan specs
    //       once we work out affinity and endpoints.
    sub.setEntry(entries.get(0));
    sub.setFileSystem(fs);

    return sub;
  }

  @Override
  public int getMaxParallelizationWidth() {
    // XXX - Finish
    return 1;
  }

  @Override
  public ScanStats getScanStats() {
    // XXX - Is 0 the correct value for second arg? What if I don't know the row count a priori?
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, 0, 1, 1);
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    // XXX - Unimplemented
    if (endpointAffinities != null) {
      return endpointAffinities;
    }
    return Collections.emptyList();
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new AvroGroupScan(this, null);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "AvroGroupScan [entries=" + entries +
            ", selectionRoot=" + selectionRoot +
            ", columns=" + columns + "]";
  }

  @Override
  public GroupScan clone(final List<SchemaPath> columns) {
    return new AvroGroupScan(this, columns);
  }

  @JsonIgnore
  public boolean canPushdownProjects(final List<SchemaPath> columns) {
    return true;
  }

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }
}
