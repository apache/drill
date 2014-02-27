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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.ReadEntryFromHDFS;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String READ_FOOTER_TIMER = MetricRegistry.name(ParquetGroupScan.class, "readFooter");
  static final String ENDPOINT_BYTES_TIMER = MetricRegistry.name(ParquetGroupScan.class, "endpointBytes");
  static final String ASSIGNMENT_TIMER = MetricRegistry.name(ParquetGroupScan.class, "applyAssignments");
  static final String ASSIGNMENT_AFFINITY_HIST = MetricRegistry.name(ParquetGroupScan.class, "assignmentAffinity");
  
  final Histogram assignmentAffinityStats = metrics.histogram(ASSIGNMENT_AFFINITY_HIST);

  private ListMultimap<Integer, RowGroupInfo> mappings;
  private List<RowGroupInfo> rowGroupInfos;
  private final List<ReadEntryWithPath> entries;
  private final Stopwatch watch = new Stopwatch();
  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final FileSystem fs;
  private List<EndpointAffinity> endpointAffinities;

  private List<SchemaPath> columns;

  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @JsonProperty("format")
  public ParquetFormatConfig getFormatConfig() {
    return this.formatConfig;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return this.formatPlugin.getStorageConfig();
  }

  @JsonCreator
  public ParquetGroupScan( //
      @JsonProperty("entries") List<ReadEntryWithPath> entries, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, // 
      @JsonProperty("columns") List<SchemaPath> columns //
      ) throws IOException, ExecutionSetupException {
    engineRegistry.init(DrillConfig.create());
    this.columns = columns;
    if(formatConfig == null) formatConfig = new ParquetFormatConfig();
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin);
    this.fs = formatPlugin.getFileSystem().getUnderlying();
    this.formatConfig = formatPlugin.getConfig();
    this.entries = entries;
    this.readFooterFromEntries();

  }

  public ParquetGroupScan(List<FileStatus> files, //
      ParquetFormatPlugin formatPlugin) //
      throws IOException {
    this.formatPlugin = formatPlugin;
    this.columns = null;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = formatPlugin.getFileSystem().getUnderlying();
    
    this.entries = Lists.newArrayList();
    for(FileStatus file : files){
      entries.add(new ReadEntryWithPath(file.getPath().toString()));
    }
    
    readFooter(files);
  }

  private void readFooterFromEntries()  throws IOException {
    List<FileStatus> files = Lists.newArrayList();
    for(ReadEntryWithPath e : entries){
      files.add(fs.getFileStatus(new Path(e.getPath())));
    }
    readFooter(files);
  }
  
  private void readFooter(List<FileStatus> statuses) throws IOException {
    watch.reset();
    watch.start();
    Timer.Context tContext = metrics.timer(READ_FOOTER_TIMER).time();
    
    
    rowGroupInfos = Lists.newArrayList();
    long start = 0, length = 0;
    ColumnChunkMetaData columnChunkMetaData;
    for (FileStatus status : statuses) {
      List<Footer> footers = ParquetFileReader.readFooters(formatPlugin.getHadoopConfig(), status);
      if (footers.size() == 0) {
        throw new IOException(String.format("Unable to find footer for file %s", status.getPath().getName()));
      }

      for (Footer footer : footers) {
        int index = 0;
        ParquetMetadata metadata = footer.getParquetMetadata();
        for (BlockMetaData rowGroup : metadata.getBlocks()) {
          // need to grab block information from HDFS
          columnChunkMetaData = rowGroup.getColumns().iterator().next();
          start = columnChunkMetaData.getFirstDataPageOffset();
          // this field is not being populated correctly, but the column chunks know their sizes, just summing them for
          // now
          // end = start + rowGroup.getTotalByteSize();
          length = 0;
          for (ColumnChunkMetaData col : rowGroup.getColumns()) {
            length += col.getTotalSize();
          }
          String filePath = footer.getFile().toUri().getPath();
          rowGroupInfos.add(new ParquetGroupScan.RowGroupInfo(filePath, start, length, index));
          logger.debug("rowGroupInfo path: {} start: {} length {}", filePath, start, length);
          index++;
        }
      }
    }
    Preconditions.checkState(!rowGroupInfos.isEmpty(), "No row groups found");
    tContext.stop();
    watch.stop();
    logger.debug("Took {} ms to get row group infos", watch.elapsed(TimeUnit.MILLISECONDS));
  }

  @JsonIgnore
  public FileSystem getFileSystem() {
    return this.fs;
  }

  public static class RowGroupInfo extends ReadEntryFromHDFS implements CompleteWork, FileWork {

    private EndpointByteMap byteMap;
    private int rowGroupIndex;

    @JsonCreator
    public RowGroupInfo(@JsonProperty("path") String path, @JsonProperty("start") long start,
        @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex) {
      super(path, start, length);
      this.rowGroupIndex = rowGroupIndex;
    }

    public RowGroupReadEntry getRowGroupReadEntry() {
      return new RowGroupReadEntry(this.getPath(), this.getStart(), this.getLength(), this.rowGroupIndex);
    }

    public int getRowGroupIndex() {
      return this.rowGroupIndex;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }

    @Override
    public long getTotalBytes() {
      return this.getLength();
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    public void setEndpointByteMap(EndpointByteMap byteMap) {
      this.byteMap = byteMap;
    }
  }

  /**
   * Calculates the affinity each endpoint has for this scan, by adding up the affinity each endpoint has for each
   * rowGroup
   * 
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    
    if (this.endpointAffinities == null) {
      BlockMapBuilder bmb = new BlockMapBuilder(fs, formatPlugin.getContext().getBits());
      try{
        for (RowGroupInfo rgi : rowGroupInfos) {
          EndpointByteMap ebm = bmb.getEndpointByteMap(rgi);
          rgi.setEndpointByteMap(ebm);
        }
      } catch (IOException e) {
        logger.warn("Failure while determining operator affinity.", e);
        return Collections.emptyList();
      }

      this.endpointAffinities = AffinityCreator.getAffinityMap(rowGroupInfos);
    }
    return this.endpointAffinities;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) throws PhysicalOperatorSetupException {

    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, rowGroupInfos);

  }

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    return new ParquetRowGroupScan(formatPlugin, convertToReadEntries(rowGroupsForMinor), columns);
  }

  
  
  private List<RowGroupReadEntry> convertToReadEntries(List<RowGroupInfo> rowGroups){
    List<RowGroupReadEntry> entries = Lists.newArrayList();
    for (RowGroupInfo rgi : rowGroups) {
      RowGroupReadEntry rgre = new RowGroupReadEntry(rgi.getPath(), rgi.getStart(), rgi.getLength(),
          rgi.getRowGroupIndex());
      entries.add(rgre);
    }
    return entries;
  }


  @Override
  public int getMaxParallelizationWidth() {
    return rowGroupInfos.size();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public OperatorCost getCost() {
    // TODO Figure out how to properly calculate cost
    return new OperatorCost(1, rowGroupInfos.size(), 1, 1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10, 10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    // TODO return copy of self
    return this;
  }

}
