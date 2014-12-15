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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractFileGroupScan;
import org.apache.drill.exec.physical.base.FileGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSelection;
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
public class ParquetGroupScan extends AbstractFileGroupScan {
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
  private String selectionRoot;

  private List<SchemaPath> columns;

  /*
   * total number of rows (obtained from parquet footer)
   */
  private long rowCount;

  /*
   * total number of non-null value for each column in parquet files.
   */
  private Map<SchemaPath, Long> columnValueCounts;

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
      @JsonProperty("columns") List<SchemaPath> columns, //
      @JsonProperty("selectionRoot") String selectionRoot //
      ) throws IOException, ExecutionSetupException {
    this.columns = columns;
    if (formatConfig == null) {
      formatConfig = new ParquetFormatConfig();
    }
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin);
    this.fs = formatPlugin.getFileSystem();
    this.formatConfig = formatPlugin.getConfig();
    this.entries = entries;
    this.selectionRoot = selectionRoot;
    this.readFooterFromEntries();

  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public ParquetGroupScan(List<FileStatus> files, //
      ParquetFormatPlugin formatPlugin, //
      String selectionRoot,
      List<SchemaPath> columns) //
          throws IOException {
    this.formatPlugin = formatPlugin;
    this.columns = columns;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = formatPlugin.getFileSystem();

    this.entries = Lists.newArrayList();
    for (FileStatus file : files) {
      entries.add(new ReadEntryWithPath(file.getPath().toString()));
    }

    this.selectionRoot = selectionRoot;

    readFooter(files);
  }

  /*
   * This is used to clone another copy of the group scan.
   */
  private ParquetGroupScan(ParquetGroupScan that) {
    this.columns = that.columns;
    this.endpointAffinities = that.endpointAffinities;
    this.entries = that.entries;
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.fs = that.fs;
    this.mappings = that.mappings;
    this.rowCount = that.rowCount;
    this.rowGroupInfos = that.rowGroupInfos;
    this.selectionRoot = that.selectionRoot;
    this.columnValueCounts = that.columnValueCounts;
  }

  private void readFooterFromEntries()  throws IOException {
    List<FileStatus> files = Lists.newArrayList();
    for (ReadEntryWithPath e : entries) {
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
    rowCount = 0;
    columnValueCounts = new HashMap<SchemaPath, Long>();

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
          long valueCountInGrp = 0;
          // need to grab block information from HDFS
          columnChunkMetaData = rowGroup.getColumns().iterator().next();
          start = columnChunkMetaData.getFirstDataPageOffset();
          // this field is not being populated correctly, but the column chunks know their sizes, just summing them for
          // now
          // end = start + rowGroup.getTotalByteSize();
          length = 0;
          for (ColumnChunkMetaData col : rowGroup.getColumns()) {
            length += col.getTotalSize();
            valueCountInGrp = Math.max(col.getValueCount(), valueCountInGrp);
            SchemaPath path = SchemaPath.getSimplePath(col.getPath().toString().replace("[", "").replace("]", "").toLowerCase());

            long previousCount = 0;
            long currentCount = 0;

            if (! columnValueCounts.containsKey(path)) {
              // create an entry for this column
              columnValueCounts.put(path, previousCount /* initialize to 0 */);
            } else {
              previousCount = columnValueCounts.get(path);
            }

            boolean statsAvail = (col.getStatistics() != null && !col.getStatistics().isEmpty());

            if (statsAvail && previousCount != GroupScan.NO_COLUMN_STATS) {
              currentCount = col.getValueCount() - col.getStatistics().getNumNulls(); // only count non-nulls
              columnValueCounts.put(path, previousCount + currentCount);
            } else {
              // even if 1 chunk does not have stats, we cannot rely on the value count for this column
              columnValueCounts.put(path, GroupScan.NO_COLUMN_STATS);
            }

          }

          String filePath = footer.getFile().toUri().getPath();
          rowGroupInfos.add(new ParquetGroupScan.RowGroupInfo(filePath, start, length, index));
          logger.debug("rowGroupInfo path: {} start: {} length {}", filePath, start, length);
          index++;

          rowCount += rowGroup.getRowCount();
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

  @Override
  public void modifyFileSelection(FileSelection selection) {
    entries.clear();
    for (String fileName : selection.getAsFiles()) {
      entries.add(new ReadEntryWithPath(fileName));
    }
  }

  public static class RowGroupInfo extends ReadEntryFromHDFS implements CompleteWork, FileWork {

    private EndpointByteMap byteMap;
    private int rowGroupIndex;
    private String root;

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
      try {
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

    return new ParquetRowGroupScan(formatPlugin, convertToReadEntries(rowGroupsForMinor), columns, selectionRoot);
  }

  private List<RowGroupReadEntry> convertToReadEntries(List<RowGroupInfo> rowGroups) {
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
  public ScanStats getScanStats() {
    int columnCount = columns == null ? 20 : columns.size();
    return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, rowCount, 1, rowCount * columnCount);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetGroupScan(this);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "ParquetGroupScan [entries=" + entries
        + ", selectionRoot=" + selectionRoot
        + ", numFiles=" + getEntries().size()
        + ", columns=" + columns + "]";
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    ParquetGroupScan newScan = new ParquetGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public FileGroupScan clone(FileSelection selection) throws IOException {
    ParquetGroupScan newScan = new ParquetGroupScan(this);
    newScan.modifyFileSelection(selection);
    newScan.readFooterFromEntries();
    return newScan;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  /**
   *  Return column value count for the specified column. If does not contain such column, return 0.
   */
  @Override
  public long getColumnValueCount(SchemaPath column) {
    return columnValueCounts.containsKey(column) ? columnValueCounts.get(column) : 0;
  }

}
