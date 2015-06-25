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
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.IntervalHolder;
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
import org.apache.drill.exec.store.ParquetOutputRecordWriter;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryFromHDFS;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableIntervalVector;
import org.apache.drill.exec.vector.NullableSmallIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableTinyIntVector;
import org.apache.drill.exec.vector.NullableUInt1Vector;
import org.apache.drill.exec.vector.NullableUInt2Vector;
import org.apache.drill.exec.vector.NullableUInt4Vector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.joda.time.DateTimeUtils;
import parquet.column.statistics.Statistics;
import parquet.format.ConvertedType;
import parquet.format.FileMetaData;
import parquet.format.SchemaElement;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.org.codehaus.jackson.annotate.JsonCreator;

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
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractFileGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);
  static final MetricRegistry metrics = DrillMetrics.getInstance();
  static final String READ_FOOTER_TIMER = MetricRegistry.name(ParquetGroupScan.class, "readFooter");

  private final List<ReadEntryWithPath> entries;
  private final Stopwatch watch = new Stopwatch();
  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final DrillFileSystem fs;
  private final String selectionRoot;

  private List<EndpointAffinity> endpointAffinities;
  private List<SchemaPath> columns;
  private ListMultimap<Integer, RowGroupInfo> mappings;
  private List<RowGroupInfo> rowGroupInfos;

  /*
   * total number of rows (obtained from parquet footer)
   */
  private long rowCount;

  /*
   * total number of non-null value for each column in parquet files.
   */
  private Map<SchemaPath, Long> columnValueCounts;

  @JsonCreator
  public ParquetGroupScan( //
      @JsonProperty("userName") String userName,
      @JsonProperty("entries") List<ReadEntryWithPath> entries, //
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, //
      @JsonProperty("columns") List<SchemaPath> columns, //
      @JsonProperty("selectionRoot") String selectionRoot //
      ) throws IOException, ExecutionSetupException {
    super(ImpersonationUtil.resolveUserName(userName));
    this.columns = columns;
    if (formatConfig == null) {
      formatConfig = new ParquetFormatConfig();
    }
    Preconditions.checkNotNull(storageConfig);
    Preconditions.checkNotNull(formatConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, formatConfig);
    Preconditions.checkNotNull(formatPlugin);
    this.fs = ImpersonationUtil.createFileSystem(getUserName(), formatPlugin.getFsConf());
    this.formatConfig = formatPlugin.getConfig();
    this.entries = entries;
    this.selectionRoot = selectionRoot;
    this.readFooterFromEntries();
  }

  public ParquetGroupScan( //
      String userName,
      FileSelection selection, //
      ParquetFormatPlugin formatPlugin, //
      String selectionRoot,
      List<SchemaPath> columns) //
          throws IOException {
    super(userName);
    this.formatPlugin = formatPlugin;
    this.columns = columns;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = ImpersonationUtil.createFileSystem(userName, formatPlugin.getFsConf());

    this.entries = Lists.newArrayList();
    List<FileStatus> files = selection.getFileStatusList(fs);
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
    super(that);
    this.columns = that.columns == null ? null : Lists.newArrayList(that.columns);
    this.endpointAffinities = that.endpointAffinities == null ? null : Lists.newArrayList(that.endpointAffinities);
    this.entries = that.entries == null ? null : Lists.newArrayList(that.entries);
    this.formatConfig = that.formatConfig;
    this.formatPlugin = that.formatPlugin;
    this.fs = that.fs;
    this.mappings = that.mappings == null ? null : ArrayListMultimap.create(that.mappings);
    this.rowCount = that.rowCount;
    this.rowGroupInfos = that.rowGroupInfos == null ? null : Lists.newArrayList(that.rowGroupInfos);
    this.selectionRoot = that.selectionRoot;
    this.columnValueCounts = that.columnValueCounts == null ? null : new HashMap(that.columnValueCounts);
    this.columnTypeMap = that.columnTypeMap == null ? null : new HashMap(that.columnTypeMap);
    this.partitionValueMap = that.partitionValueMap == null ? null : new HashMap(that.partitionValueMap);
    this.fileSet = that.fileSet == null ? null : new HashSet(that.fileSet);
  }


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

  public String getSelectionRoot() {
    return selectionRoot;
  }

  private void readFooterFromEntries()  throws IOException {
    List<FileStatus> files = Lists.newArrayList();
    for (ReadEntryWithPath e : entries) {
      files.add(fs.getFileStatus(new Path(e.getPath())));
    }
    readFooter(files);
  }

  private void readFooter(final List<FileStatus> statuses) {
    final UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(getUserName());
    try {
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          readFooterHelper(statuses);
          return null;
        }
      });
    } catch (InterruptedException | IOException e) {
      final String errMsg = String.format("Failed to read footer entries from parquet input files: %s", e.getMessage());
      logger.error(errMsg, e);
      throw new DrillRuntimeException(errMsg, e);
    }
  }

  public Set<String> getFileSet() {
    return fileSet;
  }

  private Set<String> fileSet = Sets.newHashSet();

  private void readFooterHelper(List<FileStatus> statuses) throws IOException {
    watch.reset();
    watch.start();
    Timer.Context tContext = metrics.timer(READ_FOOTER_TIMER).time();

    columnTypeMap.clear();
    fileSet.clear();
    partitionValueMap.clear();

    rowGroupInfos = Lists.newArrayList();
    long start = 0, length = 0;
    rowCount = 0;
    columnValueCounts = new HashMap<SchemaPath, Long>();

    ColumnChunkMetaData columnChunkMetaData;

    List<Footer> footers = FooterGatherer.getFooters(formatPlugin.getFsConf(), statuses, 16);
    boolean first = true;
    ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
    for (Footer footer : footers) {
      int index = 0;
      ParquetMetadata metadata = footer.getParquetMetadata();
      FileMetaData fileMetaData = metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, metadata);
      HashMap<String, SchemaElement> schemaElements = new HashMap<>();
      for (SchemaElement se : fileMetaData.getSchema()) {
        schemaElements.put(se.getName(), se);
      }
      for (BlockMetaData rowGroup : metadata.getBlocks()) {
        String file = Path.getPathWithoutSchemeAndAuthority(footer.getFile()).toString();
        fileSet.add(file);
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
          SchemaPath schemaPath = SchemaPath.getSimplePath(col.getPath().toString().replace("[", "").replace("]", "").toLowerCase());

          long previousCount = 0;
          long currentCount = 0;

          if (! columnValueCounts.containsKey(schemaPath)) {
            // create an entry for this column
            columnValueCounts.put(schemaPath, previousCount /* initialize to 0 */);
          } else {
            previousCount = columnValueCounts.get(schemaPath);
          }

          boolean statsAvail = (col.getStatistics() != null && !col.getStatistics().isEmpty());

          if (statsAvail && previousCount != GroupScan.NO_COLUMN_STATS) {
            currentCount = col.getValueCount() - col.getStatistics().getNumNulls(); // only count non-nulls
            columnValueCounts.put(schemaPath, previousCount + currentCount);
          } else {
            // even if 1 chunk does not have stats, we cannot rely on the value count for this column
            columnValueCounts.put(schemaPath, GroupScan.NO_COLUMN_STATS);
          }

          // check if this column can be used for partition pruning
          SchemaElement se = schemaElements.get(schemaPath.getAsUnescapedPath());
          boolean partitionColumn = checkForPartitionColumn(schemaPath, col, se, first);
          if (partitionColumn) {
            Map<SchemaPath,Object> map = partitionValueMap.get(file);
            if (map == null) {
              map = Maps.newHashMap();
              partitionValueMap.put(file, map);
            }
            Object value = map.get(schemaPath);
            Object currentValue = col.getStatistics().genericGetMax();
            if (value != null) {
              if (value != currentValue) {
                columnTypeMap.remove(schemaPath);
              }
            } else {
              map.put(schemaPath, currentValue);
            }
          } else {
            columnTypeMap.remove(schemaPath);
          }
        }

        String filePath = footer.getFile().toUri().getPath();
        rowGroupInfos.add(new ParquetGroupScan.RowGroupInfo(filePath, start, length, index));
        logger.debug("rowGroupInfo path: {} start: {} length {}", filePath, start, length);
        index++;

        rowCount += rowGroup.getRowCount();
        first = false;
      }

    }
    Preconditions.checkState(!rowGroupInfos.isEmpty(), "No row groups found");
    tContext.stop();
    watch.stop();
    logger.debug("Took {} ms to get row group infos", watch.elapsed(TimeUnit.MILLISECONDS));
  }

  @JsonIgnore
  private Map<SchemaPath,MajorType> columnTypeMap = Maps.newHashMap();

  /**
      * When reading the very first footer, any column is a potential partition column. So for the first footer, we check
      * every column to see if it is single valued, and if so, add it to the list of potential partition columns. For the
      * remaining footers, we will not find any new partition columns, but we may discover that what was previously a
      * potential partition column now no longer qualifies, so it needs to be removed from the list.
      * @param column
      * @param columnChunkMetaData
      * @param se
      * @param first
      * @return whether column is a potential partition column
      */
  private boolean checkForPartitionColumn(SchemaPath column, ColumnChunkMetaData columnChunkMetaData, SchemaElement se, boolean first) {
    if (first) {
      if (hasSingleValue(columnChunkMetaData)) {
        columnTypeMap.put(column, getType(columnChunkMetaData, se));
        return true;
      } else {
        return false;
      }
    } else {
      if (!columnTypeMap.keySet().contains(column)) {
        return false;
      } else {
        if (!hasSingleValue(columnChunkMetaData)) {
          columnTypeMap.remove(column);
          return false;
        }
        if (!getType(columnChunkMetaData, se).equals(columnTypeMap.get(column))) {
          columnTypeMap.remove(column);
          return false;
        }
      }
    }
    return true;
  }

  private MajorType getType(ColumnChunkMetaData columnChunkMetaData, SchemaElement schemaElement) {
    ConvertedType originalType = schemaElement == null ? null : schemaElement.getConverted_type();

    if (originalType != null) {
      switch (originalType) {
      case DECIMAL:
        return Types.optional(MinorType.DECIMAL18);
      case DATE:
        return Types.optional(MinorType.DATE);
      case TIME_MILLIS:
        return Types.optional(MinorType.TIME);
      case TIMESTAMP_MILLIS:
        return Types.optional(MinorType.TIMESTAMP);
      case UTF8:
        return Types.optional(MinorType.VARCHAR);
      case UINT_8:
        return Types.optional(MinorType.UINT1);
      case UINT_16:
        return Types.optional(MinorType.UINT2);
      case UINT_32:
        return Types.optional(MinorType.UINT4);
      case UINT_64:
        return Types.optional(MinorType.UINT8);
      case INT_8:
        return Types.optional(MinorType.TINYINT);
      case INT_16:
        return Types.optional(MinorType.SMALLINT);
      }
    }

    PrimitiveTypeName type = columnChunkMetaData.getType();
    switch (type) {
    case BOOLEAN:
      return Types.optional(MinorType.BIT);
    case INT32:
      return Types.optional(MinorType.INT);
    case INT64:
      return Types.optional(MinorType.BIGINT);
    case FLOAT:
      return Types.optional(MinorType.FLOAT4);
    case DOUBLE:
      return Types.optional(MinorType.FLOAT8);
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      return Types.optional(MinorType.VARBINARY);
    default:
      // Should never hit this
      throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  private boolean hasSingleValue(ColumnChunkMetaData columnChunkMetaData) {
    Statistics stats = columnChunkMetaData.getStatistics();
    boolean hasStats = stats != null && !stats.isEmpty();
    if (hasStats) {
      if (stats.genericGetMin() == null || stats.genericGetMax() == null) {
        return false;
      }
      return stats.genericGetMax().equals(stats.genericGetMin());
    } else {
      return false;
    }
  }

  @Override
  public void modifyFileSelection(FileSelection selection) {
    entries.clear();
    for (String fileName : selection.getAsFiles()) {
      entries.add(new ReadEntryWithPath(fileName));
    }
  }

  public MajorType getTypeForColumn(SchemaPath schemaPath) {
    return columnTypeMap.get(schemaPath);
  }

  private Map<String,Map<SchemaPath,Object>> partitionValueMap = Maps.newHashMap();

  public void populatePruningVector(ValueVector v, int index, SchemaPath column, String file) {
    String f = Path.getPathWithoutSchemeAndAuthority(new Path(file)).toString();
    MinorType type = getTypeForColumn(column).getMinorType();
    switch (type) {
    case INT: {
      NullableIntVector intVector = (NullableIntVector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      intVector.getMutator().setSafe(index, value);
      return;
    }
    case SMALLINT: {
      NullableSmallIntVector smallIntVector = (NullableSmallIntVector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      smallIntVector.getMutator().setSafe(index, value.shortValue());
      return;
    }
    case TINYINT: {
      NullableTinyIntVector tinyIntVector = (NullableTinyIntVector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      tinyIntVector.getMutator().setSafe(index, value.byteValue());
      return;
    }
    case UINT1: {
      NullableUInt1Vector intVector = (NullableUInt1Vector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      intVector.getMutator().setSafe(index, value.byteValue());
      return;
    }
    case UINT2: {
      NullableUInt2Vector intVector = (NullableUInt2Vector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      intVector.getMutator().setSafe(index, (char) value.shortValue());
      return;
    }
    case UINT4: {
      NullableUInt4Vector intVector = (NullableUInt4Vector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      intVector.getMutator().setSafe(index, value);
      return;
    }
    case BIGINT: {
      NullableBigIntVector bigIntVector = (NullableBigIntVector) v;
      Long value = (Long) partitionValueMap.get(f).get(column);
      bigIntVector.getMutator().setSafe(index, value);
      return;
    }
    case FLOAT4: {
      NullableFloat4Vector float4Vector = (NullableFloat4Vector) v;
      Float value = (Float) partitionValueMap.get(f).get(column);
      float4Vector.getMutator().setSafe(index, value);
      return;
    }
    case FLOAT8: {
      NullableFloat8Vector float8Vector = (NullableFloat8Vector) v;
      Double value = (Double) partitionValueMap.get(f).get(column);
      float8Vector.getMutator().setSafe(index, value);
      return;
    }
    case VARBINARY: {
      NullableVarBinaryVector varBinaryVector = (NullableVarBinaryVector) v;
      Binary value = (Binary) partitionValueMap.get(f).get(column);
      byte[] bytes = value.getBytes();
      varBinaryVector.getMutator().setSafe(index, bytes, 0, bytes.length);
      return;
    }
    case DECIMAL18: {
      NullableDecimal18Vector decimalVector = (NullableDecimal18Vector) v;
      Long value = (Long) partitionValueMap.get(f).get(column);
      decimalVector.getMutator().setSafe(index, value);
      return;
    }
    case DATE: {
      NullableDateVector dateVector = (NullableDateVector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      dateVector.getMutator().set(index, DateTimeUtils.fromJulianDay(value - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5));
      return;
    }
    case TIME: {
      NullableTimeVector timeVector = (NullableTimeVector) v;
      Integer value = (Integer) partitionValueMap.get(f).get(column);
      timeVector.getMutator().set(index, value);
      return;
    }
    case TIMESTAMP: {
      NullableTimeStampVector timeStampVector = (NullableTimeStampVector) v;
      Long value = (Long) partitionValueMap.get(f).get(column);
      timeStampVector.getMutator().set(index, value);
      return;
    }
    case VARCHAR: {
      NullableVarCharVector varCharVector = (NullableVarCharVector) v;
      Binary value = (Binary) partitionValueMap.get(f).get(column);
      byte[] bytes = value.getBytes();
      varCharVector.getMutator().setSafe(index, bytes, 0, bytes.length);
      return;
    }
    default:
      throw new UnsupportedOperationException("Unsupported type: " + type);
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
        List<TimedRunnable<Void>> blockMappers = Lists.newArrayList();
        for (RowGroupInfo rgi : rowGroupInfos) {
          blockMappers.add(new BlockMapper(bmb, rgi));
        }
        TimedRunnable.run("Load Parquet RowGroup block maps", logger, blockMappers, 16);
      } catch (IOException e) {
        logger.warn("Failure while determining operator affinity.", e);
        return Collections.emptyList();
      }

      this.endpointAffinities = AffinityCreator.getAffinityMap(rowGroupInfos);
    }
    return this.endpointAffinities;
  }

  private class BlockMapper extends TimedRunnable<Void> {
    private final BlockMapBuilder bmb;
    private final RowGroupInfo rgi;

    public BlockMapper(BlockMapBuilder bmb, RowGroupInfo rgi) {
      super();
      this.bmb = bmb;
      this.rgi = rgi;
    }

    @Override
    protected Void runInner() throws Exception {
      EndpointByteMap ebm = bmb.getEndpointByteMap(rgi);
      rgi.setEndpointByteMap(ebm);
      return null;
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return new IOException(String.format("Failure while trying to get block locations for file %s starting at %d.", rgi.getPath(), rgi.getStart()));
    }

  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) throws PhysicalOperatorSetupException {

    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, rowGroupInfos, formatPlugin.getContext());
  }

  @Override
  public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String.format(
        "Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.", mappings.size(),
        minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    return new ParquetRowGroupScan(
        getUserName(), formatPlugin, convertToReadEntries(rowGroupsForMinor), columns, selectionRoot);
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

  @Override
  public List<SchemaPath> getPartitionColumns() {
    return new ArrayList(columnTypeMap.keySet());
  }
}
