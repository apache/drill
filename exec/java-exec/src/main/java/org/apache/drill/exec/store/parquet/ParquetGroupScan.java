/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
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
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.dfs.MetadataContext.PruneStatus;
import org.apache.drill.exec.store.dfs.ReadEntryFromHDFS;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.parquet.Metadata.ColumnMetadata;
import org.apache.drill.exec.store.parquet.Metadata.ParquetFileMetadata;
import org.apache.drill.exec.store.parquet.Metadata.ParquetTableMetadataBase;
import org.apache.drill.exec.store.parquet.Metadata.RowGroupMetadata;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeUtils;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@JsonTypeName("parquet-scan")
public class ParquetGroupScan extends AbstractFileGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetGroupScan.class);

  private final List<ReadEntryWithPath> entries;
  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final DrillFileSystem fs;
  private String selectionRoot;

  private boolean usedMetadataCache = false;
  private List<EndpointAffinity> endpointAffinities;
  private List<SchemaPath> columns;
  private ListMultimap<Integer, RowGroupInfo> mappings;
  private List<RowGroupInfo> rowGroupInfos;
  private Metadata.ParquetTableMetadataBase parquetTableMetadata = null;
  private String cacheFileRoot = null;

  /*
   * total number of rows (obtained from parquet footer)
   */
  private long rowCount;

  /*
   * total number of non-null value for each column in parquet files.
   */
  private Map<SchemaPath, Long> columnValueCounts;

  @JsonCreator public ParquetGroupScan( //
      @JsonProperty("userName") String userName,
      @JsonProperty("entries") List<ReadEntryWithPath> entries,//
      @JsonProperty("storage") StoragePluginConfig storageConfig, //
      @JsonProperty("format") FormatPluginConfig formatConfig, //
      @JacksonInject StoragePluginRegistry engineRegistry, //
      @JsonProperty("columns") List<SchemaPath> columns, //
      @JsonProperty("selectionRoot") String selectionRoot, //
      @JsonProperty("cacheFileRoot") String cacheFileRoot //
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
    this.cacheFileRoot = cacheFileRoot;

    init(null);
  }

  public ParquetGroupScan( //
      String userName,
      FileSelection selection, //
      ParquetFormatPlugin formatPlugin, //
      String selectionRoot,
      String cacheFileRoot,
      List<SchemaPath> columns) //
      throws IOException {
    super(userName);
    this.formatPlugin = formatPlugin;
    this.columns = columns;
    this.formatConfig = formatPlugin.getConfig();
    this.fs = ImpersonationUtil.createFileSystem(userName, formatPlugin.getFsConf());

    this.selectionRoot = selectionRoot;
    this.cacheFileRoot = cacheFileRoot;

    final FileSelection fileSelection = expandIfNecessary(selection);

    this.entries = Lists.newArrayList();
    if (fileSelection.getMetaContext() != null &&
        (fileSelection.getMetaContext().getPruneStatus() == PruneStatus.NOT_STARTED ||
          fileSelection.getMetaContext().getPruneStatus() == PruneStatus.NOT_PRUNED)) {
      // if pruning was not applicable or was attempted and nothing was pruned, initialize the
      // entries with just the selection root instead of the fully expanded list to reduce overhead.
      // The fully expanded list is already stored as part of the fileSet.
      // TODO: at some point we should examine whether the list of entries is absolutely needed.
      entries.add(new ReadEntryWithPath(fileSelection.getSelectionRoot()));
    } else {
      for (String fileName : fileSelection.getFiles()) {
        entries.add(new ReadEntryWithPath(fileName));
      }
    }

    init(fileSelection.getMetaContext());
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
    this.columnValueCounts = that.columnValueCounts == null ? null : new HashMap<>(that.columnValueCounts);
    this.columnTypeMap = that.columnTypeMap == null ? null : new HashMap<>(that.columnTypeMap);
    this.partitionValueMap = that.partitionValueMap == null ? null : new HashMap<>(that.partitionValueMap);
    this.fileSet = that.fileSet == null ? null : new HashSet<>(that.fileSet);
    this.usedMetadataCache = that.usedMetadataCache;
    this.parquetTableMetadata = that.parquetTableMetadata;
    this.cacheFileRoot = that.cacheFileRoot;
  }

  /**
   * expands the selection's folders if metadata cache is found for the selection root.<br>
   * If the selection has already been expanded or no metadata cache was found, does nothing
   *
   * @param selection actual selection before expansion
   * @return new selection after expansion, if no expansion was done returns the input selection
   *
   * @throws IOException
   */
  private FileSelection expandIfNecessary(FileSelection selection) throws IOException {
    if (selection.isExpandedFully()) {
      return selection;
    }

    // use the cacheFileRoot if provided (e.g after partition pruning)
    Path metaFilePath = new Path(cacheFileRoot != null ? cacheFileRoot : selectionRoot, Metadata.METADATA_FILENAME);
    if (!fs.exists(metaFilePath)) { // no metadata cache
      return selection;
    }

    FileSelection expandedSelection = initFromMetadataCache(selection, metaFilePath);
    return expandedSelection;
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

  public Set<String> getFileSet() {
    return fileSet;
  }

  @Override
  public boolean hasFiles() {
    return true;
  }

  @Override
  public Collection<String> getFiles() {
    return fileSet;
  }

  private Set<String> fileSet;

  @JsonIgnore
  private Map<SchemaPath, MajorType> columnTypeMap = Maps.newHashMap();

  /**
   * When reading the very first footer, any column is a potential partition column. So for the first footer, we check
   * every column to see if it is single valued, and if so, add it to the list of potential partition columns. For the
   * remaining footers, we will not find any new partition columns, but we may discover that what was previously a
   * potential partition column now no longer qualifies, so it needs to be removed from the list.
   * @return whether column is a potential partition column
   */
  private boolean checkForPartitionColumn(ColumnMetadata columnMetadata, boolean first) {
    SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
    final PrimitiveTypeName primitiveType;
    final OriginalType originalType;
    if (this.parquetTableMetadata.hasColumnMetadata()) {
      primitiveType = this.parquetTableMetadata.getPrimitiveType(columnMetadata.getName());
      originalType = this.parquetTableMetadata.getOriginalType(columnMetadata.getName());
    } else {
      primitiveType = columnMetadata.getPrimitiveType();
      originalType = columnMetadata.getOriginalType();
    }
    if (first) {
      if (hasSingleValue(columnMetadata)) {
        columnTypeMap.put(schemaPath, getType(primitiveType, originalType));
        return true;
      } else {
        return false;
      }
    } else {
      if (!columnTypeMap.keySet().contains(schemaPath)) {
        return false;
      } else {
        if (!hasSingleValue(columnMetadata)) {
          columnTypeMap.remove(schemaPath);
          return false;
        }
        if (!getType(primitiveType, originalType).equals(columnTypeMap.get(schemaPath))) {
          columnTypeMap.remove(schemaPath);
          return false;
        }
      }
    }
    return true;
  }

  private MajorType getType(PrimitiveTypeName type, OriginalType originalType) {
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
      case INT96:
        return Types.optional(MinorType.VARBINARY);
      default:
        // Should never hit this
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }

  private boolean hasSingleValue(ColumnMetadata columnChunkMetaData) {
    // ColumnMetadata will have a non-null value iff the minValue and the maxValue for the
    // rowgroup are the same
    return (columnChunkMetaData != null) && (columnChunkMetaData.hasSingleValue());
  }

  @Override public void modifyFileSelection(FileSelection selection) {
    entries.clear();
    fileSet = Sets.newHashSet();
    for (String fileName : selection.getFiles()) {
      entries.add(new ReadEntryWithPath(fileName));
      fileSet.add(fileName);
    }

    List<RowGroupInfo> newRowGroupList = Lists.newArrayList();
    for (RowGroupInfo rowGroupInfo : rowGroupInfos) {
      if (fileSet.contains(rowGroupInfo.getPath())) {
        newRowGroupList.add(rowGroupInfo);
      }
    }
    this.rowGroupInfos = newRowGroupList;
  }

  public MajorType getTypeForColumn(SchemaPath schemaPath) {
    return columnTypeMap.get(schemaPath);
  }

  private Map<String, Map<SchemaPath, Object>> partitionValueMap = Maps.newHashMap();

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
        Object s = partitionValueMap.get(f).get(column);
        byte[] bytes;
        if (s instanceof Binary) {
          bytes = ((Binary) s).getBytes();
        } else if (s instanceof String) {
          bytes = ((String) s).getBytes();
        } else if (s instanceof byte[]) {
          bytes = (byte[]) s;
        } else {
          throw new UnsupportedOperationException("Unable to create column data for type: " + type);
        }
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
        dateVector.getMutator().setSafe(index, DateTimeUtils.fromJulianDay(value - ParquetOutputRecordWriter.JULIAN_DAY_EPOC - 0.5));
        return;
      }
      case TIME: {
        NullableTimeVector timeVector = (NullableTimeVector) v;
        Integer value = (Integer) partitionValueMap.get(f).get(column);
        timeVector.getMutator().setSafe(index, value);
        return;
      }
      case TIMESTAMP: {
        NullableTimeStampVector timeStampVector = (NullableTimeStampVector) v;
        Long value = (Long) partitionValueMap.get(f).get(column);
        timeStampVector.getMutator().setSafe(index, value);
        return;
      }
      case VARCHAR: {
        NullableVarCharVector varCharVector = (NullableVarCharVector) v;
        Object s = partitionValueMap.get(f).get(column);
        byte[] bytes;
        if (s instanceof String) { // if the metadata was read from a JSON cache file it maybe a string type
          bytes = ((String) s).getBytes();
        } else if (s instanceof Binary) {
          bytes = ((Binary) s).getBytes();
        } else if (s instanceof byte[]) {
          bytes = (byte[]) s;
        } else {
          throw new UnsupportedOperationException("Unable to create column data for type: " + type);
        }
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
    private long rowCount;  // rowCount = -1 indicates to include all rows.

    @JsonCreator
    public RowGroupInfo(@JsonProperty("path") String path, @JsonProperty("start") long start,
        @JsonProperty("length") long length, @JsonProperty("rowGroupIndex") int rowGroupIndex, long rowCount) {
      super(path, start, length);
      this.rowGroupIndex = rowGroupIndex;
      this.rowCount = rowCount;
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

    public long getRowCount() {
      return rowCount;
    }

  }

  /**
   * Create and return a new file selection based on reading the metadata cache file.
   *
   * This function also initializes a few of ParquetGroupScan's fields as appropriate.
   *
   * @param selection initial file selection
   * @param metaFilePath metadata cache file path
   * @return file selection read from cache
   *
   * @throws IOException
   * @throws UserException when the updated selection is empty, this happens if the user selects an empty folder.
   */
  private FileSelection
  initFromMetadataCache(FileSelection selection, Path metaFilePath) throws IOException {
    // get the metadata for the root directory by reading the metadata file
    // parquetTableMetadata contains the metadata for all files in the selection root folder, but we need to make sure
    // we only select the files that are part of selection (by setting fileSet appropriately)

    // get (and set internal field) the metadata for the directory by reading the metadata file
    this.parquetTableMetadata = Metadata.readBlockMeta(fs, metaFilePath.toString(), selection.getMetaContext());
    List<FileStatus> fileStatuses = selection.getStatuses(fs);

    if (fileSet == null) {
      fileSet = Sets.newHashSet();
    }

    final Path first = fileStatuses.get(0).getPath();
    if (fileStatuses.size() == 1 && selection.getSelectionRoot().equals(first.toString())) {
      // we are selecting all files from selection root. Expand the file list from the cache
      for (Metadata.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        fileSet.add(file.getPath());
      }

    } else if (selection.isExpandedPartial() && !selection.hadWildcard() &&
        cacheFileRoot != null) {
      if (selection.wasAllPartitionsPruned()) {
        // if all partitions were previously pruned, we only need to read 1 file (for the schema)
        fileSet.add(this.parquetTableMetadata.getFiles().get(0).getPath());
      } else {
        // we are here if the selection is in the expanded_partial state (i.e it has directories).  We get the
        // list of files from the metadata cache file that is present in the cacheFileRoot directory and populate
        // the fileSet. However, this is *not* the final list of files that will be scanned in execution since the
        // second phase of partition pruning will apply on the files and modify the file selection appropriately.
        for (Metadata.ParquetFileMetadata file : this.parquetTableMetadata.getFiles()) {
          fileSet.add(file.getPath());
        }
      }
    } else {
      // we need to expand the files from fileStatuses
      for (FileStatus status : fileStatuses) {
        if (status.isDirectory()) {
          //TODO [DRILL-4496] read the metadata cache files in parallel
          final Path metaPath = new Path(status.getPath(), Metadata.METADATA_FILENAME);
          final Metadata.ParquetTableMetadataBase metadata = Metadata.readBlockMeta(fs, metaPath.toString(), selection.getMetaContext());
          for (Metadata.ParquetFileMetadata file : metadata.getFiles()) {
            fileSet.add(file.getPath());
          }
        } else {
          final Path path = Path.getPathWithoutSchemeAndAuthority(status.getPath());
          fileSet.add(path.toString());
        }
      }
    }

    if (fileSet.isEmpty()) {
      // no files were found, most likely we tried to query some empty sub folders
      throw UserException.validationError().message("The table you tried to query is empty").build(logger);
    }

    List<String> fileNames = Lists.newArrayList(fileSet);

    // when creating the file selection, set the selection root without the URI prefix
    // The reason is that the file names above have been created in the form
    // /a/b/c.parquet and the format of the selection root must match that of the file names
    // otherwise downstream operations such as partition pruning can break.
    final Path metaRootPath = Path.getPathWithoutSchemeAndAuthority(new Path(selection.getSelectionRoot()));
    this.selectionRoot = metaRootPath.toString();

    // Use the FileSelection constructor directly here instead of the FileSelection.create() method
    // because create() changes the root to include the scheme and authority; In future, if create()
    // is the preferred way to instantiate a file selection, we may need to do something different...
    // WARNING: file statuses and file names are inconsistent
    FileSelection newSelection = new FileSelection(selection.getStatuses(fs), fileNames, metaRootPath.toString(),
        cacheFileRoot, selection.wasAllPartitionsPruned());

    newSelection.setExpandedFully();
    newSelection.setMetaContext(selection.getMetaContext());
    return newSelection;
  }

  private void init(MetadataContext metaContext) throws IOException {
    if (entries.size() == 1 && parquetTableMetadata == null) {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(entries.get(0).getPath()));
      Path metaPath = null;
      if (fs.isDirectory(p)) {
        // Using the metadata file makes sense when querying a directory; otherwise
        // if querying a single file we can look up the metadata directly from the file
        metaPath = new Path(p, Metadata.METADATA_FILENAME);
      }
      if (metaPath != null && fs.exists(metaPath)) {
        usedMetadataCache = true;
        parquetTableMetadata = Metadata.readBlockMeta(fs, metaPath.toString(), metaContext);
      } else {
        parquetTableMetadata = Metadata.getParquetTableMetadata(fs, p.toString());
      }
    } else {
      Path p = Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot));
      Path metaPath = new Path(p, Metadata.METADATA_FILENAME);
      if (fs.isDirectory(new Path(selectionRoot)) && fs.exists(metaPath)) {
        usedMetadataCache = true;
        if (parquetTableMetadata == null) {
          parquetTableMetadata = Metadata.readBlockMeta(fs, metaPath.toString(), metaContext);
        }
        if (fileSet != null) {
          parquetTableMetadata = removeUnneededRowGroups(parquetTableMetadata);
        }
      } else {
        final List<FileStatus> fileStatuses = Lists.newArrayList();
        for (ReadEntryWithPath entry : entries) {
          getFiles(entry.getPath(), fileStatuses);
        }
        parquetTableMetadata = Metadata.getParquetTableMetadata(fs, fileStatuses);
      }
    }

    if (fileSet == null) {
      fileSet = Sets.newHashSet();
      for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        fileSet.add(file.getPath());
      }
    }

    Map<String, DrillbitEndpoint> hostEndpointMap = Maps.newHashMap();

    for (DrillbitEndpoint endpoint : formatPlugin.getContext().getBits()) {
      hostEndpointMap.put(endpoint.getAddress(), endpoint);
    }

    rowGroupInfos = Lists.newArrayList();
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      int rgIndex = 0;
      for (RowGroupMetadata rg : file.getRowGroups()) {
        RowGroupInfo rowGroupInfo =
            new RowGroupInfo(file.getPath(), rg.getStart(), rg.getLength(), rgIndex, rg.getRowCount());
        EndpointByteMap endpointByteMap = new EndpointByteMapImpl();
        for (String host : rg.getHostAffinity().keySet()) {
          if (hostEndpointMap.containsKey(host)) {
            endpointByteMap
                .add(hostEndpointMap.get(host), (long) (rg.getHostAffinity().get(host) * rg.getLength()));
          }
        }
        rowGroupInfo.setEndpointByteMap(endpointByteMap);
        rgIndex++;
        rowGroupInfos.add(rowGroupInfo);
      }
    }

    this.endpointAffinities = AffinityCreator.getAffinityMap(rowGroupInfos);

    columnValueCounts = Maps.newHashMap();
    this.rowCount = 0;
    boolean first = true;
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      for (RowGroupMetadata rowGroup : file.getRowGroups()) {
        long rowCount = rowGroup.getRowCount();
        for (ColumnMetadata column : rowGroup.getColumns()) {
          SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getName());
          Long previousCount = columnValueCounts.get(schemaPath);
          if (previousCount != null) {
            if (previousCount != GroupScan.NO_COLUMN_STATS) {
              if (column.getNulls() != null) {
                Long newCount = rowCount - column.getNulls();
                columnValueCounts.put(schemaPath, columnValueCounts.get(schemaPath) + newCount);
              }
            }
          } else {
            if (column.getNulls() != null) {
              Long newCount = rowCount - column.getNulls();
              columnValueCounts.put(schemaPath, newCount);
            } else {
              columnValueCounts.put(schemaPath, GroupScan.NO_COLUMN_STATS);
            }
          }
          boolean partitionColumn = checkForPartitionColumn(column, first);
          if (partitionColumn) {
            Map<SchemaPath, Object> map = partitionValueMap.get(file.getPath());
            if (map == null) {
              map = Maps.newHashMap();
              partitionValueMap.put(file.getPath(), map);
            }
            Object value = map.get(schemaPath);
            Object currentValue = column.getMaxValue();
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
        this.rowCount += rowGroup.getRowCount();
        first = false;
      }
    }
  }

  private ParquetTableMetadataBase removeUnneededRowGroups(ParquetTableMetadataBase parquetTableMetadata) {
    List<ParquetFileMetadata> newFileMetadataList = Lists.newArrayList();
    for (ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      if (fileSet.contains(file.getPath())) {
        newFileMetadataList.add(file);
      }
    }

    ParquetTableMetadataBase metadata = parquetTableMetadata.clone();
    metadata.assignFiles(newFileMetadataList);
    return metadata;
  }

  /**
   * Calculates the affinity each endpoint has for this scan, by adding up the affinity each endpoint has for each
   * rowGroup
   *
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return this.endpointAffinities;
  }

  private void getFiles(String path, List<FileStatus> fileStatuses) throws IOException {
    Path p = Path.getPathWithoutSchemeAndAuthority(new Path(path));
    FileStatus fileStatus = fs.getFileStatus(p);
    if (fileStatus.isDirectory()) {
      for (FileStatus f : fs.listStatus(p, new DrillPathFilter())) {
        getFiles(f.getPath().toString(), fileStatuses);
      }
    } else {
      fileStatuses.add(fileStatus);
    }
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) throws PhysicalOperatorSetupException {

    this.mappings = AssignmentCreator.getMappings(incomingEndpoints, rowGroupInfos);
  }

  @Override public ParquetRowGroupScan getSpecificScan(int minorFragmentId) {
    assert minorFragmentId < mappings.size() : String
        .format("Mappings length [%d] should be longer than minor fragment id [%d] but it isn't.",
            mappings.size(), minorFragmentId);

    List<RowGroupInfo> rowGroupsForMinor = mappings.get(minorFragmentId);

    Preconditions.checkArgument(!rowGroupsForMinor.isEmpty(),
        String.format("MinorFragmentId %d has no read entries assigned", minorFragmentId));

    return new ParquetRowGroupScan(
        getUserName(), formatPlugin, convertToReadEntries(rowGroupsForMinor), columns, selectionRoot);
  }

  private List<RowGroupReadEntry> convertToReadEntries(List<RowGroupInfo> rowGroups) {
    List<RowGroupReadEntry> entries = Lists.newArrayList();
    for (RowGroupInfo rgi : rowGroups) {
      RowGroupReadEntry entry = new RowGroupReadEntry(rgi.getPath(), rgi.getStart(), rgi.getLength(), rgi.getRowGroupIndex());
      entries.add(entry);
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
    String cacheFileString = "";
    if (usedMetadataCache) {
      // For EXPLAIN, remove the URI prefix from cacheFileRoot.  If cacheFileRoot is null, we
      // would have read the cache file from selectionRoot
      String str = (cacheFileRoot == null) ?
          Path.getPathWithoutSchemeAndAuthority(new Path(selectionRoot)).toString() :
            Path.getPathWithoutSchemeAndAuthority(new Path(cacheFileRoot)).toString();
      cacheFileString = ", cacheFileRoot=" + str;
    }
    return "ParquetGroupScan [entries=" + entries
        + ", selectionRoot=" + selectionRoot
        + ", numFiles=" + getEntries().size()
        + ", usedMetadataFile=" + usedMetadataCache
        + cacheFileString
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
    newScan.cacheFileRoot = selection.cacheFileRoot;
    newScan.init(selection.getMetaContext());
    return newScan;
  }

  @Override
  public boolean supportsLimitPushdown() {
    return true;
  }

  @Override
  public GroupScan applyLimit(long maxRecords) {
    Preconditions.checkArgument(rowGroupInfos.size() >= 0);

    maxRecords = Math.max(maxRecords, 1); // Make sure it request at least 1 row -> 1 rowGroup.
    // further optimization : minimize # of files chosen, or the affinity of files chosen.
    long count = 0;
    int index = 0;
    for (RowGroupInfo rowGroupInfo : rowGroupInfos) {
      if (count < maxRecords) {
        count += rowGroupInfo.getRowCount();
        index ++;
      } else {
        break;
      }
    }

    Set<String> fileNames = Sets.newHashSet(); // HashSet keeps a fileName unique.
    for (RowGroupInfo rowGroupInfo : rowGroupInfos.subList(0, index)) {
      fileNames.add(rowGroupInfo.getPath());
    }

    if (fileNames.size() == fileSet.size() ) {
      // There is no reduction of rowGroups. Return the original groupScan.
      logger.debug("applyLimit() does not apply!");
      return null;
    }

    try {
      FileSelection newSelection = new FileSelection(null, Lists.newArrayList(fileNames), getSelectionRoot(), cacheFileRoot, false);
      logger.debug("applyLimit() reduce parquet file # from {} to {}", fileSet.size(), fileNames.size());
      return this.clone(newSelection);
    } catch (IOException e) {
      logger.warn("Could not apply rowcount based prune due to Exception : {}", e);
      return null;
    }
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
    return new ArrayList<>(columnTypeMap.keySet());
  }
}
