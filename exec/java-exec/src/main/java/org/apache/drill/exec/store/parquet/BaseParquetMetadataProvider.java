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
package org.apache.drill.exec.store.parquet;

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.ParquetMetadataProvider;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.ColumnStatisticsImpl;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.shaded.guava.com.google.common.collect.HashMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.LinkedListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.shaded.guava.com.google.common.collect.SetMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Table;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.FileTableMetadata;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ParquetMetadataProvider} which contains base methods for obtaining metadata from
 * parquet statistics.
 */
public abstract class BaseParquetMetadataProvider implements ParquetMetadataProvider {

  /**
   * {@link HashBasedTable} cannot contain nulls, used this object to represent null values.
   */
  static final Object NULL_VALUE = new Object();

  protected final List<ReadEntryWithPath> entries;
  protected final ParquetReaderConfig readerConfig;

  protected final String tableName;
  protected final Path tableLocation;

  private ParquetGroupScanStatistics<? extends BaseMetadata> parquetGroupScanStatistics;
  protected MetadataBase.ParquetTableMetadataBase parquetTableMetadata;
  protected Set<Path> fileSet;

  private List<SchemaPath> partitionColumns;

  private Multimap<Path, RowGroupMetadata> rowGroups;
  private TableMetadata tableMetadata;
  private List<PartitionMetadata> partitions;
  private Map<Path, FileMetadata> files;

  // whether metadata for row groups should be collected to create files, partitions and table metadata
  private final boolean collectMetadata = false;

  public BaseParquetMetadataProvider(List<ReadEntryWithPath> entries,
                                     ParquetReaderConfig readerConfig,
                                     String tableName,
                                     Path tableLocation) {
    this(readerConfig, entries, tableName, tableLocation);
  }

  public BaseParquetMetadataProvider(ParquetReaderConfig readerConfig,
                                     List<ReadEntryWithPath> entries,
                                     String tableName,
                                     Path tableLocation) {
    this.entries = entries == null ? new ArrayList<>() : entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
    this.tableName = tableName;
    this.tableLocation = tableLocation;
  }

  protected void init() throws IOException {
    initInternal();

    assert parquetTableMetadata != null;

    if (fileSet == null) {
      fileSet = new HashSet<>();
      fileSet.addAll(parquetTableMetadata.getFiles().stream()
          .map(MetadataBase.ParquetFileMetadata::getPath)
          .collect(Collectors.toSet()));
    }

    initializeMetadata();
  }

  /**
   * Method which initializes all metadata kinds to get rid of parquetTableMetadata.
   * Once deserialization and serialization from/into metastore classes is done, this method should be removed
   * to allow lazy initialization.
   */
  public void initializeMetadata() {
    getTableMetadata();
    getFilesMetadata();
    getPartitionsMetadata();
    getRowGroupsMeta();
    parquetTableMetadata = null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TableMetadata getTableMetadata() {
    if (tableMetadata == null) {
      Map<String, Object> tableStatistics = new HashMap<>();
      Set<String> partitionKeys = new HashSet<>();
      Map<SchemaPath, TypeProtos.MajorType> fields = ParquetTableMetadataUtils.resolveFields(parquetTableMetadata);

      TupleSchema schema = new TupleSchema();
      for (Map.Entry<SchemaPath, TypeProtos.MajorType> pathTypePair : fields.entrySet()) {
        SchemaPathUtils.addColumnMetadata(schema, pathTypePair.getKey(), pathTypePair.getValue());
      }

      Map<SchemaPath, ColumnStatistics> columnsStatistics;
      if (collectMetadata) {
        List<? extends BaseMetadata> metadata = getFilesMetadata();
        if (metadata == null || metadata.isEmpty()) {
          metadata = getRowGroupsMeta();
        }
        tableStatistics.put(TableStatisticsKind.ROW_COUNT.getName(), TableStatisticsKind.ROW_COUNT.mergeStatistics(metadata));
        columnsStatistics = ParquetTableMetadataUtils.mergeColumnsStatistics(metadata, fields.keySet(), ParquetTableMetadataUtils.PARQUET_STATISTICS, parquetTableMetadata);
      } else {
        columnsStatistics = new HashMap<>();
        tableStatistics.put(TableStatisticsKind.ROW_COUNT.getName(), getParquetGroupScanStatistics().getRowCount());

        for (SchemaPath partitionColumn : fields.keySet()) {
          long columnValueCount = getParquetGroupScanStatistics().getColumnValueCount(partitionColumn);
          ImmutableMap<String, Long> stats = ImmutableMap.of(
              TableStatisticsKind.ROW_COUNT.getName(), columnValueCount,
              ColumnStatisticsKind.NULLS_COUNT.getName(), getParquetGroupScanStatistics().getRowCount() - columnValueCount);
          columnsStatistics.put(partitionColumn, new ColumnStatisticsImpl(stats, ParquetTableMetadataUtils.getNaturalNullsFirstComparator()));
        }
        columnsStatistics.putAll(ParquetTableMetadataUtils.populateNonInterestingColumnsStats(columnsStatistics.keySet(), parquetTableMetadata));
      }
      tableMetadata = new FileTableMetadata(tableName, tableLocation, schema, columnsStatistics, tableStatistics,
          -1, "root", partitionKeys);
    }

    return tableMetadata;
  }

  private ParquetGroupScanStatistics<? extends BaseMetadata> getParquetGroupScanStatistics() {
    if (parquetGroupScanStatistics == null) {
      if (collectMetadata) {
        parquetGroupScanStatistics = new ParquetGroupScanStatistics<>(getFilesMetadata());
      } else {
        parquetGroupScanStatistics = new ParquetGroupScanStatistics<>(getRowGroupsMeta());
      }
    }
    return parquetGroupScanStatistics;
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    if (partitionColumns == null) {
      partitionColumns = getParquetGroupScanStatistics().getPartitionColumns();
    }
    return partitionColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<PartitionMetadata> getPartitionsMetadata() {
    if (partitions == null) {
      partitions = new ArrayList<>();
      if (collectMetadata) {
        Table<SchemaPath, Object, List<FileMetadata>> colValFile = HashBasedTable.create();

        List<FileMetadata> filesMetadata = getFilesMetadata();
        partitionColumns = getParquetGroupScanStatistics().getPartitionColumns();
        for (FileMetadata fileMetadata : filesMetadata) {
          for (SchemaPath partitionColumn : partitionColumns) {
            Object partitionValue = getParquetGroupScanStatistics().getPartitionValue(fileMetadata.getLocation(), partitionColumn);
            // Table cannot contain nulls
            partitionValue = partitionValue == null ? NULL_VALUE : partitionValue;
            List<FileMetadata> partitionFiles = colValFile.get(partitionColumn, partitionValue);
            if (partitionFiles == null) {
              partitionFiles = new ArrayList<>();
              colValFile.put(partitionColumn, partitionValue, partitionFiles);
            }
            partitionFiles.add(fileMetadata);
          }
        }

        for (SchemaPath logicalExpressions : colValFile.rowKeySet()) {
          for (List<FileMetadata> partValues : colValFile.row(logicalExpressions).values()) {
            partitions.add(ParquetTableMetadataUtils.getPartitionMetadata(logicalExpressions, partValues, tableName));
          }
        }
      } else {
        for (SchemaPath partitionColumn : getParquetGroupScanStatistics().getPartitionColumns()) {
          Map<Path, Object> partitionPaths = getParquetGroupScanStatistics().getPartitionPaths(partitionColumn);
          SetMultimap<Object, Path> partitionsForValue = HashMultimap.create();

          for (Map.Entry<Path, Object> stringObjectEntry : partitionPaths.entrySet()) {
            partitionsForValue.put(stringObjectEntry.getValue(), stringObjectEntry.getKey());
          }

          for (Map.Entry<Object, Collection<Path>> valueLocationsEntry : partitionsForValue.asMap().entrySet()) {
            Map<SchemaPath, ColumnStatistics> columnsStatistics = new HashMap<>();

            Map<String, Object> statistics = new HashMap<>();
            Object partitionKey = valueLocationsEntry.getKey();
            partitionKey = valueLocationsEntry.getKey() == NULL_VALUE ? null : partitionKey;
            statistics.put(ColumnStatisticsKind.MIN_VALUE.getName(), partitionKey);
            statistics.put(ColumnStatisticsKind.MAX_VALUE.getName(), partitionKey);

            statistics.put(ColumnStatisticsKind.NULLS_COUNT.getName(), GroupScan.NO_COLUMN_STATS);
            statistics.put(TableStatisticsKind.ROW_COUNT.getName(), GroupScan.NO_COLUMN_STATS);
            columnsStatistics.put(partitionColumn,
                new ColumnStatisticsImpl<>(statistics,
                        ParquetTableMetadataUtils.getComparator(getParquetGroupScanStatistics().getTypeForColumn(partitionColumn).getMinorType())));
            partitions.add(new PartitionMetadata(partitionColumn, getTableMetadata().getSchema(),
                columnsStatistics, statistics, (Set<Path>) valueLocationsEntry.getValue(), tableName, -1));
          }
        }
      }
    }
    return partitions;
  }

  @Override
  public List<PartitionMetadata> getPartitionMetadata(SchemaPath columnName) {
    return getPartitionsMetadata().stream()
        .filter(Objects::nonNull)
        .filter(partitionMetadata -> partitionMetadata.getColumn().equals(columnName))
        .collect(Collectors.toList());
  }

  @Override
  public FileMetadata getFileMetadata(Path location) {
    return getFilesMetadata().stream()
        .filter(Objects::nonNull)
        .filter(fileMetadata -> location.equals(fileMetadata.getLocation()))
        .findAny()
        .orElse(null);
  }

  @Override
  public List<FileMetadata> getFilesForPartition(PartitionMetadata partition) {
    List<FileMetadata> prunedFiles = new ArrayList<>();
    for (FileMetadata file : getFilesMetadata()) {
      if (partition.getLocations().contains(file.getLocation())) {
        prunedFiles.add(file);
      }
    }

    return prunedFiles;
  }

  @Override
  public List<FileMetadata> getFilesMetadata() {
    return new ArrayList<>(getFilesMetadataMap().values());
  }

  @Override
  public Map<Path, FileMetadata> getFilesMetadataMap() {
    if (files == null) {
      if (entries.isEmpty() || !collectMetadata) {
        return Collections.emptyMap();
      }
      boolean addRowGroups = false;
      files = new LinkedHashMap<>();
      if (rowGroups == null) {
        rowGroups = LinkedListMultimap.create();
        addRowGroups = true;
      }
      for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        int index = 0;
        List<RowGroupMetadata> fileRowGroups = new ArrayList<>();
        for (MetadataBase.RowGroupMetadata rowGroup : file.getRowGroups()) {
          RowGroupMetadata rowGroupMetadata = ParquetTableMetadataUtils.getRowGroupMetadata(parquetTableMetadata, rowGroup, index++, file.getPath());
          fileRowGroups.add(rowGroupMetadata);

          if (addRowGroups) {
            rowGroups.put(rowGroupMetadata.getLocation(), rowGroupMetadata);
          }
        }

        FileMetadata fileMetadata = ParquetTableMetadataUtils.getFileMetadata(fileRowGroups, tableName, parquetTableMetadata);
        files.put(fileMetadata.getLocation(), fileMetadata);
      }
    }
    return files;
  }

  @Override
  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @Override
  public Set<Path> getFileSet() {
    return fileSet;
  }

  @Override
  public List<RowGroupMetadata> getRowGroupsMeta() {
    return new ArrayList<>(getRowGroupsMetadataMap().values());
  }

  @Override
  public Multimap<Path, RowGroupMetadata> getRowGroupsMetadataMap() {
    if (rowGroups == null) {
      rowGroups = ParquetTableMetadataUtils.getRowGroupsMetadata(parquetTableMetadata);
    }
    return rowGroups;
  }

  protected abstract void initInternal() throws IOException;

}
