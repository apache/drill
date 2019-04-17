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

import org.apache.drill.exec.physical.base.ParquetMetadataProvider;
import org.apache.drill.exec.physical.impl.statistics.Statistic;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.ColumnStatisticsImpl;
import org.apache.drill.metastore.StatisticsKind;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.shaded.guava.com.google.common.collect.HashBasedTable;
import org.apache.drill.shaded.guava.com.google.common.collect.HashMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.LinkedListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
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
  protected TupleMetadata schema;
  protected DrillStatsTable statsTable;

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
                                     Path tableLocation,
                                     TupleMetadata schema,
                                     DrillStatsTable statsTable) {
    this(readerConfig, entries, tableName, tableLocation, schema, statsTable);
  }

  public BaseParquetMetadataProvider(ParquetReaderConfig readerConfig,
                                     List<ReadEntryWithPath> entries,
                                     String tableName,
                                     Path tableLocation,
                                     TupleMetadata schema,
                                     DrillStatsTable statsTable) {
    this.entries = entries == null ? new ArrayList<>() : entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
    this.tableName = tableName;
    this.tableLocation = tableLocation;
    this.schema = schema;
    this.statsTable = statsTable;
  }

  public BaseParquetMetadataProvider(List<ReadEntryWithPath> entries, ParquetReaderConfig readerConfig) {
    this.entries = entries == null ? new ArrayList<>() : entries;
    this.readerConfig = readerConfig == null ? ParquetReaderConfig.getDefaultInstance() : readerConfig;
    this.tableName = null;
    this.tableLocation = null;
  }

  protected void init(BaseParquetMetadataProvider metadataProvider) throws IOException {
    // Once deserialization for metadata is provided, initInternal() call should be removed
    // and only files list is deserialized based on specified locations
    initInternal();

    assert parquetTableMetadata != null;

    if (fileSet == null) {
      fileSet = new HashSet<>();
      fileSet.addAll(parquetTableMetadata.getFiles().stream()
          .map(MetadataBase.ParquetFileMetadata::getPath)
          .collect(Collectors.toSet()));
    }

    List<Path> fileLocations = getLocations();

    // if metadata provider wasn't specified, or required metadata is absent,
    // obtains metadata from cache files or table footers
    if (metadataProvider == null
        || (metadataProvider.rowGroups != null && !metadataProvider.rowGroups.keySet().containsAll(fileLocations))
        || (metadataProvider.files != null && !metadataProvider.files.keySet().containsAll(fileLocations))) {
      initializeMetadata();
    } else {
      // reuse metadata from existing TableMetadataProvider
      if (metadataProvider.files != null && metadataProvider.files.size() != files.size()) {
        files = metadataProvider.files.entrySet().stream()
            .filter(entry -> fileLocations.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      if (metadataProvider.rowGroups != null) {
        rowGroups = LinkedListMultimap.create();
        metadataProvider.rowGroups.entries().stream()
            .filter(entry -> fileLocations.contains(entry.getKey()))
            .forEach(entry -> rowGroups.put(entry.getKey(), entry.getValue()));
      }
      TableMetadata tableMetadata = getTableMetadata();
      getPartitionsMetadata();
      getRowGroupsMeta();
      this.tableMetadata = ParquetTableMetadataUtils.updateRowCount(tableMetadata, getRowGroupsMeta());
      parquetTableMetadata = null;
    }
  }

  /**
   * Method which initializes all metadata kinds to get rid of parquetTableMetadata.
   * Once deserialization and serialization from/into metastore classes is done, this method should be removed
   * to allow lazy initialization.
   */
  public void initializeMetadata() throws IOException {
    if (statsTable != null && !statsTable.isMaterialized()) {
      statsTable.materialize();
    }
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
      Map<StatisticsKind, Object> tableStatistics = new HashMap<>(DrillStatsTable.getEstimatedTableStats(statsTable));
      Set<String> partitionKeys = new HashSet<>();
      Map<SchemaPath, TypeProtos.MajorType> fields = ParquetTableMetadataUtils.resolveFields(parquetTableMetadata);

      if (this.schema == null) {
        schema = new TupleSchema();
        fields.forEach((schemaPath, majorType) -> MetadataUtils.addColumnMetadata(schema, schemaPath, majorType));
      } else {
        // merges specified schema with schema from table
        fields.forEach((schemaPath, majorType) -> {
          if (SchemaPathUtils.getColumnMetadata(schemaPath, schema) == null) {
            MetadataUtils.addColumnMetadata(schema, schemaPath, majorType);
          }
        });
      }

      Map<SchemaPath, ColumnStatistics> columnsStatistics;
      if (collectMetadata) {
        List<? extends BaseMetadata> metadata = getFilesMetadata();
        if (metadata == null || metadata.isEmpty()) {
          metadata = getRowGroupsMeta();
        }
        tableStatistics.put(TableStatisticsKind.ROW_COUNT, TableStatisticsKind.ROW_COUNT.mergeStatistics(metadata));
        columnsStatistics = ParquetTableMetadataUtils.mergeColumnsStatistics(metadata, fields.keySet(), ParquetTableMetadataUtils.PARQUET_STATISTICS, parquetTableMetadata);
      } else {
        columnsStatistics = new HashMap<>();
        tableStatistics.put(TableStatisticsKind.ROW_COUNT, getParquetGroupScanStatistics().getRowCount());

        Set<SchemaPath> unhandledColumns = new HashSet<>();
        if (statsTable != null && statsTable.isMaterialized()) {
          unhandledColumns.addAll(statsTable.getColumns());
        }

        for (SchemaPath partitionColumn : fields.keySet()) {
          long columnValueCount = getParquetGroupScanStatistics().getColumnValueCount(partitionColumn);
          // Adds statistics values itself if statistics is available
          Map<StatisticsKind, Object> stats = new HashMap<>(DrillStatsTable.getEstimatedColumnStats(statsTable, partitionColumn));
          unhandledColumns.remove(partitionColumn);

          // adds statistics for partition columns
          stats.put(TableStatisticsKind.ROW_COUNT, columnValueCount);
          stats.put(ColumnStatisticsKind.NULLS_COUNT, getParquetGroupScanStatistics().getRowCount() - columnValueCount);
          columnsStatistics.put(partitionColumn, new ColumnStatisticsImpl(stats, ParquetTableMetadataUtils.getNaturalNullsFirstComparator()));
        }

        for (SchemaPath column : unhandledColumns) {
          columnsStatistics.put(column,
              new ColumnStatisticsImpl(DrillStatsTable.getEstimatedColumnStats(statsTable, column),
                  ParquetTableMetadataUtils.getNaturalNullsFirstComparator()));
        }
        columnsStatistics.putAll(ParquetTableMetadataUtils.populateNonInterestingColumnsStats(columnsStatistics.keySet(), parquetTableMetadata));
      }
      tableMetadata = new FileTableMetadata(tableName, tableLocation, schema, columnsStatistics, tableStatistics,
          -1L, "", partitionKeys);
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
          Multimap<Object, Path> partitionsForValue = HashMultimap.create();

          partitionPaths.forEach((path, value) -> partitionsForValue.put(value, path));

          partitionsForValue.asMap().forEach((partitionKey, value) -> {
            Map<SchemaPath, ColumnStatistics> columnsStatistics = new HashMap<>();

            Map<StatisticsKind, Object> statistics = new HashMap<>();
            partitionKey = partitionKey == NULL_VALUE ? null : partitionKey;
            statistics.put(ColumnStatisticsKind.MIN_VALUE, partitionKey);
            statistics.put(ColumnStatisticsKind.MAX_VALUE, partitionKey);

            statistics.put(ColumnStatisticsKind.NULLS_COUNT, Statistic.NO_COLUMN_STATS);
            statistics.put(TableStatisticsKind.ROW_COUNT, Statistic.NO_COLUMN_STATS);
            columnsStatistics.put(partitionColumn,
                new ColumnStatisticsImpl<>(statistics,
                        ParquetTableMetadataUtils.getComparator(getParquetGroupScanStatistics().getTypeForColumn(partitionColumn).getMinorType())));
            partitions.add(new PartitionMetadata(partitionColumn, getTableMetadata().getSchema(),
                columnsStatistics, statistics, (Set<Path>) value, tableName, -1));
          });
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
    return getFilesMetadata().stream()
        .filter(file -> partition.getLocations().contains(file.getLocation()))
        .collect(Collectors.toList());
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
  public List<Path> getLocations() {
    return parquetTableMetadata.getFiles().stream()
        .map(MetadataBase.ParquetFileMetadata::getPath)
        .collect(Collectors.toList());
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
