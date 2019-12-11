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
package org.apache.drill.exec.metastore;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.MetadataException;
import org.apache.drill.exec.metastore.MetastoreMetadataProviderManager.MetastoreMetadataProviderConfig;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.schema.SchemaProvider;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.ParquetFileTableMetadataProviderBuilder;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.exec.store.parquet.ParquetTableMetadataProviderImpl;
import org.apache.drill.exec.store.parquet.ParquetTableMetadataUtils;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.metastore.MetastoreRegistry;
import org.apache.drill.metastore.components.tables.BasicTablesRequests;
import org.apache.drill.metastore.components.tables.MetastoreTableInfo;
import org.apache.drill.metastore.metadata.BaseTableMetadata;
import org.apache.drill.metastore.metadata.FileMetadata;
import org.apache.drill.metastore.metadata.NonInterestingColumnsMetadata;
import org.apache.drill.metastore.metadata.PartitionMetadata;
import org.apache.drill.metastore.metadata.RowGroupMetadata;
import org.apache.drill.metastore.metadata.SegmentMetadata;
import org.apache.drill.metastore.metadata.TableInfo;
import org.apache.drill.metastore.metadata.TableMetadata;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.ColumnStatisticsKind;
import org.apache.drill.metastore.statistics.Statistic;
import org.apache.drill.metastore.statistics.StatisticsHolder;
import org.apache.drill.metastore.util.SchemaPathUtils;
import org.apache.drill.shaded.guava.com.google.common.collect.LinkedListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetastoreParquetTableMetadataProvider implements ParquetTableMetadataProvider {
  private static final Logger logger = LoggerFactory.getLogger(MetastoreParquetTableMetadataProvider.class);

  private final BasicTablesRequests basicTablesRequests;
  private final TableInfo tableInfo;
  private final MetastoreTableInfo metastoreTableInfo;
  private final TupleMetadata schema;
  private final List<ReadEntryWithPath> entries;
  private final List<String> paths;
  private final DrillStatsTable statsProvider;

  private final boolean useSchema;
  private final boolean useStatistics;
  private final boolean fallbackToFileMetadata;

  private BaseTableMetadata tableMetadata;
  private Map<Path, SegmentMetadata> segmentsMetadata;
  private List<PartitionMetadata> partitions;
  private Map<Path, FileMetadata> files;
  private Multimap<Path, RowGroupMetadata> rowGroups;
  private NonInterestingColumnsMetadata nonInterestingColumnsMetadata;
  // stores builder to provide lazy init for fallback ParquetTableMetadataProvider
  private final ParquetFileTableMetadataProviderBuilder fallbackBuilder;
  private ParquetTableMetadataProvider fallback;

  private MetastoreParquetTableMetadataProvider(List<ReadEntryWithPath> entries,
      MetastoreRegistry metastoreRegistry, TableInfo tableInfo, TupleMetadata schema,
      ParquetFileTableMetadataProviderBuilder fallbackBuilder, MetastoreMetadataProviderConfig config, DrillStatsTable statsProvider) {
    this.basicTablesRequests = metastoreRegistry.get().tables().basicRequests();
    this.tableInfo = tableInfo;
    this.metastoreTableInfo = basicTablesRequests.metastoreTableInfo(tableInfo);
    this.useSchema = config.useSchema();
    this.useStatistics = config.useStatistics();
    this.fallbackToFileMetadata = config.fallbackToFileMetadata();
    this.schema = schema;
    this.entries = entries == null ? new ArrayList<>() : entries;
    this.fallbackBuilder = fallbackBuilder;
    this.statsProvider = statsProvider;
    this.paths = this.entries.stream()
        .map(readEntryWithPath -> readEntryWithPath.getPath().toUri().getPath())
        .collect(Collectors.toList());
  }

  @Override
  public boolean isUsedMetadataCache() {
    return false;
  }

  @Override
  public Path getSelectionRoot() {
    return getTableMetadata().getLocation();
  }

  @Override
  public List<ReadEntryWithPath> getEntries() {
    return entries;
  }

  @Override
  public List<RowGroupMetadata> getRowGroupsMeta() {
    return new ArrayList<>(getRowGroupsMetadataMap().values());
  }

  @Override
  public List<Path> getLocations() {
    return new ArrayList<>(getFilesMetadataMap().keySet());
  }

  @Override
  public Multimap<Path, RowGroupMetadata> getRowGroupsMetadataMap() {
    throwIfChanged();
    if (rowGroups == null) {
      rowGroups = LinkedListMultimap.create();
      basicTablesRequests.rowGroupsMetadata(tableInfo, null, paths).stream()
          .collect(Collectors.groupingBy(RowGroupMetadata::getPath, Collectors.toList()))
          .forEach((path, rowGroupMetadata) -> rowGroups.putAll(path, rowGroupMetadata));
      if (rowGroups.isEmpty()) {
        if (fallbackToFileMetadata) {
          try {
            rowGroups = getFallbackTableMetadataProvider().getRowGroupsMetadataMap();
          } catch (IOException e) {
            throw MetadataException.of(MetadataException.MetadataExceptionType.FALLBACK_EXCEPTION, e);
          }
        } else {
          throw MetadataException.of(MetadataException.MetadataExceptionType.INCOMPLETE_METADATA);
        }
      }
    }
    return rowGroups;
  }

  @Override
  public Set<Path> getFileSet() {
    throwIfChanged();
    return getFilesMetadataMap().keySet();
  }

  @Override
  public TableMetadata getTableMetadata() {
    throwIfChanged();
    if (tableMetadata == null) {
      if (schema == null) {
        if (useSchema) {
          tableMetadata = basicTablesRequests.tableMetadata(tableInfo);
        } else {
          throw MetadataException.of(MetadataException.MetadataExceptionType.ABSENT_SCHEMA);
        }
      } else {
        tableMetadata = basicTablesRequests.tableMetadata(tableInfo).toBuilder()
            .schema(schema)
            .build();
      }

      if (!useStatistics) {
        // removes statistics to prevent its usage later
        tableMetadata = tableMetadata.toBuilder()
            .columnsStatistics(Collections.emptyMap())
            .build();
      }

      if (statsProvider != null) {
        if (!statsProvider.isMaterialized()) {
          statsProvider.materialize();
        }
        tableMetadata = tableMetadata.cloneWithStats(
            ParquetTableMetadataUtils.getColumnStatistics(tableMetadata.getSchema(), statsProvider),
            DrillStatsTable.getEstimatedTableStats(statsProvider));
      }
    }
    return tableMetadata;
  }

  @Override
  public List<SchemaPath> getPartitionColumns() {
    throwIfChanged();
    return basicTablesRequests.interestingColumnsAndPartitionKeys(tableInfo).partitionKeys().values().stream()
        .map(SchemaPath::getSimplePath)
        .collect(Collectors.toList());
  }

  @Override
  public List<PartitionMetadata> getPartitionsMetadata() {
    throwIfChanged();
    if (partitions == null) {
      partitions = basicTablesRequests.partitionsMetadata(tableInfo, null, null);
    }
    return partitions;
  }

  @Override
  public List<PartitionMetadata> getPartitionMetadata(SchemaPath columnName) {
    throwIfChanged();
    return basicTablesRequests.partitionsMetadata(tableInfo, null, columnName.getRootSegmentPath());
  }

  @Override
  public Map<Path, FileMetadata> getFilesMetadataMap() {
    throwIfChanged();
    if (files == null) {
      files = basicTablesRequests.filesMetadata(tableInfo, null, paths).stream()
          .collect(Collectors.toMap(FileMetadata::getPath, Function.identity()));
    }
    return files;
  }

  @Override
  public Map<Path, SegmentMetadata> getSegmentsMetadataMap() {
    throwIfChanged();
    if (segmentsMetadata == null) {
      segmentsMetadata = basicTablesRequests.segmentsMetadataByColumn(tableInfo, null, null).stream()
          .collect(Collectors.toMap(SegmentMetadata::getPath, Function.identity()));
    }
    return segmentsMetadata;
  }

  @Override
  public FileMetadata getFileMetadata(Path location) {
    throwIfChanged();
    return basicTablesRequests.fileMetadata(tableInfo, null, location.toUri().getPath());
  }

  @Override
  public List<FileMetadata> getFilesForPartition(PartitionMetadata partition) {
    throwIfChanged();
    List<String> paths = partition.getLocations().stream()
        .map(path -> path.toUri().getPath())
        .collect(Collectors.toList());
    return basicTablesRequests.filesMetadata(tableInfo, null, paths);
  }

  @Override
  public NonInterestingColumnsMetadata getNonInterestingColumnsMetadata() {
    throwIfChanged();
    if (nonInterestingColumnsMetadata == null) {
      TupleMetadata schema = getTableMetadata().getSchema();

      List<StatisticsHolder<?>> statistics = Collections.singletonList(new StatisticsHolder<>(Statistic.NO_COLUMN_STATS, ColumnStatisticsKind.NULLS_COUNT));

      List<SchemaPath> columnPaths = SchemaUtil.getSchemaPaths(schema);
      List<SchemaPath> interestingColumns = getInterestingColumns(columnPaths);
      // populates statistics for non-interesting columns and columns for which statistics wasn't collected
      Map<SchemaPath, ColumnStatistics<?>> columnsStatistics = columnPaths.stream()
          .filter(schemaPath -> !interestingColumns.contains(schemaPath)
              || SchemaPathUtils.getColumnMetadata(schemaPath, schema).isArray())
          .collect(Collectors.toMap(
              Function.identity(),
              schemaPath -> new ColumnStatistics<>(statistics, SchemaPathUtils.getColumnMetadata(schemaPath, schema).type())));
      nonInterestingColumnsMetadata = new NonInterestingColumnsMetadata(columnsStatistics);
    }
    return nonInterestingColumnsMetadata;
  }

  @Override
  public boolean checkMetadataVersion() {
    return true;
  }

  private List<SchemaPath> getInterestingColumns(List<SchemaPath> columnPaths) {
    if (useStatistics) {
      return getTableMetadata().getInterestingColumns() == null
          ? columnPaths
          : getTableMetadata().getInterestingColumns();
    } else {
      // if `metastore.metadata.use_statistics` is false, all columns are treated as non-interesting
      return Collections.emptyList();
    }
  }

  private ParquetTableMetadataProvider getFallbackTableMetadataProvider() throws IOException {
    if (fallback == null) {
      fallback = fallbackBuilder == null ? null : fallbackBuilder.build();
    }
    return fallback;
  }

  private void throwIfChanged() {
    if (basicTablesRequests.hasMetastoreTableInfoChanged(metastoreTableInfo)) {
      throw MetadataException.of(MetadataException.MetadataExceptionType.INCONSISTENT_METADATA);
    }
  }

  public static class Builder implements ParquetFileTableMetadataProviderBuilder {
    private final MetastoreMetadataProviderManager metadataProviderManager;

    private List<ReadEntryWithPath> entries;
    private DrillFileSystem fs;
    private TupleMetadata schema;

    private FileSelection selection;

    // builder for fallback ParquetFileTableMetadataProvider
    // for the case when required metadata is absent in Metastore
    private final ParquetFileTableMetadataProviderBuilder fallback;

    public Builder(MetastoreMetadataProviderManager source) {
      this.metadataProviderManager = source;
      this.fallback = new ParquetTableMetadataProviderImpl.Builder(FileSystemMetadataProviderManager.init());
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withEntries(List<ReadEntryWithPath> entries) {
      this.entries = entries;
      fallback.withEntries(entries);
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withSelectionRoot(Path selectionRoot) {
      fallback.withSelectionRoot(selectionRoot);
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withCacheFileRoot(Path cacheFileRoot) {
      fallback.withCacheFileRoot(cacheFileRoot);
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withReaderConfig(ParquetReaderConfig readerConfig) {
      fallback.withReaderConfig(readerConfig);
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withFileSystem(DrillFileSystem fs) {
      fallback.withFileSystem(fs);
      this.fs = fs;
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withCorrectCorruptedDates(boolean autoCorrectCorruptedDates) {
      fallback.withCorrectCorruptedDates(autoCorrectCorruptedDates);
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withSelection(FileSelection selection) {
      fallback.withSelection(selection);
      this.selection = selection;
      return this;
    }

    @Override
    public ParquetFileTableMetadataProviderBuilder withSchema(TupleMetadata schema) {
      fallback.withSchema(schema);
      this.schema = schema;
      return this;
    }

    @Override
    public ParquetTableMetadataProvider build() throws IOException {
      MetastoreParquetTableMetadataProvider provider;
      SchemaProvider schemaProvider = metadataProviderManager.getSchemaProvider();
      ParquetMetadataProvider source = (ParquetTableMetadataProvider) metadataProviderManager.getTableMetadataProvider();

      DrillStatsTable statsProvider = metadataProviderManager.getStatsProvider();
      // schema passed into the builder has greater priority
      try {
        if (this.schema == null) {
          schema = schemaProvider != null ? schemaProvider.read().getSchema() : null;
        }
      } catch (IOException e) {
        logger.debug("Unable to deserialize schema from schema file for table: {}", metadataProviderManager.getTableInfo().name(), e);
      }
      if (entries == null) {
        if (!selection.isExpandedFully()) {
          entries = DrillFileSystemUtil.listFiles(fs, selection.getSelectionRoot(), true).stream()
              .map(fileStatus -> new ReadEntryWithPath(Path.getPathWithoutSchemeAndAuthority(fileStatus.getPath())))
              .collect(Collectors.toList());
        } else {
          entries = selection.getFiles().stream()
              .map(Path::getPathWithoutSchemeAndAuthority)
              .map(ReadEntryWithPath::new)
              .collect(Collectors.toList());
        }
      }
      provider = new MetastoreParquetTableMetadataProvider(entries, metadataProviderManager.getMetastoreRegistry(),
          metadataProviderManager.getTableInfo(), schema, fallback, metadataProviderManager.getConfig(), statsProvider);
      // store results into metadataProviderManager to be able to use them when creating new instances
      // for the case when source wasn't provided or it contains less row group metadata than the provider
      if (source == null || source.getRowGroupsMeta().size() < provider.getRowGroupsMeta().size()) {
        metadataProviderManager.setTableMetadataProvider(provider);
      }
      return provider;
    }
  }
}
