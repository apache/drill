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
package org.apache.drill.exec.store.parquet.metadata;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collects file metadata for the given parquet file. For empty parquet file will
 * generate fake row group metadata based on file schema.
 */
public class FileMetadataCollector {

  private static final Logger logger = LoggerFactory.getLogger(FileMetadataCollector.class);

  private final ParquetMetadata metadata;
  private final FileStatus file;
  private final FileSystem fs;
  private final boolean allColumnsInteresting;
  private final boolean skipNonInteresting;
  private final Set<String> columnSet;

  private final MessageType schema;
  private final ParquetReaderUtility.DateCorruptionStatus containsCorruptDates;
  private final Map<SchemaPath, ColTypeInfo> colTypeInfoMap;

  private final Map<Metadata_V4.ColumnTypeMetadata_v4.Key, Long> totalNullCountMap = new HashMap<>();
  private final Map<Metadata_V4.ColumnTypeMetadata_v4.Key, Metadata_V4.ColumnTypeMetadata_v4> columnTypeInfo = new HashMap<>();

  private Metadata_V4.ParquetFileAndRowCountMetadata fileMetadata;

  public FileMetadataCollector(ParquetMetadata metadata,
                               FileStatus file,
                               FileSystem fs,
                               boolean allColumnsInteresting,
                               boolean skipNonInteresting,
                               Set<String> columnSet,
                               ParquetReaderConfig readerConfig) throws IOException {
    this.metadata = metadata;
    this.file = file;
    this.fs = fs;
    this.allColumnsInteresting = allColumnsInteresting;
    this.skipNonInteresting = skipNonInteresting;
    this.columnSet = columnSet;

    this.schema = metadata.getFileMetaData().getSchema();

    this.containsCorruptDates =
      ParquetReaderUtility.detectCorruptDates(metadata, Collections.singletonList(SchemaPath.STAR_COLUMN),
        readerConfig.autoCorrectCorruptedDates());
    logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

    this.colTypeInfoMap = new HashMap<>();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), ColTypeInfo.of(schema, schema, path, 0));
    }

    init();
  }

  public Metadata_V4.ParquetFileAndRowCountMetadata getFileMetadata() {
    return fileMetadata;
  }

  public Map<Metadata_V4.ColumnTypeMetadata_v4.Key, Metadata_V4.ColumnTypeMetadata_v4> getColumnTypeInfo() {
    return columnTypeInfo;
  }

  private void init() throws IOException {
    long totalRowCount = 0;

    List<Metadata_V4.RowGroupMetadata_v4> rowGroupMetadataList = new ArrayList<>();

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<Metadata_V4.ColumnMetadata_v4> columnMetadataList = new ArrayList<>();
      long length = 0;
      totalRowCount = totalRowCount + rowGroup.getRowCount();
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        String[] columnName = col.getPath().toArray();
        Statistics<?> stats = col.getStatistics();
        PrimitiveType.PrimitiveTypeName primitiveTypeName = col.getPrimitiveType().getPrimitiveTypeName();
        addColumnMetadata(columnName, stats, primitiveTypeName, columnMetadataList);
        length += col.getTotalSize();
      }

      // DRILL-5009: Skip the RowGroup if it is empty
      // Note we still read the schema even if there are no values in the RowGroup
      if (rowGroup.getRowCount() == 0) {
        continue;
      }

      Metadata_V4.RowGroupMetadata_v4 rowGroupMeta = new Metadata_V4.RowGroupMetadata_v4(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
        getHostAffinity(rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }

    // add fake row group based on file schema in case when file is empty or all row groups are empty
    if (rowGroupMetadataList.isEmpty()) {
      List<Metadata_V4.ColumnMetadata_v4> columnMetadataList = new ArrayList<>();
      for (ColumnDescriptor columnDescriptor : schema.getColumns()) {

        Statistics<?> stats = Statistics.getBuilderForReading(columnDescriptor.getPrimitiveType())
          .withMax(null)
          .withMin(null)
          .withNumNulls(0)
          .build();

        addColumnMetadata(columnDescriptor.getPath(), stats,
          columnDescriptor.getPrimitiveType().getPrimitiveTypeName(), columnMetadataList);
      }

      Metadata_V4.RowGroupMetadata_v4 rowGroupMeta = new Metadata_V4.RowGroupMetadata_v4(0L, 0L,
        0L, getHostAffinity(0, 0L), columnMetadataList);
      rowGroupMetadataList.add(rowGroupMeta);
    }

    Path path = Path.getPathWithoutSchemeAndAuthority(file.getPath());
    Metadata_V4.ParquetFileMetadata_v4 parquetFileMetadata_v4 = new Metadata_V4.ParquetFileMetadata_v4(path, file.getLen(), rowGroupMetadataList);
    this.fileMetadata = new Metadata_V4.ParquetFileAndRowCountMetadata(parquetFileMetadata_v4, totalNullCountMap, totalRowCount);
  }

  private void addColumnMetadata(String[] columnName,
                                 Statistics<?> stats,
                                 PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                 List<Metadata_V4.ColumnMetadata_v4> columnMetadataList) {
    SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
    boolean thisColumnIsInteresting = allColumnsInteresting || columnSet == null || columnSet.contains(columnSchemaName.getRootSegmentPath());

    if (skipNonInteresting && !thisColumnIsInteresting) {
      return;
    }

    ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);
    long totalNullCount = stats.getNumNulls();
    Metadata_V4.ColumnTypeMetadata_v4 columnTypeMetadata = new Metadata_V4.ColumnTypeMetadata_v4(
      columnName, primitiveTypeName,
      colTypeInfo.originalType, colTypeInfo.precision, colTypeInfo.scale,
      colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel, 0, false);
    Metadata_V4.ColumnTypeMetadata_v4.Key columnTypeMetadataKey = new Metadata_V4.ColumnTypeMetadata_v4.Key(columnTypeMetadata.name);

    totalNullCountMap.putIfAbsent(columnTypeMetadataKey, Metadata.DEFAULT_NULL_COUNT);
    if (totalNullCountMap.get(columnTypeMetadataKey) < 0 || totalNullCount < 0) {
      totalNullCountMap.put(columnTypeMetadataKey, Metadata.NULL_COUNT_NOT_EXISTS);
    } else {
      long nullCount = totalNullCountMap.get(columnTypeMetadataKey) + totalNullCount;
      totalNullCountMap.put(columnTypeMetadataKey, nullCount);
    }
    if (thisColumnIsInteresting) {
      // Save the column schema info. We'll merge it into one list
      Object minValue = null;
      Object maxValue = null;
      if (!stats.isEmpty() && stats.hasNonNullValue()) {
        minValue = stats.genericGetMin();
        maxValue = stats.genericGetMax();
        if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION
          && columnTypeMetadata.originalType == OriginalType.DATE) {
          minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
          maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
        }
      }
      long numNulls = stats.getNumNulls();
      Metadata_V4.ColumnMetadata_v4 columnMetadata = new Metadata_V4.ColumnMetadata_v4(columnTypeMetadata.name,
        primitiveTypeName, minValue, maxValue, numNulls);
      columnMetadataList.add(columnMetadata);
      columnTypeMetadata.isInteresting = true;
    }
    columnTypeInfo.put(columnTypeMetadataKey, columnTypeMetadata);
  }

  /**
   * Get the host affinity for a row group.
   *
   * @param start the start of the row group
   * @param length the length of the row group
   * @return host affinity for the row group
   */
  private Map<String, Float> getHostAffinity(long start, long length) throws IOException {
    Map<String, Float> hostAffinityMap = new HashMap<>();
    BlockLocation[] blockLocations = fs.getFileBlockLocations(file, start, length);
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        float affinity;
        if (length == 0) {
          affinity = 0.0F;
        } else {
          float blockStart = blockLocation.getOffset();
          float blockEnd = blockStart + blockLocation.getLength();
          float rowGroupEnd = start + length;
          float calcStart = blockStart < start ? start - blockStart : 0;
          float calcEnd = blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0;
          affinity = (blockLocation.getLength() - calcStart - calcEnd) / length;
        }
       hostAffinityMap.merge(host, affinity, (currentAffinity, newAffinity) -> currentAffinity + newAffinity);
      }
    }
    return hostAffinityMap;
  }

  private static class ColTypeInfo {

    OriginalType originalType;
    int precision;
    int scale;
    int repetitionLevel;
    int definitionLevel;

    ColTypeInfo(OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }

    static ColTypeInfo of(MessageType schema, Type type, String[] path, int depth) {
      if (type.isPrimitive()) {
        PrimitiveType primitiveType = (PrimitiveType) type;
        int precision = 0;
        int scale = 0;
        if (primitiveType.getDecimalMetadata() != null) {
          precision = primitiveType.getDecimalMetadata().getPrecision();
          scale = primitiveType.getDecimalMetadata().getScale();
        }

        int repetitionLevel = schema.getMaxRepetitionLevel(path);
        int definitionLevel = schema.getMaxDefinitionLevel(path);

        return new ColTypeInfo(type.getOriginalType(), precision, scale, repetitionLevel, definitionLevel);
      }
      Type t = ((GroupType) type).getType(path[depth]);
      return of(schema, t, path, depth + 1);
    }
  }
}
