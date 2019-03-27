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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.record.metadata.SchemaPathUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.resolver.TypeCastRules;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.parquet.metadata.MetadataBase;
import org.apache.drill.exec.store.parquet.metadata.MetadataVersion;
import org.apache.drill.exec.store.parquet.metadata.Metadata_V4;
import org.apache.drill.metastore.BaseMetadata;
import org.apache.drill.metastore.CollectableColumnStatisticsKind;
import org.apache.drill.metastore.ColumnStatistics;
import org.apache.drill.metastore.ColumnStatisticsImpl;
import org.apache.drill.metastore.ColumnStatisticsKind;
import org.apache.drill.metastore.FileMetadata;
import org.apache.drill.metastore.PartitionMetadata;
import org.apache.drill.metastore.RowGroupMetadata;
import org.apache.drill.metastore.StatisticsKind;
import org.apache.drill.metastore.TableMetadata;
import org.apache.drill.metastore.TableStatisticsKind;
import org.apache.drill.exec.expr.ExactStatisticsConstants;
import org.apache.drill.exec.expr.StatisticsProvider;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.LinkedListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Multimap;
import org.apache.drill.shaded.guava.com.google.common.primitives.Longs;
import org.apache.drill.shaded.guava.com.google.common.primitives.UnsignedBytes;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class for converting parquet metadata classes to metastore metadata classes.
 */
@SuppressWarnings("WeakerAccess")
public class ParquetTableMetadataUtils {

  private static final Comparator<byte[]> UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR = Comparator.nullsFirst((b1, b2) ->
      PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(Binary.fromReusedByteArray(b1), Binary.fromReusedByteArray(b2)));

  static final List<CollectableColumnStatisticsKind> PARQUET_STATISTICS =
          ImmutableList.of(
              ColumnStatisticsKind.MAX_VALUE,
              ColumnStatisticsKind.MIN_VALUE,
              ColumnStatisticsKind.NULLS_COUNT);

  private ParquetTableMetadataUtils() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Creates new map based on specified {@code columnStatistics} with added statistics
   * for implicit and partition (dir) columns.
   *
   * @param columnsStatistics           map of column statistics to expand
   * @param columns                     list of all columns including implicit or partition ones
   * @param partitionValues             list of partition values
   * @param optionManager               option manager
   * @param location                    location of metadata part
   * @param supportsFileImplicitColumns whether implicit columns are supported
   * @return map with added statistics for implicit and partition (dir) columns
   */
  public static Map<SchemaPath, ColumnStatistics> addImplicitColumnsStatistics(
      Map<SchemaPath, ColumnStatistics> columnsStatistics, List<SchemaPath> columns,
      List<String> partitionValues, OptionManager optionManager, Path location, boolean supportsFileImplicitColumns) {
    ColumnExplorer columnExplorer = new ColumnExplorer(optionManager, columns);

    Map<String, String> implicitColValues = columnExplorer.populateImplicitColumns(
        location, partitionValues, supportsFileImplicitColumns);
    columnsStatistics = new HashMap<>(columnsStatistics);
    for (Map.Entry<String, String> partitionValue : implicitColValues.entrySet()) {
      columnsStatistics.put(SchemaPath.getCompoundPath(partitionValue.getKey()),
          new StatisticsProvider.MinMaxStatistics<>(partitionValue.getValue(),
              partitionValue.getValue(), Comparator.nullsFirst(Comparator.naturalOrder())));
    }
    return columnsStatistics;
  }

  /**
   * Returns list of {@link RowGroupMetadata} received by converting parquet row groups metadata
   * taken from the specified tableMetadata.
   *
   * @param tableMetadata the source of row groups to be converted
   * @return list of {@link RowGroupMetadata}
   */
  public static Multimap<Path, RowGroupMetadata> getRowGroupsMetadata(MetadataBase.ParquetTableMetadataBase tableMetadata) {
    Multimap<Path, RowGroupMetadata> rowGroups = LinkedListMultimap.create();
    for (MetadataBase.ParquetFileMetadata file : tableMetadata.getFiles()) {
      int index = 0;
      for (MetadataBase.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
        rowGroups.put(file.getPath(), getRowGroupMetadata(tableMetadata, rowGroupMetadata, index++, file.getPath()));
      }
    }

    return rowGroups;
  }

  /**
   * Returns {@link RowGroupMetadata} instance converted from specified parquet {@code rowGroupMetadata}.
   *
   * @param tableMetadata    table metadata which contains row group metadata to convert
   * @param rowGroupMetadata row group metadata to convert
   * @param rgIndexInFile    index of current row group within the file
   * @param location         location of file with current row group
   * @return {@link RowGroupMetadata} instance converted from specified parquet {@code rowGroupMetadata}
   */
  public static RowGroupMetadata getRowGroupMetadata(MetadataBase.ParquetTableMetadataBase tableMetadata,
      MetadataBase.RowGroupMetadata rowGroupMetadata, int rgIndexInFile, Path location) {
    Map<SchemaPath, ColumnStatistics> columnsStatistics = getRowGroupColumnStatistics(tableMetadata, rowGroupMetadata);
    Map<StatisticsKind, Object> rowGroupStatistics = new HashMap<>();
    rowGroupStatistics.put(TableStatisticsKind.ROW_COUNT, rowGroupMetadata.getRowCount());
    rowGroupStatistics.put(() -> ExactStatisticsConstants.START, rowGroupMetadata.getStart());
    rowGroupStatistics.put(() -> ExactStatisticsConstants.LENGTH, rowGroupMetadata.getLength());

    Map<SchemaPath, TypeProtos.MajorType> columns = getRowGroupFields(tableMetadata, rowGroupMetadata);

    TupleSchema schema = new TupleSchema();
    columns.forEach((schemaPath, majorType) -> SchemaPathUtils.addColumnMetadata(schema, schemaPath, majorType));

    return new RowGroupMetadata(
        schema, columnsStatistics, rowGroupStatistics, rowGroupMetadata.getHostAffinity(), rgIndexInFile, location);
  }

  /**
   * Merges list of specified metadata into the map of {@link ColumnStatistics} with columns as keys.
   *
   * @param <T>                 type of metadata to collect
   * @param metadataList        list of metadata to be merged
   * @param columns             set of columns whose statistics should be merged
   * @param statisticsToCollect kinds of statistics that should be collected
   * @param parquetTableMetadata ParquetTableMetadata object to fetch the non-interesting columns
   * @return list of merged metadata
   */
  @SuppressWarnings("unchecked")
  public static <T extends BaseMetadata> Map<SchemaPath, ColumnStatistics> mergeColumnsStatistics(
          Collection<T> metadataList, Set<SchemaPath> columns, List<CollectableColumnStatisticsKind> statisticsToCollect, MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    Map<SchemaPath, ColumnStatistics> columnsStatistics = new HashMap<>();

    for (SchemaPath column : columns) {
      List<ColumnStatistics> statisticsList = new ArrayList<>();
      for (T metadata : metadataList) {
        ColumnStatistics statistics = metadata.getColumnsStatistics().get(column);
        if (statistics == null) {
          // schema change happened, set statistics which represents all nulls
          statistics = new ColumnStatisticsImpl(
              ImmutableMap.of(ColumnStatisticsKind.NULLS_COUNT, metadata.getStatistic(TableStatisticsKind.ROW_COUNT)),
              getNaturalNullsFirstComparator());
        }
        statisticsList.add(statistics);
      }
      Map<StatisticsKind, Object> statisticsMap = new HashMap<>();
      for (CollectableColumnStatisticsKind statisticsKind : statisticsToCollect) {
        Object mergedStatistic = statisticsKind.mergeStatistics(statisticsList);
        statisticsMap.put(statisticsKind, mergedStatistic);
      }
      columnsStatistics.put(column, new ColumnStatisticsImpl(statisticsMap, statisticsList.iterator().next().getValueComparator()));
    }
    columnsStatistics.putAll(populateNonInterestingColumnsStats(columnsStatistics.keySet(), parquetTableMetadata));
    return columnsStatistics;
  }

  /**
   * Returns {@link FileMetadata} instance received by merging specified {@link RowGroupMetadata} list.
   *
   * @param rowGroups list of {@link RowGroupMetadata} to be merged
   * @param tableName name of the table
   * @param parquetTableMetadata the source of column metadata for non-interesting column's statistics
   * @return {@link FileMetadata} instance
   */
  public static FileMetadata getFileMetadata(List<RowGroupMetadata> rowGroups, String tableName,
      MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    if (rowGroups.isEmpty()) {
      return null;
    }
    Map<StatisticsKind, Object> fileStatistics = new HashMap<>();
    fileStatistics.put(TableStatisticsKind.ROW_COUNT, TableStatisticsKind.ROW_COUNT.mergeStatistics(rowGroups));

    TupleMetadata schema = rowGroups.iterator().next().getSchema();

    return new FileMetadata(rowGroups.iterator().next().getLocation(), schema,
      mergeColumnsStatistics(rowGroups, rowGroups.iterator().next().getColumnsStatistics().keySet(), PARQUET_STATISTICS, parquetTableMetadata),
      fileStatistics, tableName, -1);
  }

  /**
   * Returns {@link PartitionMetadata} instance received by merging specified {@link FileMetadata} list.
   *
   * @param partitionColumn partition column
   * @param files           list of files to be merged
   * @param tableName       name of the table
   * @return {@link PartitionMetadata} instance
   */
  public static PartitionMetadata getPartitionMetadata(SchemaPath partitionColumn, List<FileMetadata> files, String tableName) {
    Set<Path> locations = new HashSet<>();
    Set<SchemaPath> columns = new HashSet<>();

    for (FileMetadata file : files) {
      columns.addAll(file.getColumnsStatistics().keySet());
      locations.add(file.getLocation());
    }

    Map<StatisticsKind, Object> partStatistics = new HashMap<>();
    partStatistics.put(TableStatisticsKind.ROW_COUNT, TableStatisticsKind.ROW_COUNT.mergeStatistics(files));

    return new PartitionMetadata(partitionColumn, files.iterator().next().getSchema(),
        mergeColumnsStatistics(files, columns, PARQUET_STATISTICS, null), partStatistics, locations, tableName, -1);
  }

  /**
   * Returns "natural order" comparator which threads nulls as min values.
   *
   * @param <T> type to compare
   * @return "natural order" comparator
   */
  public static <T extends Comparable<T>> Comparator<T> getNaturalNullsFirstComparator() {
    return Comparator.nullsFirst(Comparator.naturalOrder());
  }

  /**
   * Converts specified {@link MetadataBase.RowGroupMetadata} into the map of {@link ColumnStatistics}
   * instances with column names as keys.
   *
   * @param tableMetadata    the source of column types
   * @param rowGroupMetadata metadata to convert
   * @return map with converted row group metadata
   */
  @SuppressWarnings("unchecked")
  public static Map<SchemaPath, ColumnStatistics> getRowGroupColumnStatistics(
      MetadataBase.ParquetTableMetadataBase tableMetadata, MetadataBase.RowGroupMetadata rowGroupMetadata) {

    Map<SchemaPath, ColumnStatistics> columnsStatistics = new HashMap<>();

    for (MetadataBase.ColumnMetadata column : rowGroupMetadata.getColumns()) {
      SchemaPath colPath = SchemaPath.getCompoundPath(column.getName());

      Long nulls = column.getNulls();
      if (!column.isNumNullsSet() || nulls == null) {
        nulls = GroupScan.NO_COLUMN_STATS;
      }
      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(tableMetadata, column);
      OriginalType originalType = getOriginalType(tableMetadata, column);
      Comparator comparator = getComparator(primitiveType, originalType);

      Map<StatisticsKind, Object> statistics = new HashMap<>();
      statistics.put(ColumnStatisticsKind.MIN_VALUE, getValue(column.getMinValue(), primitiveType, originalType));
      statistics.put(ColumnStatisticsKind.MAX_VALUE, getValue(column.getMaxValue(), primitiveType, originalType));
      statistics.put(ColumnStatisticsKind.NULLS_COUNT, nulls);
      columnsStatistics.put(colPath, new ColumnStatisticsImpl(statistics, comparator));
    }
    columnsStatistics.putAll(populateNonInterestingColumnsStats(columnsStatistics.keySet(), tableMetadata));
    return columnsStatistics;
  }

  /**
   * Populates the non-interesting column's statistics
   * @param schemaPaths columns paths which should be ignored
   * @param parquetTableMetadata the source of column metadata for non-interesting column's statistics
   * @return returns non-interesting column statistics map
   */
  @SuppressWarnings("unchecked")
  public static Map<SchemaPath, ColumnStatistics> populateNonInterestingColumnsStats(
          Set<SchemaPath> schemaPaths, MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    Map<SchemaPath, ColumnStatistics> columnsStatistics = new HashMap<>();
    if (parquetTableMetadata instanceof Metadata_V4.ParquetTableMetadata_v4) {
      ConcurrentHashMap<Metadata_V4.ColumnTypeMetadata_v4.Key, Metadata_V4.ColumnTypeMetadata_v4 > columnTypeInfoMap =
        ((Metadata_V4.ParquetTableMetadata_v4) parquetTableMetadata).getColumnTypeInfoMap();
      if ( columnTypeInfoMap == null ) { return columnsStatistics; } // in some cases for runtime pruning
      for (Metadata_V4.ColumnTypeMetadata_v4 columnTypeMetadata : columnTypeInfoMap.values()) {
        SchemaPath schemaPath = SchemaPath.getCompoundPath(columnTypeMetadata.name);
        if (!schemaPaths.contains(schemaPath)) {
          Map<StatisticsKind, Object> statistics = new HashMap<>();
          statistics.put(ColumnStatisticsKind.NULLS_COUNT, GroupScan.NO_COLUMN_STATS);
          PrimitiveType.PrimitiveTypeName primitiveType = columnTypeMetadata.primitiveType;
          OriginalType originalType = columnTypeMetadata.originalType;
          Comparator comparator = getComparator(primitiveType, originalType);
          columnsStatistics.put(schemaPath, new ColumnStatisticsImpl<>(statistics, comparator));
        }
      }
    }
    return columnsStatistics;
  }

  /**
   * Handles passed value considering its type and specified {@code primitiveType} with {@code originalType}.
   *
   * @param value         value to handle
   * @param primitiveType primitive type of the column whose value should be handled
   * @param originalType  original type of the column whose value should be handled
   * @return handled value
   */
  public static Object getValue(Object value, PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (value != null) {
      switch (primitiveType) {
        case BOOLEAN:
          return Boolean.parseBoolean(value.toString());

        case INT32:
          if (originalType == OriginalType.DATE) {
            return convertToDrillDateValue(getInt(value));
          } else if (originalType == OriginalType.DECIMAL) {
            return BigInteger.valueOf(getInt(value));
          }
          return getInt(value);

        case INT64:
          if (originalType == OriginalType.DECIMAL) {
            return BigInteger.valueOf(getLong(value));
          } else {
            return getLong(value);
          }

        case FLOAT:
          return getFloat(value);

        case DOUBLE:
          return getDouble(value);

        case INT96:
          return new String(getBytes(value));

        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          if (originalType == OriginalType.DECIMAL) {
            return new BigInteger(getBytes(value));
          } else if (originalType == OriginalType.INTERVAL) {
            return getBytes(value);
          } else {
            return new String(getBytes(value));
          }
      }
    }
    return null;
  }

  private static byte[] getBytes(Object value) {
    if (value instanceof Binary) {
      return ((Binary) value).getBytes();
    } else if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) { // value is obtained from metadata cache v2+
      return ((String) value).getBytes();
    } else if (value instanceof Map) { // value is obtained from metadata cache v1
      String bytesString = (String) ((Map) value).get("bytes");
      if (bytesString != null) {
        return bytesString.getBytes();
      }
    } else if (value instanceof Long) {
      return Longs.toByteArray((Long) value);
    } else if (value instanceof Integer) {
      return Longs.toByteArray((Integer) value);
    } else if (value instanceof Float) {
      return BigDecimal.valueOf((Float) value).unscaledValue().toByteArray();
    } else if (value instanceof Double) {
      return BigDecimal.valueOf((Double) value).unscaledValue().toByteArray();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain bytes using value %s", value));
  }

  private static Integer getInt(Object value) {
    if (value instanceof Integer) {
      return (Integer) value;
    } else if (value instanceof Long) {
      return ((Long) value).intValue();
    } else if (value instanceof Float) {
      return ((Float) value).intValue();
    } else if (value instanceof Double) {
      return ((Double) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).intValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).intValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Long getLong(Object value) {
    if (value instanceof Integer) {
      return Long.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Float) {
      return ((Float) value).longValue();
    } else if (value instanceof Double) {
      return ((Double) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong(value.toString());
    } else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).longValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).longValue();
    }
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Float getFloat(Object value) {
    if (value instanceof Integer) {
      return Float.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Float.valueOf((Long) value);
    } else if (value instanceof Float) {
      return (Float) value;
    } else if (value instanceof Double) {
      return ((Double) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).floatValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).floatValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static Double getDouble(Object value) {
    if (value instanceof Integer) {
      return Double.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return Double.valueOf((Long) value);
    } else if (value instanceof Float) {
      return Double.valueOf((Float) value);
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof String) {
      return Double.parseDouble(value.toString());
    }
    // TODO: allow conversion form bytes only when actual type of data is known (to obtain scale)
    /* else if (value instanceof byte[]) {
      return new BigInteger((byte[]) value).doubleValue();
    } else if (value instanceof Binary) {
      return new BigInteger(((Binary) value).getBytes()).doubleValue();
    }*/
    throw new UnsupportedOperationException(String.format("Cannot obtain Integer using value %s", value));
  }

  private static long convertToDrillDateValue(int dateValue) {
    return dateValue * (long) DateTimeConstants.MILLIS_PER_DAY;
  }

  /**
   * Returns {@link Comparator} instance considering specified {@code primitiveType} and {@code originalType}.
   *
   * @param primitiveType primitive type of the column
   * @param originalType  original type og the column
   * @return {@link Comparator} instance
   */
  public static Comparator getComparator(PrimitiveType.PrimitiveTypeName primitiveType, OriginalType originalType) {
    if (originalType != null) {
      switch (originalType) {
        case UINT_8:
        case UINT_16:
        case UINT_32:
          return getNaturalNullsFirstComparator();
        case UINT_64:
          return getNaturalNullsFirstComparator();
        case DATE:
        case INT_8:
        case INT_16:
        case INT_32:
        case INT_64:
        case TIME_MICROS:
        case TIME_MILLIS:
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
        case DECIMAL:
        case UTF8:
          return getNaturalNullsFirstComparator();
        case INTERVAL:
          return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
        default:
          return getNaturalNullsFirstComparator();
      }
    } else {
      switch (primitiveType) {
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case BINARY:
        case INT96:
        case FIXED_LEN_BYTE_ARRAY:
          return getNaturalNullsFirstComparator();
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitiveType);
      }
    }
  }

  /**
   * Returns {@link Comparator} instance considering specified {@code type}.
   *
   * @param type type of the column
   * @return {@link Comparator} instance
   */
  public static Comparator getComparator(TypeProtos.MinorType type) {
    switch (type) {
      case INTERVALDAY:
      case INTERVAL:
      case INTERVALYEAR:
        return UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;
      case UINT1:
        return Comparator.nullsFirst(UnsignedBytes::compare);
      case UINT2:
      case UINT4:
        return Comparator.nullsFirst(Integer::compareUnsigned);
      case UINT8:
        return Comparator.nullsFirst(Long::compareUnsigned);
      default:
        return getNaturalNullsFirstComparator();
    }
  }

  /**
   * Returns map of column names with their drill types for specified {@code file}.
   *
   * @param parquetTableMetadata the source of primitive and original column types
   * @param file                 file whose columns should be discovered
   * @return map of column names with their drill types
   */
  public static Map<SchemaPath, TypeProtos.MajorType> getFileFields(
    MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ParquetFileMetadata file) {

    // does not resolve types considering all row groups, just takes type from the first row group.
    return getRowGroupFields(parquetTableMetadata, file.getRowGroups().iterator().next());
  }

  /**
   * Returns map of column names with their drill types for specified {@code rowGroup}.
   *
   * @param parquetTableMetadata the source of primitive and original column types
   * @param rowGroup             row group whose columns should be discovered
   * @return map of column names with their drill types
   */
  public static Map<SchemaPath, TypeProtos.MajorType> getRowGroupFields(
      MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.RowGroupMetadata rowGroup) {
    Map<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ColumnMetadata column : rowGroup.getColumns()) {

      PrimitiveType.PrimitiveTypeName primitiveType = getPrimitiveTypeName(parquetTableMetadata, column);
      OriginalType originalType = getOriginalType(parquetTableMetadata, column);
      int precision = 0;
      int scale = 0;
      int definitionLevel = 1;
      int repetitionLevel = 0;
      MetadataVersion metadataVersion = new MetadataVersion(parquetTableMetadata.getMetadataVersion());
      // only ColumnTypeMetadata_v3 and ColumnTypeMetadata_v4 store information about scale, precision, repetition level and definition level
      if (parquetTableMetadata.hasColumnMetadata() && (metadataVersion.compareTo(new MetadataVersion(3, 0)) >= 0)) {
        scale = parquetTableMetadata.getScale(column.getName());
        precision = parquetTableMetadata.getPrecision(column.getName());
        repetitionLevel = parquetTableMetadata.getRepetitionLevel(column.getName());
        definitionLevel = parquetTableMetadata.getDefinitionLevel(column.getName());
      }
      TypeProtos.DataMode mode;
      if (repetitionLevel >= 1) {
        mode = TypeProtos.DataMode.REPEATED;
      } else if (repetitionLevel == 0 && definitionLevel == 0) {
        mode = TypeProtos.DataMode.REQUIRED;
      } else {
        mode = TypeProtos.DataMode.OPTIONAL;
      }
      TypeProtos.MajorType columnType =
          TypeProtos.MajorType.newBuilder(ParquetReaderUtility.getType(primitiveType, originalType, scale, precision))
              .setMode(mode)
              .build();

      SchemaPath columnPath = SchemaPath.getCompoundPath(column.getName());
      TypeProtos.MajorType majorType = columns.get(columnPath);
      if (majorType == null) {
        columns.put(columnPath, columnType);
      } else {
        TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), columnType.getMinorType()));
        if (leastRestrictiveType != majorType.getMinorType()) {
          columns.put(columnPath, columnType);
        }
      }
    }
    return columns;
  }

  /**
   * Returns {@link OriginalType} type for the specified column.
   *
   * @param parquetTableMetadata the source of column type
   * @param column               column whose {@link OriginalType} should be returned
   * @return {@link OriginalType} type for the specified column
   */
  public static OriginalType getOriginalType(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    OriginalType originalType = column.getOriginalType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (originalType == null) {
      originalType = parquetTableMetadata.getOriginalType(column.getName());
    }
    return originalType;
  }

  /**
   * Returns {@link PrimitiveType.PrimitiveTypeName} type for the specified column.
   *
   * @param parquetTableMetadata the source of column type
   * @param column               column whose {@link PrimitiveType.PrimitiveTypeName} should be returned
   * @return {@link PrimitiveType.PrimitiveTypeName} type for the specified column
   */
  public static PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(MetadataBase.ParquetTableMetadataBase parquetTableMetadata, MetadataBase.ColumnMetadata column) {
    PrimitiveType.PrimitiveTypeName primitiveType = column.getPrimitiveType();
    // for the case of parquet metadata v1 version, type information isn't stored in parquetTableMetadata, but in ColumnMetadata
    if (primitiveType == null) {
      primitiveType = parquetTableMetadata.getPrimitiveType(column.getName());
    }
    return primitiveType;
  }

  /**
   * Returns map of column names with their drill types for specified {@code parquetTableMetadata}
   * with resolved types for the case of schema evolution.
   *
   * @param parquetTableMetadata table metadata whose columns should be discovered
   * @return map of column names with their drill types
   */
  static Map<SchemaPath, TypeProtos.MajorType> resolveFields(MetadataBase.ParquetTableMetadataBase parquetTableMetadata) {
    LinkedHashMap<SchemaPath, TypeProtos.MajorType> columns = new LinkedHashMap<>();
    for (MetadataBase.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      // row groups in the file have the same schema, so using the first one
      Map<SchemaPath, TypeProtos.MajorType> fileColumns = getFileFields(parquetTableMetadata, file);
      fileColumns.forEach((columnPath, type) -> {
        TypeProtos.MajorType majorType = columns.get(columnPath);
        if (majorType == null) {
          columns.put(columnPath, type);
        } else {
          TypeProtos.MinorType leastRestrictiveType = TypeCastRules.getLeastRestrictiveType(Arrays.asList(majorType.getMinorType(), type.getMinorType()));
          if (leastRestrictiveType != majorType.getMinorType()) {
            columns.put(columnPath, type);
          }
        }
      });
    }
    return columns;
  }

  /**
   * Updates row cont and column nulls count for specified table metadata and returns new {@link TableMetadata} instance with updated statistics.
   *
   * @param tableMetadata table statistics to update
   * @param statistics    list of statistics whose row count should be considered
   * @return new {@link TableMetadata} instance with updated statistics
   */
  public static TableMetadata updateRowCount(TableMetadata tableMetadata, Collection<? extends BaseMetadata> statistics) {
    Map<StatisticsKind, Object> newStats = new HashMap<>();

    newStats.put(TableStatisticsKind.ROW_COUNT, TableStatisticsKind.ROW_COUNT.mergeStatistics(statistics));

    Map<SchemaPath, ColumnStatistics> columnsStatistics =
        mergeColumnsStatistics(statistics, tableMetadata.getColumnsStatistics().keySet(),
            ImmutableList.of(ColumnStatisticsKind.NULLS_COUNT), null);

    return tableMetadata.cloneWithStats(columnsStatistics, newStats);
  }
}
