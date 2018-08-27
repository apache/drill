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
package org.apache.drill.exec.store.parquet.stat;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.store.parquet.columnreaders.ParquetToDrillTypeConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.joda.time.DateTimeConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ParquetFooterStatCollector implements ColumnStatCollector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFooterStatCollector.class);

  private final ParquetMetadata footer;
  private final int rowGroupIndex;
  private final OptionManager options;
  private final Map<String, String> implicitColValues;
  private final boolean autoCorrectCorruptDates;

  public ParquetFooterStatCollector(ParquetMetadata footer, int rowGroupIndex, Map<String, String> implicitColValues,
      boolean autoCorrectCorruptDates, OptionManager options) {
    this.footer = footer;
    this.rowGroupIndex = rowGroupIndex;

    // Reasons to pass implicit columns and their values:
    // 1. Differentiate implicit columns from regular non-exist columns. Implicit columns do not
    //    exist in parquet metadata. Without such knowledge, implicit columns is treated as non-exist
    //    column.  A condition on non-exist column would lead to matches = ALL, which is not the
    //    right behavior for condition on implicit columns.

    // 2. Pass in the implicit column name with corresponding values, and wrap them in Statistics with
    //    min and max having same value. This expands the possibility of pruning.
    //    For example, regCol = 5 or dir0 = 1995. If regCol is not a partition column, we would not do
    //    any partition pruning in the current partition pruning logical. Pass the implicit column values
    //    may allow us to prune some row groups using condition regCol = 5 or dir0 = 1995.

    this.implicitColValues = implicitColValues;
    this.autoCorrectCorruptDates = autoCorrectCorruptDates;
    this.options = options;
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> collectColStat(Set<SchemaPath> fields) {
    Stopwatch timer = Stopwatch.createStarted();

    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates =
        ParquetReaderUtility.detectCorruptDates(footer, new ArrayList<>(fields), autoCorrectCorruptDates);

    // map from column name to ColumnDescriptor
    Map<SchemaPath, ColumnDescriptor> columnDescMap = new HashMap<>();

    // map from column name to ColumnChunkMetaData
    final Map<SchemaPath, ColumnChunkMetaData> columnChkMetaMap = new HashMap<>();

    // map from column name to MajorType
    final Map<SchemaPath, TypeProtos.MajorType> columnTypeMap = new HashMap<>();

    // map from column name to SchemaElement
    final Map<SchemaPath, SchemaElement> schemaElementMap = new HashMap<>();

    // map from column name to column statistics.
    final Map<SchemaPath, ColumnStatistics> statMap = new HashMap<>();

    final org.apache.parquet.format.FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);

    for (final ColumnDescriptor column : footer.getFileMetaData().getSchema().getColumns()) {
      final SchemaPath schemaPath = SchemaPath.getCompoundPath(column.getPath());
      if (fields.contains(schemaPath)) {
        columnDescMap.put(schemaPath, column);
      }
    }

    for (final SchemaElement se : fileMetaData.getSchema()) {
      final SchemaPath schemaPath = SchemaPath.getSimplePath(se.getName());
      if (fields.contains(schemaPath)) {
        schemaElementMap.put(schemaPath, se);
      }
    }

    for (final ColumnChunkMetaData colMetaData: footer.getBlocks().get(rowGroupIndex).getColumns()) {
      final SchemaPath schemaPath = SchemaPath.getCompoundPath(colMetaData.getPath().toArray());
      if (fields.contains(schemaPath)) {
        columnChkMetaMap.put(schemaPath, colMetaData);
      }
    }

    for (final SchemaPath path : fields) {
      if (columnDescMap.containsKey(path) && schemaElementMap.containsKey(path) && columnChkMetaMap.containsKey(path)) {
        ColumnDescriptor columnDesc =  columnDescMap.get(path);
        SchemaElement se = schemaElementMap.get(path);
        ColumnChunkMetaData metaData = columnChkMetaMap.get(path);

        TypeProtos.MajorType type = ParquetToDrillTypeConverter.toMajorType(columnDesc.getType(), se.getType_length(),
            getDataMode(columnDesc), se, options);

        columnTypeMap.put(path, type);

        Statistics stat = metaData.getStatistics();
        if (type.getMinorType() == TypeProtos.MinorType.DATE) {
          stat = convertDateStatIfNecessary(metaData.getStatistics(), containsCorruptDates);
        }

        statMap.put(path, new ColumnStatistics(stat, type));
      } else {
        final String columnName = path.getRootSegment().getPath();
        if (implicitColValues.containsKey(columnName)) {
          TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.VARCHAR);
          Statistics stat = new BinaryStatistics();
          stat.setNumNulls(0);
          byte[] val = implicitColValues.get(columnName).getBytes();
          stat.setMinMaxFromBytes(val, val);
          statMap.put(path, new ColumnStatistics(stat, type));
        }
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Took {} ms to column statistics for row group", timer.elapsed(TimeUnit.MILLISECONDS));
    }

    return statMap;
  }

  private static TypeProtos.DataMode getDataMode(ColumnDescriptor column) {
    if (column.getMaxRepetitionLevel() > 0 ) {
      return TypeProtos.DataMode.REPEATED;
    } else if (column.getMaxDefinitionLevel() == 0) {
      return TypeProtos.DataMode.REQUIRED;
    } else {
      return TypeProtos.DataMode.OPTIONAL;
    }
  }

  public static Statistics convertDateStatIfNecessary(Statistics stat,
      ParquetReaderUtility.DateCorruptionStatus containsCorruptDates) {
    IntStatistics dateStat = (IntStatistics) stat;
    LongStatistics dateMLS = new LongStatistics();

    boolean isDateCorrect = containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;

    // Only do conversion when stat is NOT empty.
    if (!dateStat.isEmpty()) {
        dateMLS.setMinMax(
            convertToDrillDateValue(dateStat.getMin(), isDateCorrect),
            convertToDrillDateValue(dateStat.getMax(), isDateCorrect));
        dateMLS.setNumNulls(dateStat.getNumNulls());
    }

    return dateMLS;

  }

  private static long convertToDrillDateValue(int dateValue, boolean isDateCorrect) {
    // See DRILL-4203 for the background regarding date type corruption issue in Drill CTAS prior to 1.9.0 release.
    if (isDateCorrect) {
      return dateValue * (long) DateTimeConstants.MILLIS_PER_DAY;
    } else {
      return (dateValue - ParquetReaderUtility.CORRECT_CORRUPT_DATE_SHIFT) * DateTimeConstants.MILLIS_PER_DAY;
    }
  }

}
