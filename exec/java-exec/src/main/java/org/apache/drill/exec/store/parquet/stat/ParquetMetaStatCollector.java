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

import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeConstants;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ColumnTypeMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetTableMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ColumnMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetTableMetadataBase;

public class ParquetMetaStatCollector implements  ColumnStatCollector {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetMetaStatCollector.class);

  private final ParquetTableMetadataBase parquetTableMetadata;
  private final List<? extends ColumnMetadata> columnMetadataList;
  private final Map<String, String> implicitColValues;
  public ParquetMetaStatCollector(ParquetTableMetadataBase parquetTableMetadata,
      List<? extends ColumnMetadata> columnMetadataList, Map<String, String> implicitColValues) {
    this.parquetTableMetadata = parquetTableMetadata;
    this.columnMetadataList = columnMetadataList;

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
  }

  @Override
  public Map<SchemaPath, ColumnStatistics> collectColStat(Set<SchemaPath> fields) {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;

    // map from column to ColumnMetadata
    final Map<SchemaPath, ColumnMetadata> columnMetadataMap = new HashMap<>();

    // map from column name to column statistics.
    final Map<SchemaPath, ColumnStatistics> statMap = new HashMap<>();

    for (final ColumnMetadata columnMetadata : columnMetadataList) {
      SchemaPath schemaPath = SchemaPath.getCompoundPath(columnMetadata.getName());
      columnMetadataMap.put(schemaPath, columnMetadata);
    }

    for (SchemaPath field : fields) {
      ColumnMetadata columnMetadata = columnMetadataMap.get(field.getUnIndexed());
      if (columnMetadata != null) {
        ColumnStatisticsBuilder statisticsBuilder = ColumnStatisticsBuilder.builder()
          .setMin(columnMetadata.getMinValue())
          .setMax(columnMetadata.getMaxValue())
          .setNumNulls(columnMetadata.getNulls() == null ? GroupScan.NO_COLUMN_STATS: columnMetadata.getNulls())
          .setPrimitiveType(parquetTableMetadata.getPrimitiveType(columnMetadata.getName()))
          .setOriginalType(parquetTableMetadata.getOriginalType(columnMetadata.getName()));

        // ColumnTypeMetadata_v3 stores information about scale and precision
        if (parquetTableMetadata instanceof ParquetTableMetadata_v3) {
          ColumnTypeMetadata_v3 columnTypeInfo = ((ParquetTableMetadata_v3) parquetTableMetadata)
                                                                          .getColumnTypeInfo(columnMetadata.getName());
          statisticsBuilder.setScale(columnTypeInfo.scale);
          statisticsBuilder.setPrecision(columnTypeInfo.precision);
        }

        statMap.put(field, statisticsBuilder.build());

      } else {
        final String columnName = field.getRootSegment().getPath();
        if (implicitColValues.containsKey(columnName)) {
          TypeProtos.MajorType type = Types.required(TypeProtos.MinorType.VARCHAR);
          Statistics stat = new BinaryStatistics();
          stat.setNumNulls(0);
          byte[] val = implicitColValues.get(columnName).getBytes();
          stat.setMinMaxFromBytes(val, val);
          statMap.put(field, new ColumnStatistics(stat, type));
        }
      }
    }

    if (timer != null) {
      logger.debug("Took {} ms to column statistics for row group", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
    }

    return statMap;
  }

  /**
   * Helper class that creates parquet {@link ColumnStatistics} based on given
   * min and max values, type, number of nulls, precision and scale.
   *
   */
  private static class ColumnStatisticsBuilder {

    private Object min;
    private Object max;
    private long numNulls;
    private PrimitiveType.PrimitiveTypeName primitiveType;
    private OriginalType originalType;
    private int scale;
    private int precision;

    static ColumnStatisticsBuilder builder() {
      return new ColumnStatisticsBuilder();
    }

    ColumnStatisticsBuilder setMin(Object min) {
      this.min = min;
      return this;
    }

    ColumnStatisticsBuilder setMax(Object max) {
      this.max = max;
      return this;
    }

    ColumnStatisticsBuilder setNumNulls(long numNulls) {
      this.numNulls = numNulls;
      return this;
    }

    ColumnStatisticsBuilder setPrimitiveType(PrimitiveType.PrimitiveTypeName primitiveType) {
      this.primitiveType = primitiveType;
      return this;
    }

    ColumnStatisticsBuilder setOriginalType(OriginalType originalType) {
      this.originalType = originalType;
      return this;
    }

    ColumnStatisticsBuilder setScale(int scale) {
      this.scale = scale;
      return this;
    }

    ColumnStatisticsBuilder setPrecision(int precision) {
      this.precision = precision;
      return this;
    }


    /**
     * Builds column statistics using given primitive and original types,
     * scale, precision, number of nulls, min and max values.
     * Min and max values for binary statistics are set only if allowed.
     *
     * @return column statistics
     */
    ColumnStatistics build() {
      Statistics stat = Statistics.getStatsBasedOnType(primitiveType);
      Statistics convertedStat = stat;

      TypeProtos.MajorType type = ParquetReaderUtility.getType(primitiveType, originalType, scale, precision);
      stat.setNumNulls(numNulls);

      if (min != null && max != null) {
        switch (type.getMinorType()) {
          case INT :
          case TIME:
            ((IntStatistics) stat).setMinMax(Integer.parseInt(min.toString()), Integer.parseInt(max.toString()));
            break;
          case BIGINT:
          case TIMESTAMP:
            ((LongStatistics) stat).setMinMax(Long.parseLong(min.toString()), Long.parseLong(max.toString()));
            break;
          case FLOAT4:
            ((FloatStatistics) stat).setMinMax(Float.parseFloat(min.toString()), Float.parseFloat(max.toString()));
            break;
          case FLOAT8:
            ((DoubleStatistics) stat).setMinMax(Double.parseDouble(min.toString()), Double.parseDouble(max.toString()));
            break;
          case DATE:
            convertedStat = new LongStatistics();
            convertedStat.setNumNulls(stat.getNumNulls());
            long minMS = convertToDrillDateValue(Integer.parseInt(min.toString()));
            long maxMS = convertToDrillDateValue(Integer.parseInt(max.toString()));
            ((LongStatistics) convertedStat ).setMinMax(minMS, maxMS);
            break;
          case BIT:
            ((BooleanStatistics) stat).setMinMax(Boolean.parseBoolean(min.toString()), Boolean.parseBoolean(max.toString()));
            break;
          case VARCHAR:
            if (min instanceof Binary && max instanceof Binary) { // when read directly from parquet footer
              ((BinaryStatistics) stat).setMinMaxFromBytes(((Binary) min).getBytes(), ((Binary) max).getBytes());
            } else if (min instanceof byte[] && max instanceof byte[]) { // when deserialized from Drill metadata file
              ((BinaryStatistics) stat).setMinMaxFromBytes((byte[]) min, (byte[]) max);
            } else {
              logger.trace("Unexpected class for Varchar statistics for min / max values. Min: {}. Max: {}.",
                min.getClass(), max.getClass());
            }
            break;
          case VARDECIMAL:
            byte[] minBytes = null;
            byte[] maxBytes = null;
            boolean setLength = false;

            switch (primitiveType) {
              case INT32:
              case INT64:
                minBytes = new BigInteger(min.toString()).toByteArray();
                maxBytes = new BigInteger(max.toString()).toByteArray();
                break;
              case FIXED_LEN_BYTE_ARRAY:
                setLength = true;
                // fall through
              case BINARY:
                // wrap up into BigInteger to avoid PARQUET-1417
                if (min instanceof Binary && max instanceof Binary) { // when read directly from parquet footer
                  minBytes = new BigInteger(((Binary) min).getBytes()).toByteArray();
                  maxBytes = new BigInteger(((Binary) max).getBytes()).toByteArray();
                } else if (min instanceof byte[] && max instanceof byte[]) {  // when deserialized from Drill metadata file
                  minBytes = new BigInteger((byte[]) min).toByteArray();
                  maxBytes = new BigInteger((byte[]) max).toByteArray();
                } else {
                  logger.trace("Unexpected class for Binary Decimal statistics for min / max values. Min: {}. Max: {}.",
                    min.getClass(), max.getClass());
                }
                break;
              default:
                logger.trace("Unexpected primitive type [{}] for Decimal statistics.", primitiveType);
            }

            if (minBytes == null || maxBytes == null) {
              break;
            }

            int length = setLength ? maxBytes.length : 0;

            PrimitiveType decimalType = org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
              .as(OriginalType.DECIMAL)
              .length(length)
              .precision(precision)
              .scale(scale)
              .named("decimal_type");

            convertedStat = Statistics.getBuilderForReading(decimalType)
              .withMin(minBytes)
              .withMax(maxBytes)
              .withNumNulls(numNulls)
              .build();
            break;

          default:
            logger.trace("Unsupported minor type [{}] for parquet statistics.", type.getMinorType());
        }
      }
      return new ColumnStatistics(convertedStat, type);
    }

    private long convertToDrillDateValue(int dateValue) {
      return dateValue * (long) DateTimeConstants.MILLIS_PER_DAY;
    }

  }

}
