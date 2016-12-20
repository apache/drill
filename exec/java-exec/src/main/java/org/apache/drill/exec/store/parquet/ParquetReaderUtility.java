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

import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.work.ExecErrorConstants;
import org.apache.parquet.SemanticVersion;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.Chronology;
import org.joda.time.DateTimeConstants;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class where we can capture common logic between the two parquet readers
 */
public class ParquetReaderUtility {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetReaderUtility.class);

  /**
   * Number of days between Julian day epoch (January 1, 4713 BC) and Unix day epoch (January 1, 1970).
   * The value of this constant is {@value}.
   */
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
  /**
   * All old parquet files (which haven't "is.date.correct=true" or "parquet-writer.version" properties
   * in metadata) have a corrupt date shift: {@value} days or 2 * {@value #JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH}
   */
  public static final long CORRECT_CORRUPT_DATE_SHIFT = 2 * JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH;
  private static final Chronology UTC = org.joda.time.chrono.ISOChronology.getInstanceUTC();
  /**
   * The year 5000 (or 1106685 day from Unix epoch) is chosen as the threshold for auto-detecting date corruption.
   * This balances two possible cases of bad auto-correction. External tools writing dates in the future will not
   * be shifted unless they are past this threshold (and we cannot identify them as external files based on the metadata).
   * On the other hand, historical dates written with Drill wouldn't risk being incorrectly shifted unless they were
   * something like 10,000 years in the past.
   */
  public static final int DATE_CORRUPTION_THRESHOLD =
      (int) (UTC.getDateTimeMillis(5000, 1, 1, 0) / DateTimeConstants.MILLIS_PER_DAY);
  /**
   * Version 2 (and later) of the Drill Parquet writer uses the date format described in the
   * <a href="https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#date">Parquet spec</a>.
   * Prior versions had dates formatted with {@link org.apache.drill.exec.store.parquet.ParquetReaderUtility#CORRECT_CORRUPT_DATE_SHIFT}
   */
  public static final int DRILL_WRITER_VERSION_STD_DATE_FORMAT = 2;
  /**
   * For most recently created parquet files, we can determine if we have corrupted dates (see DRILL-4203)
   * based on the file metadata. For older files that lack statistics we must actually test the values
   * in the data pages themselves to see if they are likely corrupt.
   */
  public enum DateCorruptionStatus {
    META_SHOWS_CORRUPTION {
      @Override
      public String toString() {
        return "It is determined from metadata that the date values are definitely CORRUPT";
      }
    },
    META_SHOWS_NO_CORRUPTION {
      @Override
      public String toString() {
        return "It is determined from metadata that the date values are definitely CORRECT";
      }
    },
    META_UNCLEAR_TEST_VALUES {
      @Override
      public String toString() {
        return "Not enough info in metadata, parquet reader will test individual date values";
      }
    }
  }

  public static void checkDecimalTypeEnabled(OptionManager options) {
    if (! options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE)) {
      throw UserException.unsupportedError()
        .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
        .build(logger);
    }
  }

  public static int getIntFromLEBytes(byte[] input, int start) {
    int out = 0;
    int shiftOrder = 0;
    for (int i = start; i < start + 4; i++) {
      out |= (((input[i]) & 0xFF) << shiftOrder);
      shiftOrder += 8;
    }
    return out;
  }

  public static Map<String, SchemaElement> getColNameToSchemaElementMapping(ParquetMetadata footer) {
    HashMap<String, SchemaElement> schemaElements = new HashMap<>();
    FileMetaData fileMetaData = new ParquetMetadataConverter().toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
    for (SchemaElement se : fileMetaData.getSchema()) {
      schemaElements.put(se.getName(), se);
    }
    return schemaElements;
  }

  public static int autoCorrectCorruptedDate(int corruptedDate) {
    return (int) (corruptedDate - CORRECT_CORRUPT_DATE_SHIFT);
  }

  public static void correctDatesInMetadataCache(Metadata.ParquetTableMetadataBase parquetTableMetadata) {
    DateCorruptionStatus cacheFileCanContainsCorruptDates =
        new MetadataVersion(parquetTableMetadata.getMetadataVersion()).compareTo(new MetadataVersion(3, 0)) >= 0 ?
        DateCorruptionStatus.META_SHOWS_NO_CORRUPTION : DateCorruptionStatus.META_UNCLEAR_TEST_VALUES;
    if (cacheFileCanContainsCorruptDates == DateCorruptionStatus.META_UNCLEAR_TEST_VALUES) {
      // Looking for the DATE data type of column names in the metadata cache file ("metadata_version" : "v2")
      String[] names = new String[0];
      if (new MetadataVersion(2, 0).equals(new MetadataVersion(parquetTableMetadata.getMetadataVersion()))) {
        for (Metadata.ColumnTypeMetadata_v2 columnTypeMetadata :
            ((Metadata.ParquetTableMetadata_v2) parquetTableMetadata).columnTypeInfo.values()) {
          if (OriginalType.DATE.equals(columnTypeMetadata.originalType)) {
            names = columnTypeMetadata.name;
          }
        }
      }
      for (Metadata.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
        // Drill has only ever written a single row group per file, only need to correct the statistics
        // on the first row group
        Metadata.RowGroupMetadata rowGroupMetadata = file.getRowGroups().get(0);
        Long rowCount = rowGroupMetadata.getRowCount();
        for (Metadata.ColumnMetadata columnMetadata : rowGroupMetadata.getColumns()) {
          // Setting Min/Max values for ParquetTableMetadata_v1
          if (new MetadataVersion(1, 0).equals(new MetadataVersion(parquetTableMetadata.getMetadataVersion()))) {
            OriginalType originalType = columnMetadata.getOriginalType();
            if (OriginalType.DATE.equals(originalType) && columnMetadata.hasSingleValue(rowCount) &&
                (Integer) columnMetadata.getMaxValue() > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
              int newMinMax = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) columnMetadata.getMaxValue());
              columnMetadata.setMax(newMinMax);
              columnMetadata.setMin(newMinMax);
            }
          }
          // Setting Max values for ParquetTableMetadata_v2
          else if (new MetadataVersion(2, 0).equals(new MetadataVersion(parquetTableMetadata.getMetadataVersion())) &&
              columnMetadata.getName() != null && Arrays.equals(columnMetadata.getName(), names) &&
              columnMetadata.hasSingleValue(rowCount) && (Integer) columnMetadata.getMaxValue() >
              ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
            int newMax = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) columnMetadata.getMaxValue());
            columnMetadata.setMax(newMax);
          }
        }
      }
    }
  }

  /**
   * Checks assigns byte arrays to min/max values obtained from the deserialized string
   * for BINARY.
   *
   * @param parquetTableMetadata table metadata that should be corrected
   */
  public static void correctBinaryInMetadataCache(Metadata.ParquetTableMetadataBase parquetTableMetadata) {
    // Looking for the names of the columns with BINARY data type
    // in the metadata cache file for V2 and all v3 versions
    Set<List<String>> columnsNames = getBinaryColumnsNames(parquetTableMetadata);

    for (Metadata.ParquetFileMetadata file : parquetTableMetadata.getFiles()) {
      for (Metadata.RowGroupMetadata rowGroupMetadata : file.getRowGroups()) {
        Long rowCount = rowGroupMetadata.getRowCount();
        for (Metadata.ColumnMetadata columnMetadata : rowGroupMetadata.getColumns()) {
          // Setting Min/Max values for ParquetTableMetadata_v1
          if (new MetadataVersion(1, 0).equals(new MetadataVersion(parquetTableMetadata.getMetadataVersion()))) {
            if (columnMetadata.getPrimitiveType() == PrimitiveTypeName.BINARY
                || columnMetadata.getPrimitiveType() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
              setMinMaxValues(columnMetadata, rowCount);
            }
          }
          // Setting Min/Max values for V2 and all V3 versions before V3_3
          else if (new MetadataVersion(parquetTableMetadata.getMetadataVersion()).compareTo(new MetadataVersion(3, 3)) < 0
                    && columnsNames.contains(Arrays.asList(columnMetadata.getName()))) {
            setMinMaxValues(columnMetadata, rowCount);
          }
          // Setting Min/Max values for V3_3 and all younger versions
          else if (new MetadataVersion(parquetTableMetadata.getMetadataVersion()).compareTo(new MetadataVersion(3, 3)) >= 0
                      && columnsNames.contains(Arrays.asList(columnMetadata.getName()))) {
            convertMinMaxValues(columnMetadata, rowCount);
          }
        }
      }
    }
  }

  /**
   * Returns the set of the lists with names of the columns with BINARY or
   * FIXED_LEN_BYTE_ARRAY data type from {@code ParquetTableMetadataBase columnTypeMetadataCollection}
   * if parquetTableMetadata has version v2 or v3 (including minor versions).
   *
   * @param parquetTableMetadata table metadata the source of the columns to check
   * @return set of the lists with column names
   */
  private static Set<List<String>> getBinaryColumnsNames(Metadata.ParquetTableMetadataBase parquetTableMetadata) {
    Set<List<String>> names = Sets.newHashSet();
    if (parquetTableMetadata instanceof Metadata.ParquetTableMetadata_v2) {
      for (Metadata.ColumnTypeMetadata_v2 columnTypeMetadata :
        ((Metadata.ParquetTableMetadata_v2) parquetTableMetadata).columnTypeInfo.values()) {
        if (columnTypeMetadata.primitiveType == PrimitiveTypeName.BINARY
            || columnTypeMetadata.primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
          names.add(Arrays.asList(columnTypeMetadata.name));
        }
      }
    } else if (parquetTableMetadata instanceof Metadata.ParquetTableMetadata_v3) {
      for (Metadata.ColumnTypeMetadata_v3 columnTypeMetadata :
        ((Metadata.ParquetTableMetadata_v3) parquetTableMetadata).columnTypeInfo.values()) {
        if (columnTypeMetadata.primitiveType == PrimitiveTypeName.BINARY
            || columnTypeMetadata.primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
          names.add(Arrays.asList(columnTypeMetadata.name));
        }
      }
    }
    return names;
  }

  /**
   * Checks that column has single value and replaces Min and Max by their byte values
   * in {@code Metadata.ColumnMetadata columnMetadata} if their values were stored as strings.
   *
   * @param columnMetadata column metadata that should be changed
   * @param rowCount       rows count in column chunk
   */
  private static void setMinMaxValues(Metadata.ColumnMetadata columnMetadata, long rowCount) {
    if (columnMetadata.hasSingleValue(rowCount)) {
      Object minValue = columnMetadata.getMinValue();
      if (minValue != null && minValue instanceof String) {
        byte[] bytes = ((String) minValue).getBytes();
        columnMetadata.setMax(bytes);
        columnMetadata.setMin(bytes);
      }
    }
  }

  /**
   * Checks that column has single value and replaces Min and Max by their byte values from Base64 data
   * in Metadata.ColumnMetadata columnMetadata if their values were stored as strings.
   *
   * @param columnMetadata column metadata that should be changed
   * @param rowCount       rows count in column chunk
   */
  private static void convertMinMaxValues(Metadata.ColumnMetadata columnMetadata, long rowCount) {
    if (columnMetadata.hasSingleValue(rowCount)) {
      Object minValue = columnMetadata.getMinValue();
      if (minValue != null && minValue instanceof String) {
        byte[] bytes = Base64.decodeBase64(((String) minValue).getBytes());
        columnMetadata.setMax(bytes);
        columnMetadata.setMin(bytes);
      }
    }
  }

  /**
   * Check for corrupted dates in a parquet file. See Drill-4203
   */
  public static DateCorruptionStatus detectCorruptDates(ParquetMetadata footer,
                                           List<SchemaPath> columns,
                                           boolean autoCorrectCorruptDates) {
    // old drill files have "parquet-mr" as created by string, and no drill version, need to check min/max values to see
    // if they look corrupt
    //  - option to disable this auto-correction based on the date values, in case users are storing these
    //    dates intentionally

    // migrated parquet files have 1.8.1 parquet-mr version with drill-r0 in the part of the name usually containing "SNAPSHOT"

    // new parquet files are generated with "is.date.correct" property have no corruption dates

    String createdBy = footer.getFileMetaData().getCreatedBy();
    String drillVersion = footer.getFileMetaData().getKeyValueMetaData().get(ParquetRecordWriter.DRILL_VERSION_PROPERTY);
    String writerVersionValue = footer.getFileMetaData().getKeyValueMetaData().get(ParquetRecordWriter.WRITER_VERSION_PROPERTY);
    // This flag can be present in parquet files which were generated with 1.9.0-SNAPSHOT and 1.9.0 drill versions.
    // If this flag is present it means that the version of the drill parquet writer is 2
    final String isDateCorrectFlag = "is.date.correct";
    String isDateCorrect = footer.getFileMetaData().getKeyValueMetaData().get(isDateCorrectFlag);
    if (drillVersion != null) {
      int writerVersion = 1;
      if (writerVersionValue != null) {
        writerVersion = Integer.parseInt(writerVersionValue);
      }
      else if (Boolean.valueOf(isDateCorrect)) {
        writerVersion = DRILL_WRITER_VERSION_STD_DATE_FORMAT;
      }
      return writerVersion >= DRILL_WRITER_VERSION_STD_DATE_FORMAT ? DateCorruptionStatus.META_SHOWS_NO_CORRUPTION
          // loop through parquet column metadata to find date columns, check for corrupt values
          : checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
    } else {
      // Possibly an old, un-migrated Drill file, check the column statistics to see if min/max values look corrupt
      // only applies if there is a date column selected
      if (createdBy == null || createdBy.equals("parquet-mr")) {
        return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
      } else {
        // check the created by to see if it is a migrated Drill file
        try {
          VersionParser.ParsedVersion parsedCreatedByVersion = VersionParser.parse(createdBy);
          // check if this is a migrated Drill file, lacking a Drill version number, but with
          // "drill" in the parquet created-by string
          if (parsedCreatedByVersion.hasSemanticVersion()) {
            SemanticVersion semVer = parsedCreatedByVersion.getSemanticVersion();
            String pre = semVer.pre + "";
            if (semVer.major == 1 && semVer.minor == 8 && semVer.patch == 1 && pre.contains("drill")) {
              return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
            }
          }
          // written by a tool that wasn't Drill, the dates are not corrupted
          return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
        } catch (VersionParser.VersionParseException e) {
          // If we couldn't parse "created by" field, check column metadata of date columns
          return checkForCorruptDateValuesInStatistics(footer, columns, autoCorrectCorruptDates);
        }
      }
    }
  }


  /**
   * Detect corrupt date values by looking at the min/max values in the metadata.
   *
   * This should only be used when a file does not have enough metadata to determine if
   * the data was written with an external tool or an older version of Drill
   * ({@link org.apache.drill.exec.store.parquet.ParquetRecordWriter#WRITER_VERSION_PROPERTY} <
   * {@link org.apache.drill.exec.store.parquet.ParquetReaderUtility#DRILL_WRITER_VERSION_STD_DATE_FORMAT})
   *
   * This method only checks the first Row Group, because Drill has only ever written
   * a single Row Group per file.
   *
   * @param footer
   * @param columns
   * @param autoCorrectCorruptDates user setting to allow enabling/disabling of auto-correction
   *                                of corrupt dates. There are some rare cases (storing dates thousands
   *                                of years into the future, with tools other than Drill writing files)
   *                                that would result in the date values being "corrected" into bad values.
   */
  public static DateCorruptionStatus checkForCorruptDateValuesInStatistics(ParquetMetadata footer,
                                                              List<SchemaPath> columns,
                                                              boolean autoCorrectCorruptDates) {
    // Users can turn-off date correction in cases where we are detecting corruption based on the date values
    // that are unlikely to appear in common datasets. In this case report that no correction needs to happen
    // during the file read
    if (! autoCorrectCorruptDates) {
      return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
    }
    // Drill produced files have only ever have a single row group, if this changes in the future it won't matter
    // as we will know from the Drill version written in the files that the dates are correct
    int rowGroupIndex = 0;
    Map<String, SchemaElement> schemaElements = ParquetReaderUtility.getColNameToSchemaElementMapping(footer);
    findDateColWithStatsLoop : for (SchemaPath schemaPath : columns) {
      List<ColumnDescriptor> parquetColumns = footer.getFileMetaData().getSchema().getColumns();
      for (int i = 0; i < parquetColumns.size(); ++i) {
        ColumnDescriptor column = parquetColumns.get(i);
        // this reader only supports flat data, this is restricted in the ParquetScanBatchCreator
        // creating a NameSegment makes sure we are using the standard code for comparing names,
        // currently it is all case-insensitive
        if (Utilities.isStarQuery(columns)
            || new PathSegment.NameSegment(column.getPath()[0]).equals(schemaPath.getRootSegment())) {
          int colIndex = -1;
          ConvertedType convertedType = schemaElements.get(column.getPath()[0]).getConverted_type();
          if (convertedType != null && convertedType.equals(ConvertedType.DATE)) {
            List<ColumnChunkMetaData> colChunkList = footer.getBlocks().get(rowGroupIndex).getColumns();
            for (int j = 0; j < colChunkList.size(); j++) {
              if (colChunkList.get(j).getPath().equals(ColumnPath.get(column.getPath()))) {
                colIndex = j;
                break;
              }
            }
          }
          if (colIndex == -1) {
            // column does not appear in this file, skip it
            continue;
          }
          Statistics statistics = footer.getBlocks().get(rowGroupIndex).getColumns().get(colIndex).getStatistics();
          Integer max = (Integer) statistics.genericGetMax();
          if (statistics.hasNonNullValue()) {
            if (max > ParquetReaderUtility.DATE_CORRUPTION_THRESHOLD) {
              return DateCorruptionStatus.META_SHOWS_CORRUPTION;
            }
          } else {
            // no statistics, go check the first page
            return DateCorruptionStatus.META_UNCLEAR_TEST_VALUES;
          }
        }
      }
    }
    return DateCorruptionStatus.META_SHOWS_NO_CORRUPTION;
  }

  /**
   * Utilities for converting from parquet INT96 binary (impala, hive timestamp)
   * to date time value. This utilizes the Joda library.
   */
  public static class NanoTimeUtils {

    public static final long NANOS_PER_MILLISECOND = 1000000;

  /**
   * @param binaryTimeStampValue
   *          hive, impala timestamp values with nanoseconds precision
   *          are stored in parquet Binary as INT96 (12 constant bytes)
   * @param retainLocalTimezone
   *          parquet files don't keep local timeZone according to the
   *          <a href="https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#timestamp">Parquet spec</a>,
   *          but some tools (hive, for example) retain local timezone for parquet files by default
   *          Note: Impala doesn't retain local timezone by default
   * @return  Timestamp in milliseconds - the number of milliseconds since January 1, 1970, 00:00:00 GMT
   *          represented by @param binaryTimeStampValue.
   *          The nanos precision is cut to millis. Therefore the length of single timestamp value is
   *          {@value NullableTimeStampHolder#WIDTH} bytes instead of 12 bytes.
   */
    public static long getDateTimeValueFromBinary(Binary binaryTimeStampValue, boolean retainLocalTimezone) {
      // This method represents binaryTimeStampValue as ByteBuffer, where timestamp is stored as sum of
      // julian day number (4 bytes) and nanos of day (8 bytes)
      NanoTime nt = NanoTime.fromBinary(binaryTimeStampValue);
      int julianDay = nt.getJulianDay();
      long nanosOfDay = nt.getTimeOfDayNanos();
      long dateTime = (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * DateTimeConstants.MILLIS_PER_DAY
          + nanosOfDay / NANOS_PER_MILLISECOND;
      if (retainLocalTimezone) {
        return DateTimeZone.getDefault().convertUTCToLocal(dateTime);
      } else {
        return dateTime;
      }
    }
  }
}
