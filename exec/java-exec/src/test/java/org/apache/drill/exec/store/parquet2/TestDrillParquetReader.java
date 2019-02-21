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
package org.apache.drill.exec.store.parquet2;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.BaseTestQuery;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDrillParquetReader extends BaseTestQuery {
  // enable decimal data type and make sure DrillParquetReader is used to handle test queries
  @BeforeClass
  public static void setup() throws Exception {
    alterSession(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true);
    alterSession(ExecConstants.PARQUET_NEW_RECORD_READER, true);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
    resetSessionOption(ExecConstants.PARQUET_NEW_RECORD_READER);
  }

  private void testColumn(String columnName) throws Exception {
    BigDecimal result = new BigDecimal("1.20000000");

    testBuilder()
      .ordered()
      .sqlQuery("select %s from cp.`parquet2/decimal28_38.parquet`", columnName)
      .baselineColumns(columnName)
      .baselineValues(result)
      .go();
  }

  @Test
  public void testRequiredDecimal28() throws Exception {
    testColumn("d28_req");
  }

  @Test
  public void testRequiredDecimal38() throws Exception {
    testColumn("d38_req");
  }

  @Test
  public void testOptionalDecimal28() throws Exception {
    testColumn("d28_opt");
  }

  @Test
  public void testOptionalDecimal38() throws Exception {
    testColumn("d38_opt");
  }

  @Test
  public void test4349() throws Exception {
    // start by creating a parquet file from the input csv file
    runSQL("CREATE TABLE dfs.tmp.`4349` AS SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.`parquet2/4349.csv.gz`");

    // querying the parquet file should return the same results found in the csv file
    testBuilder()
      .unOrdered()
      .sqlQuery("SELECT * FROM dfs.tmp.`4349` WHERE id = 'b'")
      .sqlBaselineQuery("SELECT columns[0] id, CAST(NULLIF(columns[1], '') AS DOUBLE) val FROM cp.`parquet2/4349.csv.gz` WHERE columns[0] = 'b'")
      .go();
  }

  @Test
  public void testUnsignedAndSignedIntTypes() throws Exception {
    testBuilder()
      .unOrdered()
      .sqlQuery("select * from cp.`parquet/uint_types.parquet`")
      .baselineColumns("uint8_field", "uint16_field", "uint32_field", "uint64_field", "int8_field", "int16_field",
        "required_uint8_field", "required_uint16_field", "required_uint32_field", "required_uint64_field",
        "required_int8_field", "required_int16_field")
      .baselineValues(255, 65535, 2147483647, 9223372036854775807L, 255, 65535, -1, -1, -1, -1L, -2147483648, -2147483648)
      .baselineValues(-1, -1, -1, -1L, -2147483648, -2147483648, 255, 65535, 2147483647, 9223372036854775807L, 255, 65535)
      .baselineValues(null, null, null, null, null, null, 0, 0, 0, 0L, 0, 0)
      .go();
  }

  @Test
  public void testLogicalIntTypes() throws Exception {
    String query = String.format("select " +
        "t.uint_64 as uint_64, t.uint_32 as uint_32, t.uint_16 as uint_16, t.uint_8 as uint_8,  " +
        "t.int_64 as int_64, t.int_32 as int_32, t.int_16 as int_16, t.int_8 as int_8  " +
        "from cp.`parquet/logical_int.parquet` t" );
    String[] columns = {"uint_64", "uint_32", "uint_16", "uint_8", "int_64", "int_32", "int_16", "int_8" };
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues(0L, 0, 0, 0, 0L, 0, 0, 0)
        .baselineValues(-1L, -1, -1, -1, -1L, -1, -1, -1)
        .baselineValues(1L, 1, 1, 1, -9223372036854775808L, 1, 1, 1)
        .baselineValues(9223372036854775807L, 2147483647, 65535, 255, 9223372036854775807L, -2147483648, -32768, -128)
        .build()
        .run();
  }

  @Test //DRILL-5971
  public void testLogicalIntTypes2() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    Arrays.fill(bytesOnes, (byte)1);
    byte[] bytesZeros = new byte[12];
    String query = String.format(
        " select " +
            " t.rowKey as rowKey, " +
            " t._UTF8 as _UTF8, " +
            " t._Enum as _Enum, " +
            " t._INT32_RAW as _INT32_RAW, " +
            " t._INT_8 as _INT_8, " +
            " t._INT_16 as _INT_16, " +
            " t._INT_32 as _INT_32, " +
            " t._UINT_8 as _UINT_8, " +
            " t._UINT_16 as _UINT_16, " +
            " t._UINT_32 as _UINT_32, " +
            " t._INT64_RAW as _INT64_RAW, " +
            " t._INT_64 as _INT_64, " +
            " t._UINT_64 as _UINT_64, " +
            " t._DATE_int32 as _DATE_int32, " +
            " t._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
            " t._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
            " t._TIMESTAMP_MICROS_int64 as _TIMESTAMP_MICROS_int64, " +
            " t._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
            " t._INT96_RAW as _INT96_RAW " +
            " from " +
            " cp.`parquet/parquet_logical_types_simple.parquet` t " +
            " order by t.rowKey "
    );
    String[] columns = {
        "rowKey ",
        "_UTF8",
        "_Enum",
        "_INT32_RAW",
        "_INT_8",
        "_INT_16",
        "_INT_32",
        "_UINT_8",
        "_UINT_16",
        "_UINT_32",
        "_INT64_RAW",
        "_INT_64",
        "_UINT_64",
        "_DATE_int32",
        "_TIME_MILLIS_int32",
        "_TIMESTAMP_MILLIS_int64",
        "_TIMESTAMP_MICROS_int64",
        "_INTERVAL_fixed_len_byte_array_12",
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, LocalDate.parse("5350-02-17"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1973-11-29T21:33:09.012"), 123456789012L,
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, LocalDate.parse("1969-12-31"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0xFFFFFFFF), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("2038-01-19T03:14:07.999"), 9223372036854775807L,
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, LocalDate.parse("1970-01-01"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1970-01-01T00:00:00.0"), 0L, new Period("PT0S"), bytesZeros)
        .build()
        .run();
  }

  @Test //DRILL-5971
  public void testLogicalIntTypes3() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    Arrays.fill(bytesOnes, (byte)1);
    byte[] bytesZeros = new byte[12];
    String query = String.format(
        " select " +
            " t.rowKey as rowKey, " +
            " t._UTF8 as _UTF8, " +
            " t._Enum as _Enum, " +
            " t._INT32_RAW as _INT32_RAW, " +
            " t._INT_8 as _INT_8, " +
            " t._INT_16 as _INT_16, " +
            " t._INT_32 as _INT_32, " +
            " t._UINT_8 as _UINT_8, " +
            " t._UINT_16 as _UINT_16, " +
            " t._UINT_32 as _UINT_32, " +
            " t._INT64_RAW as _INT64_RAW, " +
            " t._INT_64 as _INT_64, " +
            " t._UINT_64 as _UINT_64, " +
            " t._DATE_int32 as _DATE_int32, " +
            " t._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
            " t._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
            " t._TIMESTAMP_MICROS_int64 as _TIMESTAMP_MICROS_int64, " +
            " t._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
            " t._INT96_RAW as _INT96_RAW " +
            " from " +
            " cp.`parquet/parquet_logical_types_simple_nullable.parquet` t " +
            " order by t.rowKey "
    );
    String[] columns = {
        "rowKey ",
        "_UTF8",
        "_Enum",
        "_INT32_RAW",
        "_INT_8",
        "_INT_16",
        "_INT_32",
        "_UINT_8",
        "_UINT_16",
        "_UINT_32",
        "_INT64_RAW",
        "_INT_64",
        "_UINT_64",
        "_DATE_int32",
        "_TIME_MILLIS_int32",
        "_TIMESTAMP_MILLIS_int64",
        "_TIMESTAMP_MICROS_int64",
        "_INTERVAL_fixed_len_byte_array_12",
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, LocalDate.parse("5350-02-17"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1973-11-29T21:33:09.012"), 123456789012L,
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, LocalDate.parse("1969-12-31"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0xFFFFFFFF), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("2038-01-19T03:14:07.999"), 9223372036854775807L,
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, LocalDate.parse("1970-01-01"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1970-01-01T00:00:00.0"), 0L, new Period("PT0S"), bytesZeros)
        .baselineValues(4, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null)
        .build().run();
  }

  @Test //DRILL-6670: include tests on data with dictionary encoding disabled
  public void testLogicalIntTypes4() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    Arrays.fill(bytesOnes, (byte)1);
    byte[] bytesZeros = new byte[12];
    String query = String.format(
        " select " +
            " t.rowKey as rowKey, " +
            " t._UTF8 as _UTF8, " +
            " t._Enum as _Enum, " +
            " t._INT32_RAW as _INT32_RAW, " +
            " t._INT_8 as _INT_8, " +
            " t._INT_16 as _INT_16, " +
            " t._INT_32 as _INT_32, " +
            " t._UINT_8 as _UINT_8, " +
            " t._UINT_16 as _UINT_16, " +
            " t._UINT_32 as _UINT_32, " +
            " t._INT64_RAW as _INT64_RAW, " +
            " t._INT_64 as _INT_64, " +
            " t._UINT_64 as _UINT_64, " +
            " t._DATE_int32 as _DATE_int32, " +
            " t._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
            " t._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
            " t._TIMESTAMP_MICROS_int64 as _TIMESTAMP_MICROS_int64, " +
            " t._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
            " t._INT96_RAW as _INT96_RAW " +
            " from " +
            " cp.`parquet/parquet_logical_types_simple_nodict.parquet` t " +
            " order by t.rowKey "
    );
    String[] columns = {
        "rowKey ",
        "_UTF8",
        "_Enum",
        "_INT32_RAW",
        "_INT_8",
        "_INT_16",
        "_INT_32",
        "_UINT_8",
        "_UINT_16",
        "_UINT_32",
        "_INT64_RAW",
        "_INT_64",
        "_UINT_64",
        "_DATE_int32",
        "_TIME_MILLIS_int32",
        "_TIMESTAMP_MILLIS_int64",
        "_TIMESTAMP_MICROS_int64",
        "_INTERVAL_fixed_len_byte_array_12",
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, LocalDate.parse("5350-02-17"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1973-11-29T21:33:09.012"), 123456789012L,
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, LocalDate.parse("1969-12-31"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0xFFFFFFFF), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("2038-01-19T03:14:07.999"), 9223372036854775807L,
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, LocalDate.parse("1970-01-01"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1970-01-01T00:00:00.0"), 0L, new Period("PT0S"), bytesZeros)
        .build()
        .run();
  }

  @Test //DRILL-6670: include tests on data with dictionary encoding disabled
  public void testLogicalIntTypes5() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    Arrays.fill(bytesOnes, (byte)1);
    byte[] bytesZeros = new byte[12];
    String query = String.format(
        " select " +
            " t.rowKey as rowKey, " +
            " t._UTF8 as _UTF8, " +
            " t._Enum as _Enum, " +
            " t._INT32_RAW as _INT32_RAW, " +
            " t._INT_8 as _INT_8, " +
            " t._INT_16 as _INT_16, " +
            " t._INT_32 as _INT_32, " +
            " t._UINT_8 as _UINT_8, " +
            " t._UINT_16 as _UINT_16, " +
            " t._UINT_32 as _UINT_32, " +
            " t._INT64_RAW as _INT64_RAW, " +
            " t._INT_64 as _INT_64, " +
            " t._UINT_64 as _UINT_64, " +
            " t._DATE_int32 as _DATE_int32, " +
            " t._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
            " t._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
            " t._TIMESTAMP_MICROS_int64 as _TIMESTAMP_MICROS_int64, " +
            " t._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
            " t._INT96_RAW as _INT96_RAW " +
            " from " +
            " cp.`parquet/parquet_logical_types_simple_nullable_nodict.parquet` t " +
            " order by t.rowKey "
    );
    String[] columns = {
        "rowKey ",
        "_UTF8",
        "_Enum",
        "_INT32_RAW",
        "_INT_8",
        "_INT_16",
        "_INT_32",
        "_UINT_8",
        "_UINT_16",
        "_UINT_32",
        "_INT64_RAW",
        "_INT_64",
        "_UINT_64",
        "_DATE_int32",
        "_TIME_MILLIS_int32",
        "_TIMESTAMP_MILLIS_int64",
        "_TIMESTAMP_MICROS_int64",
        "_INTERVAL_fixed_len_byte_array_12",
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, LocalDate.parse("5350-02-17"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(1234567), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1973-11-29T21:33:09.012"), 123456789012L,
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, LocalDate.parse("1969-12-31"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0xFFFFFFFF), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("2038-01-19T03:14:07.999"), 9223372036854775807L,
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, LocalDate.parse("1970-01-01"),
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC).toLocalTime(),
            LocalDateTime.parse("1970-01-01T00:00:00.0"), 0L, new Period("PT0S"), bytesZeros)
        .baselineValues(4, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null, null)
        .build().run();
  }

  @Test // DRILL-6856
  public void testIsTrueOrNullCondition() throws Exception {
    testBuilder()
        .sqlQuery("SELECT col_bln " +
            "FROM cp.`parquetFilterPush/blnTbl/0_0_2.parquet` " +
            "WHERE col_bln IS true OR col_bln IS null " +
            "ORDER BY col_bln")
        .ordered()
        .baselineColumns("col_bln")
        .baselineValuesForSingleColumn(true, null)
        .go();
  }

}
