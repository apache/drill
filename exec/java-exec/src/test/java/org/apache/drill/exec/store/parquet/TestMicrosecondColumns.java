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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.apache.drill.categories.ParquetTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.TestBuilder;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({ParquetTest.class, UnlikelyTest.class})
public class TestMicrosecondColumns extends ClusterTest {

  private static final String TIME_FORMAT = "HH:mm:ss.SSS";
  private static final String TO_TIME_TEMPLATE = "TO_TIME('%s', 'HH:mm:ss.SSS')";
  private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern(TIME_FORMAT);
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  private static final String TO_TIMESTAMP_TEMPLATE = "TO_TIMESTAMP('%s', 'yyy-MM-dd''T''HH:mm:ss.SSS')";
  private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT);

  // The parquet files used in the test cases, created by ParquetSimpleTestFileGenerator.
  private static final String DATAFILE = "cp.`parquet/microseconds.parquet`";
  private static final String DATAFILE_SMALL_DIFF = "cp.`parquet/microseconds_small_diff.parquet`";

  // Test values for the _TIME_MICROS_int64 field. Will be written to the test parquet file by
  // ParquetSimpleTestFileGenerator.
   static final long[] TIME_MICROS_VALUES = {
      toMicrosecondTime(0, 32, 58, 174711),
      toMicrosecondTime(9, 0, 22, 654321),
      toMicrosecondTime(22, 12, 41, 123456)
  };

  // Test values for the _TIMESTAMP_MICROS_int64 field. Will be written to the test parquet file by
  // ParquetSimpleTestFileGenerator.
   static final long[] TIMESTAMP_MICROS_VALUES = {
      toMicrosecondTimestamp(2021, 8, 1, 22, 12, 41, 123456),
      toMicrosecondTimestamp(2022, 5, 6, 9, 0, 22, 654321),
      toMicrosecondTimestamp(2023, 2, 10, 0, 32, 58, 174711)
  };

  // Test values with small differences (less than a millisecond) for the _TIME_MICROS_int64 field.
  // Used for testing ORDER BY. Written to the test parquet file by ParquetSimpleTestFileGenerator.
  static final long[] TIME_MICROS_SMALL_DIFF_VALUES = {
      toMicrosecondTime(10, 11, 12, 336804),
      toMicrosecondTime(10, 11, 12, 336587),
      toMicrosecondTime(10, 11, 12, 336172),
      toMicrosecondTime(10, 11, 12, 336991),
      toMicrosecondTime(10, 11, 12, 336336)
  };

  // Test values with small differences (less than a millisecond) for the _TIMESTAMP_MICROS_int64
  // field. Used for testing ORDER BY. Written to the test parquet file by ParquetSimpleTestFileGenerator.
  static final long[] TIMESTAMP_MICROS_SMALL_DIFF_VALUES = {
      toMicrosecondTimestamp(2024, 3, 16, 19, 1, 54, 182665),
      toMicrosecondTimestamp(2024, 3, 16, 19, 1, 54, 182429),
      toMicrosecondTimestamp(2024, 3, 16, 19, 1, 54, 182707),
      toMicrosecondTimestamp(2024, 3, 16, 19, 1, 54, 182003),
      toMicrosecondTimestamp(2024, 3, 16, 19, 1, 54, 182860)
  };

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }


  @After
  public void restoreSession() {
    client.alterSession(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64, false);
    client.alterSession(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64, false);
  }


  @Test
  public void testSelectTimeColumns() throws Exception {
    // DRILL-8423
    String query = "select _TIME_MICROS_int64 as t from %s";
    testBuilder()
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(toLocalTime(TIME_MICROS_VALUES[0]))
        .baselineValues(toLocalTime(TIME_MICROS_VALUES[1]))
        .baselineValues(toLocalTime(TIME_MICROS_VALUES[2]))
        .go();
  }


  @Test
  public void testLessThanSmallestTime() throws Exception {
    // No time values should be less than the smallest value in the parquet file
    int expectedCount = 0;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[0]);
    executeFilterQuery("_TIME_MICROS_int64 < " + timeExpr, expectedCount);
  }


  @Test
  public void testLessThanMidTime() throws Exception {
    // The smallest time value should be less than the middle value in the parquet file
    int expectedCount = 1;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[1]);
    executeFilterQuery("_TIME_MICROS_int64 < " + timeExpr, expectedCount);
  }


  @Test
  public void testLessThanLargestTime() throws Exception {
    // The smallest and middle time values should be less than the largest value in the parquet file
    int expectedCount = 2;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[2]);
    executeFilterQuery("_TIME_MICROS_int64 < " + timeExpr, expectedCount);
  }


  @Test
  public void testGreaterThanSmallestTime() throws Exception {
    // The middle and largest time values should be greater than the smallest value in the parquet
    // file
    int expectedCount = 2;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[0]);
    executeFilterQuery("_TIME_MICROS_int64 > " + timeExpr, expectedCount);
  }


  @Test
  public void testGreaterThanMidTime() throws Exception {
    // The largest time value should be greater than the middle value in the parquet file
    int expectedCount = 1;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[1]);
    executeFilterQuery("_TIME_MICROS_int64 > " + timeExpr, expectedCount);
  }


  @Test
  public void testGreaterThanLargestTime() throws Exception {
    // No time value should be greater than the largest value in the parquet file
    int expectedCount = 0;
    String timeExpr = createToTimeFragment(TIME_MICROS_VALUES[2]);
    executeFilterQuery("_TIME_MICROS_int64 > " + timeExpr, expectedCount);
  }


  @Test
  public void testTimeRange() throws Exception {
    // The middle time test value should be greater than the smallest value and less than the
    // largest in the parquet file
    int expectedCount = 1;
    String lower = createToTimeFragment(TIME_MICROS_VALUES[0]);
    String upper = createToTimeFragment(TIME_MICROS_VALUES[2]);
    executeFilterQuery("_TIME_MICROS_int64 > " + lower + " and _TIME_MICROS_int64 < " + upper, expectedCount);
  }


  @Test
  public void testSelectTimeColumnAsBigInt() throws Exception {
    String query = "select _TIME_MICROS_int64 as t from %s";
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIME_MICROS_VALUES[0])
        .baselineValues(TIME_MICROS_VALUES[1])
        .baselineValues(TIME_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testSelectStarTimeColumnAsBigInt() throws Exception {
    // PARQUET_READER_TIME_MICROS_AS_INT64 should only affect time_micros columns, not
    // timestamp_micros columns.
    String query = "select * from %s";
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("rowKey","_TIME_MICROS_int64","_TIMESTAMP_MICROS_int64")
        .baselineValues(1, TIME_MICROS_VALUES[0], toLocalDateTime(TIMESTAMP_MICROS_VALUES[0]))
        .baselineValues(2, TIME_MICROS_VALUES[1], toLocalDateTime(TIMESTAMP_MICROS_VALUES[1]))
        .baselineValues(3, TIME_MICROS_VALUES[2], toLocalDateTime(TIMESTAMP_MICROS_VALUES[2]))
        .go();
  }


  @Test
  public void testSelectTimeColumnAsBigIntWithBigIntFilter() throws Exception {
    String query = "select _TIME_MICROS_int64 as t from %s where _TIME_MICROS_int64 > " + TIME_MICROS_VALUES[0];
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIME_MICROS_VALUES[1])
        .baselineValues(TIME_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testSelectTimeColumnAsBigIntWithTimeFilter() throws Exception {
    String query = "select _TIME_MICROS_int64 as t from %s where TO_TIME(_TIME_MICROS_int64/1000) > " + createToTimeFragment(TIME_MICROS_VALUES[0]);
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIME_MICROS_VALUES[1])
        .baselineValues(TIME_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testOrderByTimeColumnAsBigInt() throws Exception {
    long[] sortedValues = Arrays.copyOf(TIME_MICROS_SMALL_DIFF_VALUES, TIME_MICROS_SMALL_DIFF_VALUES.length);
    Arrays.sort(sortedValues);
    String query = "select _TIME_MICROS_int64 as t from %s ORDER BY t";
    TestBuilder builder = testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE_SMALL_DIFF)
        .ordered()
        .baselineColumns("t");
    for (long expectedValue : sortedValues) {
      builder.baselineValues(expectedValue);
    }
    builder.go();
  }


  @Test
  public void testOrderByDescTimeColumnAsBigInt() throws Exception {
    long[] sortedValues = Arrays.copyOf(TIME_MICROS_SMALL_DIFF_VALUES, TIME_MICROS_SMALL_DIFF_VALUES.length);
    Arrays.sort(sortedValues);
    String query = "select _TIME_MICROS_int64 as t from %s ORDER BY t DESC";
    TestBuilder builder = testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIME_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE_SMALL_DIFF)
        .ordered()
        .baselineColumns("t");
    for (int i=sortedValues.length-1; i>= 0; i--) {
      builder.baselineValues(sortedValues[i]);
    }
    builder.go();
  }


  @Test
  public void testSelectTimestampColumns() throws Exception {
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s";
    testBuilder()
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(toLocalDateTime(TIMESTAMP_MICROS_VALUES[0]))
        .baselineValues(toLocalDateTime(TIMESTAMP_MICROS_VALUES[1]))
        .baselineValues(toLocalDateTime(TIMESTAMP_MICROS_VALUES[2]))
        .go();
  }


  @Test
  public void testLessThanSmallestTimestamp() throws Exception {
    // No timestamp values should be less than the smallest value in the parquet file
    int expectedCount = 0;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[0]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 < " + timestampExpr, expectedCount);
  }


  @Test
  public void testLessThanMidTimestamp() throws Exception {
    // The smallest timestamp value should be less than the middle value in the parquet file
    int expectedCount = 1;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[1]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 < " + timestampExpr, expectedCount);
  }


  @Test
  public void testLessThanLargestTimestamp() throws Exception {
    // The smallest and middle timestamp values should be less than the largest value in the parquet
    // file
    int expectedCount = 2;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[2]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 < " + timestampExpr, expectedCount);
  }


  @Test
  public void testLessThanTimestampLongIntoTheFuture() throws Exception {
    // All test timestamps should be less than a timestamp several hundred years into the future
    // See https://issues.apache.org/jira/browse/DRILL-8421
    int expectedCount = 3;
    String whereClause = "_TIMESTAMP_MICROS_int64 < TO_TIMESTAMP('2502-04-04 00:00:00', 'yyyy-MM-dd HH:mm:ss')";
    executeFilterQuery(whereClause, expectedCount);
  }


  @Test
  public void testGreaterThanSmallestTimestamp() throws Exception {
    // The middle and largest timestamp values should be greater than the smallest value in the
    // parquet file
    int expectedCount = 2;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[0]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 > " + timestampExpr, expectedCount);
  }


  @Test
  public void testGreaterThanMidTimestamp() throws Exception {
    // The largest timestamp value should be greater than the middle value in the parquet file
    int expectedCount = 1;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[1]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 > " + timestampExpr, expectedCount);
  }


  @Test
  public void testGreaterThanLargestTimestamp() throws Exception {
    // No timestamp values should be greater than the largest value in the parquet file
    int expectedCount = 0;
    String timestampExpr = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[2]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 > " + timestampExpr, expectedCount);
  }


  @Test
  public void testGreaterThanTimestampLongIntoTheFuture() throws Exception {
    // No test timestamps should be greater than a timestamp several hundred years into the future
    // See https://issues.apache.org/jira/browse/DRILL-8421
    int expectedCount = 0;
    String whereClause = "_TIMESTAMP_MICROS_int64 > TO_TIMESTAMP('2502-04-04 00:00:00', 'yyyy-MM-dd HH:mm:ss')";
    executeFilterQuery(whereClause, expectedCount);
  }


  @Test
  public void testTimestampRange() throws Exception {
    // The middle timestamp test value should be greater than the smallest value and less than the
    // largest in the parquet file
    String lower = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[0]);
    String upper = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[2]);
    executeFilterQuery("_TIMESTAMP_MICROS_int64 > " + lower + " and _TIMESTAMP_MICROS_int64 < " + upper, 1);
  }


  @Test
  public void testSelectTimestampColumnAsBigInt() throws Exception {
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s";
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIMESTAMP_MICROS_VALUES[0])
        .baselineValues(TIMESTAMP_MICROS_VALUES[1])
        .baselineValues(TIMESTAMP_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testSelectStarTimestampColumnAsBigInt() throws Exception {
    // PARQUET_READER_TIMESTAMP_MICROS_AS_INT64 should only affect timestamp_micros columns, not
    // time_micros columns.
    String query = "select * from %s";
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("rowKey","_TIME_MICROS_int64","_TIMESTAMP_MICROS_int64")
        .baselineValues(1, toLocalTime(TIME_MICROS_VALUES[0]), TIMESTAMP_MICROS_VALUES[0])
        .baselineValues(2, toLocalTime(TIME_MICROS_VALUES[1]), TIMESTAMP_MICROS_VALUES[1])
        .baselineValues(3, toLocalTime(TIME_MICROS_VALUES[2]), TIMESTAMP_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testSelectTimestampColumnAsBigIntWithBigIntFilter() throws Exception {
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s where _TIMESTAMP_MICROS_int64 > " + TIMESTAMP_MICROS_VALUES[0];
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIMESTAMP_MICROS_VALUES[1])
        .baselineValues(TIMESTAMP_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testSelectTimestampColumnAsBigIntWithTimestampFilter() throws Exception {
    // TO_TIMESTAMP(double) creates a timestamp in the system default timezone, must compare to
    // a TO_TIMESTAMP(string ,format) in the same timezone.
    String toTimestampTerm = createToTimestampFragment(TIMESTAMP_MICROS_VALUES[0], ZoneId.systemDefault());
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s where TO_TIMESTAMP(_TIMESTAMP_MICROS_int64/1000000) > " + toTimestampTerm;
    testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("t")
        .baselineValues(TIMESTAMP_MICROS_VALUES[1])
        .baselineValues(TIMESTAMP_MICROS_VALUES[2])
        .go();
  }


  @Test
  public void testOrderByTimestampColumnAsBigInt() throws Exception {
    long[] sortedValues = Arrays.copyOf(TIMESTAMP_MICROS_SMALL_DIFF_VALUES, TIMESTAMP_MICROS_SMALL_DIFF_VALUES.length);
    Arrays.sort(sortedValues);
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s ORDER BY t";
    TestBuilder builder = testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE_SMALL_DIFF)
        .ordered()
        .baselineColumns("t");
    for (long expectedValue : sortedValues) {
      builder.baselineValues(expectedValue);
    }
    builder.go();
  }


  @Test
  public void testOrderByDescTimestampColumnAsBigInt() throws Exception {
    long[] sortedValues = Arrays.copyOf(TIMESTAMP_MICROS_SMALL_DIFF_VALUES, TIMESTAMP_MICROS_SMALL_DIFF_VALUES.length);
    Arrays.sort(sortedValues);
    String query = "select _TIMESTAMP_MICROS_int64 as t from %s ORDER BY t DESC";
    TestBuilder builder = testBuilder()
        .enableSessionOption(ExecConstants.PARQUET_READER_TIMESTAMP_MICROS_AS_INT64)
        .sqlQuery(query, DATAFILE_SMALL_DIFF)
        .ordered()
        .baselineColumns("t");
    for (int i=sortedValues.length-1; i>= 0; i--) {
      builder.baselineValues(sortedValues[i]);
    }
    builder.go();
  }


  private void executeFilterQuery(String whereClause, long expectedCount) throws Exception {
    String query = "select count(*) as c from %s where " + whereClause;
    testBuilder()
        .sqlQuery(query, DATAFILE)
        .unOrdered()
        .baselineColumns("c")
        .baselineValues(expectedCount)
        .go();
  }


  private static String createToTimeFragment(long micros) {
    return String.format(TO_TIME_TEMPLATE, TIME_FORMATTER.format(toLocalTime(micros)));
  }


  private static String createToTimestampFragment(long micros) {
    return String.format(TO_TIMESTAMP_TEMPLATE, TIMESTAMP_FORMATTER.format(toLocalDateTime(micros)));
  }


  private static String createToTimestampFragment(long micros, ZoneId timeZone) {
    return String.format(TO_TIMESTAMP_TEMPLATE, TIMESTAMP_FORMATTER.format(toLocalDateTime(micros, timeZone)));
  }


  private static LocalTime toLocalTime(long micros) {
    return LocalTime.ofNanoOfDay((micros/1000L) * 1000_000L);
  }


  private static LocalDateTime toLocalDateTime(long micros) {
    return toLocalDateTime(micros, ZoneOffset.ofHours(0));
  }


  private static LocalDateTime toLocalDateTime(long micros, ZoneId timeZone) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(micros/1000L), timeZone);
  }


  private static long toMicrosecondTime(int hour, int minute, int second, int microOfSecond) {
    return LocalTime.of(hour, minute, second, microOfSecond*1000).toNanoOfDay() / 1000L;
  }


  private static long toMicrosecondTimestamp(
      int year,
      int month,
      int dayOfMonth,
      int hour,
      int minute,
      int second,
      int microOfSecond) {

    Instant instant =
        LocalDateTime
            .of(year, month, dayOfMonth, hour, minute, second, microOfSecond*1000)
            .toInstant(ZoneOffset.ofHours(0));

    return instant.getEpochSecond() * 1000_000L + instant.getNano() / 1000L;
  }
}
