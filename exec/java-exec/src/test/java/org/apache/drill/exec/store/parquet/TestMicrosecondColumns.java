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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.drill.categories.ParquetTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;

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

  // The parquet file used in the test cases, created by ParquetSimpleTestFileGenerator.
  private static final String DATAFILE = "cp.`parquet/microseconds.parquet`";

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


  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher));
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


  private static LocalTime toLocalTime(long micros) {
    return LocalTime.ofNanoOfDay((micros/1000L) * 1000_000L);
  }


  private static LocalDateTime toLocalDateTime(long micros) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(micros/1000L), ZoneOffset.ofHours(0));
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
