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
package org.apache.drill.exec.fn.impl;

import static org.apache.drill.exec.expr.fn.impl.DateUtility.formatDate;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.formatTime;
import static org.apache.drill.exec.expr.fn.impl.DateUtility.formatTimeStamp;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.test.BaseTestQuery;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestDateTruncFunctions extends BaseTestQuery {

  @Test
  public void dateTruncOnTime() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', time '2:30:21.5') as `second`, " +
        "date_trunc('MINUTE', time '2:30:21.5') as `minute`, " +
        "date_trunc('HOUR', time '2:30:21.5') as `hour`, " +
        "date_trunc('DAY', time '2:30:21.5') as `day`, " +
        "date_trunc('MONTH', time '2:30:21.5') as `month`, " +
        "date_trunc('YEAR', time '2:30:21.5') as `year`, " +
        "date_trunc('QUARTER', time '2:30:21.5') as `quarter`, " +
        "date_trunc('DECADE', time '2:30:21.5') as `decade`, " +
        "date_trunc('CENTURY', time '2:30:21.5') as `century`, " +
        "date_trunc('MILLENNIUM', time '2:30:21.5') as `millennium` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            new Time(formatTime.parseDateTime("2:30:21.0").getMillis()), // seconds
            new Time(formatTime.parseDateTime("2:30:00.0").getMillis()), // minute
            new Time(formatTime.parseDateTime("2:00:00.0").getMillis()), // hour
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // day
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // month
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // year
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // quarter
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // decade
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis()), // century
            new Time(formatTime.parseDateTime("0:00:00.0").getMillis())) // millennium
        .go();
  }

  @Test
  public void dateTruncOnDateSimpleUnits() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', date '2011-2-3') as `second`, " +
        "date_trunc('MINUTE', date '2011-2-3') as `minute`, " +
        "date_trunc('HOUR', date '2011-2-3') as `hour`, " +
        "date_trunc('DAY', date '2011-2-3') as `day`, " +
        "date_trunc('WEEK', date '2011-2-3') as `week`, " +
        "date_trunc('MONTH', date '2011-2-3') as `month`, " +
        "date_trunc('YEAR', date '2011-2-3') as `year`, " +
        "date_trunc('QUARTER', date '2011-5-3') as `q1`, " +
        "date_trunc('QUARTER', date '2011-7-13') as `q2`, " +
        "date_trunc('QUARTER', date '2011-9-13') as `q3`, " +
        "date_trunc('DECADE', date '2011-2-3') as `decade1`, " +
        "date_trunc('DECADE', date '2072-2-3') as `decade2`, " +
        "date_trunc('DECADE', date '1978-2-3') as `decade3` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "week" , "year", "q1", "q2", "q3", "decade1", "decade2", "decade3")
        .baselineValues(
            new Date(formatDate.parseDateTime("2011-02-03").getMillis()), // seconds
            new Date(formatDate.parseDateTime("2011-02-03").getMillis()), // minute
            new Date(formatDate.parseDateTime("2011-02-03").getMillis()), // hour
            new Date(formatDate.parseDateTime("2011-02-03").getMillis()), // day
            new Date(formatDate.parseDateTime("2011-02-01").getMillis()), // month
            new Date(formatDate.parseDateTime("2011-01-31").getMillis()), // week
            new Date(formatDate.parseDateTime("2011-01-01").getMillis()), // year
            new Date(formatDate.parseDateTime("2011-04-01").getMillis()), // quarter-1
            new Date(formatDate.parseDateTime("2011-07-01").getMillis()), // quarter-2
            new Date(formatDate.parseDateTime("2011-07-01").getMillis()), // quarter-3
            new Date(formatDate.parseDateTime("2010-01-01").getMillis()), // decade-1
            new Date(formatDate.parseDateTime("2070-01-01").getMillis()), // decade-2
            new Date(formatDate.parseDateTime("1970-01-01").getMillis())) // decade-3
        .go();
  }

  @Test
  public void dateTruncOnDateCentury() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('CENTURY', date '2011-2-3') as c1, " +
        "date_trunc('CENTURY', date '2000-2-3') as c2, " +
        "date_trunc('CENTURY', date '1901-11-3') as c3, " +
        "date_trunc('CENTURY', date '900-2-3') as c4, " +
        "date_trunc('CENTURY', date '0001-1-3') as c5 " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2", "c3", "c4", "c5")
        .baselineValues(
            new Date(formatDate.parseDateTime("2001-01-01").getMillis()), // c1
            new Date(formatDate.parseDateTime("1901-01-01").getMillis()), // c2
            new Date(formatDate.parseDateTime("1901-01-01").getMillis()), // c3
            new Date(formatDate.parseDateTime("0801-01-01").getMillis()), // c4
            new Date(formatDate.parseDateTime("0001-01-01").getMillis())) // c5
        .go();
  }

  @Test
  public void test() throws Exception {
    org.joda.time.MutableDateTime dateTime = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
    dateTime.setMillis(formatDate.parseDateTime("2001-01-01"));
    dateTime.setRounding(dateTime.getChronology().centuryOfEra());
  }

  @Test
  public void dateTruncOnDateMillennium() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('MILLENNIUM', date '2011-2-3') as `m1`, " +
        "date_trunc('MILLENNIUM', date '2000-11-3') as `m2`, " +
        "date_trunc('MILLENNIUM', date '1983-05-18') as `m3`, " +
        "date_trunc('MILLENNIUM', date '990-11-3') as `m4`, " +
        "date_trunc('MILLENNIUM', date '0001-11-3') as `m5` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("m1", "m2", "m3", "m4", "m5")
        .baselineValues(
                new Date(formatDate.parseDateTime("2001-01-01").getMillis()), // m1
                new Date(formatDate.parseDateTime("1001-01-01").getMillis()), // m2
                new Date(formatDate.parseDateTime("1001-01-01").getMillis()), // m3
                new Date(formatDate.parseDateTime("0001-01-01").getMillis()), // m4
                new Date(formatDate.parseDateTime("0001-01-01").getMillis())) // m5
        .go();
  }

  @Test
  public void dateTruncOnTimeStampSimpleUnits() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', timestamp '2011-2-3 10:11:12.100') as `second`, " +
        "date_trunc('MINUTE', timestamp '2011-2-3 10:11:12.100') as `minute`, " +
        "date_trunc('HOUR', timestamp '2011-2-3 10:11:12.100') as `hour`, " +
        "date_trunc('DAY', timestamp '2011-2-3 10:11:12.100') as `day`, " +
        "date_trunc('WEEK', timestamp '2011-2-3 10:11:12.100') as `week`, " +
        "date_trunc('MONTH', timestamp '2011-2-3 10:11:12.100') as `month`, " +
        "date_trunc('YEAR', timestamp '2011-2-3 10:11:12.100') as `year`, " +
        "date_trunc('QUARTER', timestamp '2011-5-3 10:11:12.100') as `q1`, " +
        "date_trunc('QUARTER', timestamp '2011-7-13 10:11:12.100') as `q2`, " +
        "date_trunc('QUARTER', timestamp '2011-9-13 10:11:12.100') as `q3`, " +
        "date_trunc('DECADE', timestamp '2011-2-3 10:11:12.100') as `decade1`, " +
        "date_trunc('DECADE', timestamp '2072-2-3 10:11:12.100') as `decade2`, " +
        "date_trunc('DECADE', timestamp '1978-2-3 10:11:12.100') as `decade3` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "week" , "year", "q1", "q2", "q3", "decade1", "decade2", "decade3")
        .baselineValues(
            new Timestamp(formatTimeStamp.parseDateTime("2011-02-03 10:11:12.0").getMillis()), // seconds
            new Timestamp(formatTimeStamp.parseDateTime("2011-02-03 10:11:00.0").getMillis()), // minute
            new Timestamp(formatTimeStamp.parseDateTime("2011-02-03 10:00:00.0").getMillis()), // hour
            new Timestamp(formatTimeStamp.parseDateTime("2011-02-03 00:00:00.0").getMillis()), // day
            new Timestamp(formatTimeStamp.parseDateTime("2011-02-01 00:00:00.0").getMillis()), // month
            new Timestamp(formatTimeStamp.parseDateTime("2011-01-31 00:00:00.0").getMillis()), // week
            new Timestamp(formatTimeStamp.parseDateTime("2011-01-01 00:00:00.0").getMillis()), // year
            new Timestamp(formatTimeStamp.parseDateTime("2011-04-01 00:00:00.0").getMillis()), // quarter-1
            new Timestamp(formatTimeStamp.parseDateTime("2011-07-01 00:00:00.0").getMillis()), // quarter-2
            new Timestamp(formatTimeStamp.parseDateTime("2011-07-01 00:00:00.0").getMillis()), // quarter-3
            new Timestamp(formatTimeStamp.parseDateTime("2010-01-01 00:00:00.0").getMillis()), // decade-1
            new Timestamp(formatTimeStamp.parseDateTime("2070-01-01 00:00:00.0").getMillis()), // decade-2
            new Timestamp(formatTimeStamp.parseDateTime("1970-01-01 00:00:00.0").getMillis())) // decade-3
        .go();
  }

  @Test
  public void dateTruncOnTimeStampCentury() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('CENTURY', timestamp '2011-2-3 10:11:12.100') as c1, " +
        "date_trunc('CENTURY', timestamp '2000-2-3 10:11:12.100') as c2, " +
        "date_trunc('CENTURY', timestamp '1901-11-3 10:11:12.100') as c3, " +
        "date_trunc('CENTURY', timestamp '900-2-3 10:11:12.100') as c4, " +
        "date_trunc('CENTURY', timestamp '0001-1-3 10:11:12.100') as c5 " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c1", "c2", "c3", "c4", "c5")
        .baselineValues(
            new Timestamp(formatTimeStamp.parseDateTime("2001-01-01 00:00:00.0").getMillis()), // c1
            new Timestamp(formatTimeStamp.parseDateTime("1901-01-01 00:00:00.0").getMillis()), // c2
            new Timestamp(formatTimeStamp.parseDateTime("1901-01-01 00:00:00.0").getMillis()), // c3
            new Timestamp(formatTimeStamp.parseDateTime("0801-01-01 00:00:00.0").getMillis()), // c4
            new Timestamp(formatTimeStamp.parseDateTime("0001-01-01 00:00:00.0").getMillis())) // c5
        .go();
  }

  @Test
  public void dateTruncOnTimeStampMillennium() throws Exception {
    // TODO: It would be good to have some tests on dates in BC period, but looks like currently Calcite parser is
    // not accepting date literals in BC.
    final String query = "SELECT " +
        "date_trunc('MILLENNIUM', timestamp '2011-2-3 10:11:12.100') as `m1`, " +
        "date_trunc('MILLENNIUM', timestamp '2000-11-3 10:11:12.100') as `m2`, " +
        "date_trunc('MILLENNIUM', timestamp '1983-05-18 10:11:12.100') as `m3`, " +
        "date_trunc('MILLENNIUM', timestamp '990-11-3 10:11:12.100') as `m4`, " +
        "date_trunc('MILLENNIUM', timestamp '0001-11-3 10:11:12.100') as `m5` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            new Timestamp(formatTimeStamp.parseDateTime("2001-01-01 00:00:00.0").getMillis()), // m1
            new Timestamp(formatTimeStamp.parseDateTime("1001-01-01 00:00:00.0").getMillis()), // m2
            new Timestamp(formatTimeStamp.parseDateTime("1001-01-01 00:00:00.0").getMillis()), // m3
            new Timestamp(formatTimeStamp.parseDateTime("0001-01-01 00:00:00.0").getMillis()), // m4
            new Timestamp(formatTimeStamp.parseDateTime("0001-01-01 00:00:00.0").getMillis())) // m5
        .go();
  }

  @Test
  public void dateTruncOnIntervalYear() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', interval '217-7' year(3) to month) as `second`, " +
        "date_trunc('MINUTE', interval '217-7' year(3) to month) as `minute`, " +
        "date_trunc('HOUR', interval '217-7' year(3) to month) as `hour`, " +
        "date_trunc('DAY', interval '217-7' year(3) to month) as `day`, " +
        "date_trunc('MONTH', interval '217-7' year(3) to month) as `month`, " +
        "date_trunc('YEAR', interval '217-7' year(3) to month) as `year`, " +
        "date_trunc('QUARTER', interval '217-7' year(3) to month) as `quarter`, " +
        "date_trunc('DECADE', interval '217-7' year(3) to month) as `decade`, " +
        "date_trunc('CENTURY', interval '217-7' year(3) to month) as `century`, " +
        "date_trunc('MILLENNIUM', interval '217-7' year(3) to month) as `millennium` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            new Period("P217Y7M").normalizedStandard(), // seconds
            new Period("P217Y7M").normalizedStandard(), // minute
            new Period("P217Y7M").normalizedStandard(), // hour
            new Period("P217Y7M").normalizedStandard(), // day
            new Period("P217Y7M").normalizedStandard(), // month
            new Period("P217Y").normalizedStandard(), // year
            new Period("P217Y6M").normalizedStandard(), // quarter
            new Period("P210Y").normalizedStandard(), // decade
            new Period("P200Y").normalizedStandard(), // century
            new Period("PT0S").normalizedStandard()) // millennium
        .go();
  }

  @Test
  public void dateTruncOnIntervalDay() throws Exception {
    final String query = "SELECT " +
        "date_trunc('SECOND', interval '200 10:20:30.123' day(3) to second) as `second`, " +
        "date_trunc('MINUTE', interval '200 10:20:30.123' day(3) to second) as `minute`, " +
        "date_trunc('HOUR', interval '200 10:20:30.123' day(3) to second) as `hour`, " +
        "date_trunc('DAY', interval '200 10:20:30.123' day(3) to second) as `day`, " +
        "date_trunc('MONTH', interval '200 10:20:30.123' day(3) to second) as `month`, " +
        "date_trunc('YEAR', interval '200 10:20:30.123' day(3) to second) as `year`, " +
        "date_trunc('QUARTER', interval '200 10:20:30.123' day(3) to second) as `quarter`, " +
        "date_trunc('DECADE', interval '200 10:20:30.123' day(3) to second) as `decade`, " +
        "date_trunc('CENTURY', interval '200 10:20:30.123' day(3) to second) as `century`, " +
        "date_trunc('MILLENNIUM', interval '200 10:20:30.123' day(3) to second) as `millennium` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "quarter", "decade", "century", "millennium")
        .baselineValues(
            new Period().plusDays(200).plusMillis(37230000), // seconds
            new Period().plusDays(200).plusMillis(37200000), // minute
            new Period().plusDays(200).plusMillis(36000000), // hour
            new Period().plusDays(200), // day
            new Period("PT0S"), // month
            new Period("PT0S"), // year
            new Period("PT0S"), // quarter
            new Period("PT0S"), // decade
            new Period("PT0S"), // century
            new Period("PT0S")) // millennium
        .go();
  }

  @Test
  public void testDateTrunc() throws Exception {
    String query = "select "
        + "date_trunc('MINUTE', time '2:30:21.5') as TIME1, "
        + "date_trunc('SECOND', time '2:30:21.5') as TIME2, "
        + "date_trunc('HOUR', timestamp '1991-05-05 10:11:12.100') as TS1, "
        + "date_trunc('SECOND', timestamp '1991-05-05 10:11:12.100') as TS2, "
        + "date_trunc('MONTH', date '2011-2-2') as DATE1, "
        + "date_trunc('YEAR', date '2011-2-2') as DATE2 "
        + "from cp.`employee.json` where employee_id < 2";

    Time time1 = new Time(formatTime.parseDateTime("2:30:00.0").getMillis());
    Time time2 = new Time(formatTime.parseDateTime("2:30:21.0").getMillis());
    Timestamp ts1 = new Timestamp(formatTimeStamp.parseDateTime("1991-05-05 10:00:00.0").getMillis());
    Timestamp ts2 = new Timestamp(formatTimeStamp.parseDateTime("1991-05-05 10:11:12.0").getMillis());
    Date date1 = new Date(formatDate.parseDateTime("2011-02-01").getMillis());
    Date date2 = new Date(formatDate.parseDateTime("2011-01-01").getMillis());

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("TIME1", "TIME2", "TS1", "TS2", "DATE1", "DATE2")
        .baselineValues(time1, time2, ts1, ts2, date1, date2)
        .go();
  }
}
