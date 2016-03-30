/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestExtractFunction2 extends BaseTestQuery {
  @Test
  public void extractOnTime() throws Exception {
    final String query = "SELECT " +
        "extract(SECOND FROM time '2:30:21.5') as `second`, " +
        "extract(MINUTE FROM time '2:30:21.5') as `minute`, " +
        "extract(HOUR FROM time '2:30:21.5') as `hour`, " +
        "extract(EPOCH FROM time '2:30:21.5') as `epoch` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "epoch")
        .baselineValues(
            21.5d, // seconds
            30L, // minute
            2L, // hour
            9021.5) // epoch
        .go();
  }

  @Test
  public void extractOnDate() throws Exception {
    final String query = "SELECT " +
        "extract(SECOND FROM date '2011-2-3') as `second`, " +
        "extract(MINUTE FROM date '2011-2-3') as `minute`, " +
        "extract(HOUR FROM date '2011-2-3') as `hour`, " +
        "extract(DAY FROM date '2011-2-3') as `day`, " +
        "extract(DOW  FROM date '2011-2-3') as `dow`, " +
        "extract(DOY  FROM date '2011-2-3') as `doy`, " +
        "extract(WEEK FROM date '2011-2-3') as `week`, " +
        "extract(MONTH FROM date '2011-2-3') as `month`, " +
        "extract(YEAR FROM date '2011-2-3') as `year`, " +
        "extract(EPOCH FROM date '2011-2-3') as `epoch`, " +
        "extract(QUARTER FROM date '2011-5-3') as `q1`, " +
        "extract(QUARTER FROM date '2011-7-13') as `q2`, " +
        "extract(QUARTER FROM date '2011-9-13') as `q3`, " +
        "extract(DECADE FROM date '2011-2-3') as `decade1`, " +
        "extract(DECADE FROM date '2072-2-3') as `decade2`, " +
        "extract(DECADE FROM date '1978-2-3') as `decade3`, " +
        "extract(CENTURY FROM date '2011-2-3') as c1, " +
        "extract(CENTURY FROM date '2000-2-3') as c2, " +
        "extract(CENTURY FROM date '1901-11-3') as c3, " +
        "extract(CENTURY FROM date '900-2-3') as c4, " +
        "extract(CENTURY FROM date '0001-1-3') as c5, " +
        "extract(MILLENNIUM FROM date '2011-2-3') as `m1`, " +
        "extract(MILLENNIUM FROM date '2000-11-3') as `m2`, " +
        "extract(MILLENNIUM FROM date '1983-05-18') as `m3`, " +
        "extract(MILLENNIUM FROM date '990-11-3') as `m4`, " +
        "extract(MILLENNIUM FROM date '0001-11-3') as `m5` " +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "dow", "doy", "week", "month" , "year", "epoch", "q1", "q2", "q3",
            "decade1", "decade2", "decade3", "c1", "c2", "c3", "c4", "c5", "m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            0d, // seconds
            0L, // minute
            0L, // hour
            3L, // day
            4L, // dow
            34L, // doy
            5L, // week
            2L, // month
            2011L, // year
            1296691200d, // epoch
            2L, // quarter-1
            3L, // quarter-2
            3L, // quarter-3
            201L, // decade-1
            207L, // decade-2
            197L, // decade-3
            21L, // century-1
            20L, // century-2
            20L, // century-3
            9L, // century-4
            1L, // century-5
            3L, // millennium-1
            2L, // millennium-2
            2L, // millennium-3
            1L, // millennium-4
            1L // millennium-5
        )
        .go();
  }

  @Test
  public void extractOnTimeStamp() throws Exception {
    final String query = "SELECT " +
        " extract(SECOND  FROM timestamp '2011-2-3 10:11:12.100') as `second`, " +
        " extract(MINUTE  FROM timestamp '2011-2-3 10:11:12.100') as `minute`, " +
        " extract(HOUR  FROM timestamp '2011-2-3 10:11:12.100') as `hour`, " +
        " extract(DAY  FROM timestamp '2011-2-3 10:11:12.100') as `day`, " +
        " extract(DOW  FROM timestamp '2011-2-3 10:11:12.100') as `dow`, " +
        " extract(DOY  FROM timestamp '2011-2-3 10:11:12.100') as `doy`, " +
        " extract(WEEK  FROM timestamp '2011-2-3 10:11:12.100') as `week`, " +
        " extract(MONTH  FROM timestamp '2011-2-3 10:11:12.100') as `month`, " +
        " extract(YEAR  FROM timestamp '2011-2-3 10:11:12.100') as `year`, " +
        " extract(EPOCH  FROM timestamp '2011-5-3 10:11:12.100') as `epoch`, " +
        " extract(QUARTER  FROM timestamp '2011-5-3 10:11:12.100') as `q1`, " +
        " extract(QUARTER  FROM timestamp '2011-7-13 10:11:12.100') as `q2`, " +
        " extract(QUARTER  FROM timestamp '2011-9-13 10:11:12.100') as `q3`, " +
        " extract(DECADE  FROM timestamp '2011-2-3 10:11:12.100') as `decade1`, " +
        " extract(DECADE  FROM timestamp '2072-2-3 10:11:12.100') as `decade2`, " +
        " extract(DECADE  FROM timestamp '1978-2-3 10:11:12.100') as `decade3`, " +
        " extract(CENTURY  FROM timestamp '2011-2-3 10:11:12.100') as c1, " +
        " extract(CENTURY  FROM timestamp '2000-2-3 10:11:12.100') as c2, " +
        " extract(CENTURY  FROM timestamp '1901-11-3 10:11:12.100') as c3, " +
        " extract(CENTURY  FROM timestamp '900-2-3 10:11:12.100') as c4, " +
        " extract(CENTURY  FROM timestamp '0001-1-3 10:11:12.100') as c5, " +
        " extract(MILLENNIUM  FROM timestamp '2011-2-3 10:11:12.100') as `m1`, " +
        " extract(MILLENNIUM  FROM timestamp '2000-11-3 10:11:12.100') as `m2`, " +
        " extract(MILLENNIUM  FROM timestamp '1983-05-18 10:11:12.100') as `m3`, " +
        " extract(MILLENNIUM  FROM timestamp '990-11-3 10:11:12.100') as `m4`, " +
        " extract(MILLENNIUM  FROM timestamp '0001-11-3 10:11:12.100') as `m5` " +
        "FROM sys.version";

    System.out.println(query);

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "dow", "doy", "week", "month", "year", "epoch", "q1", "q2", "q3", "decade1", "decade2", "decade3",
            "c1", "c2", "c3", "c4", "c5", "m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            12.1d, // seconds
            11L, // minute
            10L, // hour
            3L, // day
            4L, // dow
            34L, // doy
            5L, // week
            2L, // month
            2011L, // year
            1304417472.1d, // epoch
            2L, // quarter-1
            3L, // quarter-2
            3L, // quarter-3
            201L, // decade-1
            207L, // decade-2
            197L, // decade-3
            21L, // century-1
            20L, // century-2
            20L, // century-3
            9L, // century-4
            1L, // century-5
            3L, // millennium-1
            2L, // millennium-2
            2L, // millennium-3
            1L, // millennium-4
            1L // millennium-5
        )
        .go();
  }

  @Test
  public void extractOnIntervalYear() throws Exception {
    final String query = "SELECT  " +
        "  extract(SECOND  FROM interval '217-7' year(3) to month) as `second`," +
        "  extract(MINUTE  FROM interval '217-7' year(3) to month) as `minute`," +
        "  extract(HOUR  FROM interval '217-7' year(3) to month) as `hour`," +
        "  extract(DAY  FROM interval '217-7' year(3) to month) as `day`," +
        "  extract(MONTH  FROM interval '217-7' year(3) to month) as `month`," +
        "  extract(YEAR  FROM interval '217-7' year(3) to month) as `year`," +
        "  extract(EPOCH  FROM interval '217-7' year(3) to month) as `epoch`," +
        "  extract(QUARTER  FROM interval '217-7' year(3) to month) as `q1`," +
        "  extract(QUARTER  FROM interval '217-10' year(3) to month) as `q2`," +
        "  extract(QUARTER  FROM interval '217-2' year(3) to month) as `q3`," +
        "  extract(DECADE  FROM interval '217-7' year(3) to month) as `decade1`," +
        "  extract(DECADE  FROM interval '2-7' year(3) to month) as `decade2`," +
        "  extract(DECADE  FROM interval '778-7' year(3) to month) as `decade3`," +
        "  extract(CENTURY  FROM interval '234-7' year(3) to month) as `c1`," +
        "  extract(CENTURY  FROM interval '24-7' year(3) to month) as `c2`," +
        "  extract(CENTURY  FROM interval '643-7' year(3) to month) as `c3`," +
        "  extract(CENTURY  FROM interval '2330-7' year(4) to month) as `c4`," +
        "  extract(CENTURY  FROM interval '3452-7' year(4) to month) as `c5`," +
        "  extract(MILLENNIUM  FROM interval '3452-7' year(4) to month) as `m1`," +
        "  extract(MILLENNIUM  FROM interval '643-7' year(3) to month) as `m2`," +
        "  extract(MILLENNIUM  FROM interval '234-7' year(3) to month) as `m3`," +
        "  extract(MILLENNIUM  FROM interval '778-7' year(3) to month) as `m4`," +
        "  extract(MILLENNIUM  FROM interval '1998-7' year(4) to month) as `m5`" +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "epoch", "q1", "q2", "q3", "decade1", "decade2", "decade3",
            "c1", "c2", "c3", "c4", "c5", "m1", "m2", "m3", "m4", "m5")
        .baselineValues(
            0d, // seconds
            0L, // minute
            0L, // hour
            0L, // day
            7L, // month
            217L, // year
            6866208000d, // epoch
            3L, // quarter-1
            4L, // quarter-2
            1L, // quarter-3
            21L, // decade-1
            0L, // decade-2
            77L, // decade-3
            2L, // century-1
            0L, // century-2
            6L, // century-3
            23L, // century-4
            34L, // century-5
            3L, // millennium-1
            0L, // millennium-2
            0L, // millennium-3
            0L, // millennium-4
            1L // millennium-5
        )
        .go();
  }

  @Test
  public void extractOnIntervalDay() throws Exception {
    final String query = "SELECT  " +
        "  extract(SECOND  FROM interval '200 10:20:30.123' day(3) to second) as `second`, " +
        "  extract(MINUTE  FROM interval '200 10:20:30.123' day(3) to second) as `minute`, " +
        "  extract(HOUR  FROM interval '200 10:20:30.123' day(3) to second) as `hour`, " +
        "  extract(DAY  FROM interval '200 10:20:30.123' day(3) to second) as `day`, " +
        "  extract(MONTH  FROM interval '200 10:20:30.123' day(3) to second) as `month`, " +
        "  extract(YEAR  FROM interval '200 10:20:30.123' day(3) to second) as `year`, " +
        "  extract(EPOCH  FROM interval '200 10:20:30.123' day(3) to second) as `epoch`," +
        "  extract(QUARTER  FROM interval '200 10:20:30.123' day(3) to second) as `q1`," +
        "  extract(QUARTER  FROM interval '2000 10:20:30.123' day(4) to second) as `q2`," +
        "  extract(QUARTER  FROM interval '500 10:20:30.123' day(3) to second) as `q3`," +
        "  extract(DECADE  FROM interval '200 10:20:30.123' day(3) to second) as `decade1`," +
        "  extract(DECADE  FROM interval '200 10:20:30.123' day(3) to second) as `decade2`," +
        "  extract(DECADE  FROM interval '200 10:20:30.123' day(3) to second) as `decade3`," +
        "  extract(CENTURY  FROM interval '200 10:20:30.123' day(3) to second) as `c1`," +
        "  extract(CENTURY  FROM interval '200000 10:20:30.123' day(6) to second) as `c2`," +
        "  extract(MILLENNIUM  FROM interval '20000 10:20:30.123' day(5) to second) as `m1`" +
        "FROM sys.version";

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("second", "minute", "hour", "day", "month", "year", "epoch", "q1", "q2", "q3", "decade1", "decade2", "decade3",
            "c1", "c2", "m1")
        .baselineValues(
            30.123d, // seconds
            20L, // minute
            10L, // hour
            200L, // day
            0L, // month
            0L, // year
            17317230.123d, // epoch
            1L, // quarter-1
            1L, // quarter-2
            1L, // quarter-3
            0L, // decade-1
            0L, // decade-2
            0L, // decade-3
            0L, // century-1
            0L, // century-2
            0L // millennium-1
        )
        .go();
  }
}
