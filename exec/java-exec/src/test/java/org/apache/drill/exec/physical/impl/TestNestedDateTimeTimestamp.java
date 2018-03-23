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
package org.apache.drill.exec.physical.impl;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.test.BaseTestQuery;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

/**
 * For DRILL-6242, output for Date, Time, Timestamp should use different classes
 */
public class TestNestedDateTimeTimestamp extends BaseTestQuery {
    private static final String DATAFILE = "cp.`datetime.parquet`";
    private static final Map<String,Object> expectedRecord = new TreeMap<String,Object>();

    static {
        /**
         * Data in the parquet file represents this equavilent JSON, but with typed data, time, and timestamps:
         * {
         *    "date" : "1970-01-11",
         *    "time" : "00:00:03.600",
         *    "timestamp" : "2018-03-23T17:40:52.123Z",
         *    "date_list" : [ "1970-01-11" ],
         *    "time_list" : [ "00:00:03.600" ],
         *    "timestamp_list" : [ "2018-03-23T17:40:52.123Z" ],
         *    "time_map" : {
         *      "date" : "1970-01-11",
         *      "time" : "00:00:03.600",
         *      "timestamp" : "2018-03-23T17:40:52.123Z"
         *    }
         *  }
         *
         * Note that when the above data is read in to Drill, Drill modifies the timestamp
         * to local time zone, and preserving the <date> and <time> values.  This effectively
         * changes the timestamp, if the time zone is not UTC.
         */

        Date date = Date.valueOf("1970-01-11");
        Time time = new Time(Timestamp.valueOf("1970-01-01 00:00:03.600").getTime());
        Timestamp timestamp = Timestamp.valueOf("2018-03-23 17:40:52.123");
        expectedRecord.put("`date`", date);
        expectedRecord.put("`time`", time);
        expectedRecord.put("`timestamp`", timestamp);
        expectedRecord.put("`date_list`", Arrays.asList(date));
        expectedRecord.put("`time_list`", Arrays.asList(time));
        expectedRecord.put("`timestamp_list`", Arrays.asList(timestamp));
        Map<String,Object> nestedMap = new TreeMap<String,Object>();
        nestedMap.put("date", date);
        nestedMap.put("time", time);
        nestedMap.put("timestamp", timestamp);

        expectedRecord.put("`time_map`", nestedMap);
    }


    /**
     * Test reading of from the parquet file that contains nested time, date, and timestamp
     */
    @Test
    public void testNested() throws Exception {
      String query = String.format("select * from %s limit 1", DATAFILE);
      testBuilder()
              .sqlQuery(query)
              .ordered()
              .baselineRecords(Arrays.asList(expectedRecord))
              .build()
              .run();
    }

    /**
     * Test the textual display to make sure it is consistent with actual JSON output
     */
    @Test
    public void testNestedDateTimePrint() throws Exception {
        List<QueryDataBatch> resultList = testSqlWithResults(String.format("select * from %s limit 1", DATAFILE));
        String actual = getResultString(resultList, " | ");

        final String expected =
                "date | time | timestamp | date_list | time_list | timestamp_list | time_map\n" +
                "1970-01-11 | 00:00:03 | 2018-03-23 17:40:52.123 | [\"1970-01-11\"] | [\"00:00:03.600\"] | [\"2018-03-23 17:40:52.123\"] | {\"date\":\"1970-01-11\",\"time\":\"00:00:03.600\",\"timestamp\":\"2018-03-23 17:40:52.123\"}";

        Assert.assertEquals(expected.trim(), actual.trim());
    }

    /**
     * Test the json output is consistent as before
     */
    @Test
    public void testNestedDateTimeCTASJson() throws Exception {
        String query = String.format("select * from %s limit 1", DATAFILE);
        String testName = "ctas_nested_datetime";
        try {
            test("alter session set store.format = 'json'");
            test("alter session set store.json.extended_types = false");
            test("use dfs.tmp");
            test("create table " + testName + "_json as " + query);

            final String readQuery = "select * from `" + testName + "_json` t1 ";

            testBuilder()
                .sqlQuery(readQuery)
                .ordered()
                .jsonBaselineFile("baseline_nested_datetime.json")
                .build()
                .run();
        } finally {
          test("drop table " + testName + "_json");
          test("alter session reset store.format ");
          test("alter session reset store.json.extended_types ");
        }
    }

    /**
     * Test the extended json output is consistent as before
     */
    @Test
    public void testNestedDateTimeCTASExtendedJson() throws Exception {
        String query = String.format("select * from %s limit 1", DATAFILE);
        String testName = "ctas_nested_datetime_extended";
        try {
            test("alter session set store.format = 'json'");
            test("alter session set store.json.extended_types = true");
            test("use dfs.tmp");
            test("create table " + testName + "_json as " + query);

            final String readQuery = "select * from `" + testName + "_json` t1 ";

            testBuilder()
                .sqlQuery(readQuery)
                .ordered()
                .jsonBaselineFile("datetime.parquet")
                .build()
                .run();
        } finally {
          test("drop table " + testName + "_json");
          test("alter session reset store.format ");
          test("alter session reset store.json.extended_types ");
        }
    }

    /**
     * Test parquet output is consistent as before
     */
    @Test
    public void testNestedDateTimeCTASParquet() throws Exception {
        String query = String.format("select * from %s limit 1", DATAFILE);
        String testName = "ctas_nested_datetime_extended";
        try {
            test("alter session set store.format = 'parquet'");
            test("use dfs.tmp");
            test("create table " + testName + "_parquet as " + query);

            final String readQuery = "select * from `" + testName + "_parquet` t1 ";

            testBuilder()
                .sqlQuery(readQuery)
                .ordered()
                .jsonBaselineFile("datetime.parquet")
                .build()
                .run();
        } finally {
          test("drop table " + testName + "_parquet");
          test("alter session reset store.format ");
        }
    }

    /**
     * Testing time zone change and revert
     */
    @Test
    public void testTimeZoneChangeAndReverse() throws Exception {
        long timeMillis[] = new long[]{864000000L, 3600L, 1521826852123L};

        for (int i = 0 ; i < timeMillis.length ; i++) {
            DateTime time1 = new org.joda.time.DateTime(timeMillis[i], org.joda.time.DateTimeZone.UTC);
            DateTime time2 = new DateTime(timeMillis[i], org.joda.time.DateTimeZone.UTC).withZoneRetainFields(org.joda.time.DateTimeZone.getDefault());
            DateTime time3 = new DateTime(time2.getMillis()).withZoneRetainFields(org.joda.time.DateTimeZone.UTC);

            Assert.assertEquals(time1.toString(), time3.toString());
            Assert.assertEquals(time1.toString().substring(0,23), time2.toString().substring(0,23));

            System.out.println("time1 = " + time1 + ", time2 = " + time2 + ", time3 = " + time3);
            System.out.println("  time1 = " + time1.toString().substring(0,23) + "\n  time2 = " + time2.toString().substring(0,23) + "\n  time3 = " + time3.toString().substring(0,23));

        }
    }
}
