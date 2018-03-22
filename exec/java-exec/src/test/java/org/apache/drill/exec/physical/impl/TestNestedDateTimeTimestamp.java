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
import org.junit.Assert;
import org.junit.Test;

/**
 * For DRILL-6242, output for Date, Time, Timestamp should use different classes
 */
public class TestNestedDateTimeTimestamp extends BaseTestQuery {
    private static final String DATAFILE = "cp.`datetime.parquet`";

    /**
     * Data in the parquet file represents this equavilent JSON, but with typed data, time, and timestamps:
     * {
     *    "date" : "1970-01-11",
     *    "time" : "00:00:03.600",
     *    "timestamp" : "2018-03-21 20:30:07.364",
     *    "date_list" : [ "1970-01-11" ],
     *    "time_list" : [ "00:00:03.600" ],
     *    "timestamp_list" : [ "2018-03-21 20:30:07.364" ],
     *    "time_map" : {
     *      "date" : "1970-01-11",
     *      "time" : "00:00:03.600",
     *      "timestamp" : "2018-03-21 20:30:07.364"
     *    }
     *  }
     */
    @Test
    public void testNested() throws Exception {
      String query = String.format("select * from %s limit 1", DATAFILE);
      Map<String,Object> expectedRecord = new TreeMap<String,Object>();

      Date date = Date.valueOf("1970-01-11");
      Time time = new Time(Timestamp.valueOf("1970-01-01 00:00:03.600").getTime());
      Timestamp timestamp = Timestamp.valueOf("2018-03-21 20:30:07.364");
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
                "1970-01-11 | 00:00:03 | 2018-03-21 20:30:07.364 | [\"1970-01-11\"] | [\"00:00:03.600\"] | [\"2018-03-21 20:30:07.364\"] | {\"date\":\"1970-01-11\",\"time\":\"00:00:03.600\",\"timestamp\":\"2018-03-21 20:30:07.364\"}";

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
                .jsonBaselineFile("baseline_nested_datetime_extended.json")
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
                .jsonBaselineFile("baseline_nested_datetime_extended.json")
                .build()
                .run();
        } finally {
          test("drop table " + testName + "_parquet");
          test("alter session reset store.format ");
        }
    }
}
