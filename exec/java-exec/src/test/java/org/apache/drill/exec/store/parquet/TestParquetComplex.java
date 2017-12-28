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

import org.apache.drill.test.BaseTestQuery;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.drill.test.TestBuilder.mapOf;

public class TestParquetComplex extends BaseTestQuery {

  private static final String DATAFILE = "cp.`store/parquet/complex/complex.parquet`";

  @Test
  public void sort() throws Exception {
    String query = String.format("select * from %s order by amount", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void topN() throws Exception {
    String query = String.format("select * from %s order by amount limit 5", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline_sorted.json")
            .build()
            .run();
  }

  @Test
  public void hashJoin() throws Exception{
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void mergeJoin() throws Exception{
    test("alter session set `planner.enable_hashjoin` = false");
    String query = String.format("select t1.amount, t1.`date`, t1.marketing_info, t1.`time`, t1.trans_id, t1.trans_info, t1.user_info " +
            "from %s t1, %s t2 where t1.amount = t2.amount", DATAFILE, DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectAllColumns() throws Exception {
    String query = String.format("select amount, `date`, marketing_info, `time`, trans_id, trans_info, user_info from %s", DATAFILE);
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void selectMap() throws Exception {
    String query = "select marketing_info from cp.`store/parquet/complex/complex.parquet`";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline5.json")
            .build()
            .run();
  }

  @Test
  public void selectMapAndElements() throws Exception {
    String query = "select marketing_info, t.marketing_info.camp_id as camp_id, t.marketing_info.keywords[2] as keyword2 from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline6.json")
            .build()
            .run();
  }

  @Test
  public void selectMultiElements() throws Exception {
    String query = "select t.marketing_info.camp_id as camp_id, t.marketing_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline7.json")
            .build()
            .run();
  }

  @Test
  public void testStar() throws Exception {
    testBuilder()
            .sqlQuery("select * from cp.`store/parquet/complex/complex.parquet`")
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline.json")
            .build()
            .run();
  }

  @Test
  public void missingColumnInMap() throws Exception {
    String query = "select t.trans_info.keywords as keywords from cp.`store/parquet/complex/complex.parquet` t";
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline2.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void secondElementInMap() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords as keywords from %s t", DATAFILE);
    String[] columns = {"keywords"};
    testBuilder()
            .sqlQuery(query)
            .ordered()
            .jsonBaselineFile("store/parquet/complex/baseline3.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArray() throws Exception {
    String query = String.format("select t.`marketing_info`.keywords[0] as keyword0, t.`marketing_info`.keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test
  public void elementsOfArrayCaseInsensitive() throws Exception {
    String query = String.format("select t.`MARKETING_INFO`.keywords[0] as keyword0, t.`Marketing_Info`.Keywords[2] as keyword2 from %s t", DATAFILE);
    String[] columns = {"keyword0", "keyword2"};
    testBuilder()
            .sqlQuery(query)
            .unOrdered()
            .jsonBaselineFile("store/parquet/complex/baseline4.json")
            .baselineColumns(columns)
            .build()
            .run();
  }

  @Test //DRILL-3533
  public void notxistsField() throws Exception {
    String query = String.format("select t.`marketing_info`.notexists as notexists1,\n" +
                                        "t.`marketing_info`.camp_id as id,\n" +
                                        "t.`marketing_info.camp_id` as notexists2\n" +
                                  "from %s t", DATAFILE);
    String[] columns = {"notexists1", "id", "notexists2"};
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .jsonBaselineFile("store/parquet/complex/baseline8.json")
        .baselineColumns(columns)
        .build()
        .run();
  }

  @Test //DRILL-5971
  public void testComplexLogicalIntTypes() throws Exception {
    String query = String.format("select t.complextype as complextype,  " +
            "t.uint_64 as uint_64, t.uint_32 as uint_32, t.uint_16 as uint_16, t.uint_8 as uint_8,  " +
            "t.int_64 as int_64, t.int_32 as int_32, t.int_16 as int_16, t.int_8 as int_8  " +
            "from cp.`store/parquet/complex/logical_int_complex.parquet` t" );
    String[] columns = {"complextype", "uint_64", "uint_32", "uint_16", "uint_8", "int_64", "int_32", "int_16", "int_8" };
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(columns)
        .baselineValues(mapOf("a","a","b","b")  , 0L                   , 0           , 0        , 0       , 0L                    , 0            , 0       ,0       )
        .baselineValues(mapOf("a","a","b","b")  , -1L                  , -1          , -1       , -1      , -1L                   , -1           , -1      , -1     )
        .baselineValues(mapOf("a","a","b","b")  , 1L                   , 1           , 1        , 1       , -9223372036854775808L , 1            , 1       , 1      )
        .baselineValues(mapOf("a","a","b","b")  , 9223372036854775807L , 2147483647  , 65535    , 255     , 9223372036854775807L  , -2147483648  , -32768  , -128   )
        .build()
        .run();
  }

  @Test //DRILL-5971
  public void testComplexLogicalIntTypes2() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    byte[] bytesZeros = new byte[12];
    Arrays.fill(bytesOnes, (byte) 1);
    String query = String.format(
        " select " +
        " t.rowKey as rowKey, " +
        " t.StringTypes._UTF8 as _UTF8, " +
        " t.StringTypes._Enum as _Enum, " +
        " t.NumericTypes.Int32._INT32_RAW as _INT32_RAW, " +
        " t.NumericTypes.Int32._INT_8 as _INT_8, " +
        " t.NumericTypes.Int32._INT_16 as _INT_16, " +
        " t.NumericTypes.Int32._INT_32 as _INT_32, " +
        " t.NumericTypes.Int32._UINT_8 as _UINT_8, " +
        " t.NumericTypes.Int32._UINT_16 as _UINT_16, " +
        " t.NumericTypes.Int32._UINT_32 as _UINT_32, " +
        " t.NumericTypes.Int64._INT64_RAW as _INT64_RAW, " +
        " t.NumericTypes.Int64._INT_64 as _INT_64, " +
        " t.NumericTypes.Int64._UINT_64 as _UINT_64, " +
        " t.NumericTypes.DateTimeTypes._DATE_int32 as _DATE_int32, " +
        " t.NumericTypes.DateTimeTypes._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
        " t.NumericTypes.DateTimeTypes._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
        " t.NumericTypes.DateTimeTypes._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
        " t.NumericTypes.Int96._INT96_RAW as _INT96_RAW " +
        " from " +
        " cp.`store/parquet/complex/parquet_logical_types_complex.parquet` t " +
        " order by t.rowKey "
    );
    String[] columns = {
        "rowKey " ,
        "_UTF8" ,
        "_Enum" ,
        "_INT32_RAW" ,
        "_INT_8" ,
        "_INT_16" ,
        "_INT_32" ,
        "_UINT_8" ,
        "_UINT_16" ,
        "_UINT_32" ,
        "_INT64_RAW" ,
        "_INT_64" ,
        "_UINT_64" ,
        "_DATE_int32" ,
        "_TIME_MILLIS_int32" ,
        "_TIMESTAMP_MILLIS_int64" ,
        "_INTERVAL_fixed_len_byte_array_12" ,
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, new DateTime("5350-02-17"),
            new DateTime(1234567, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("1973-11-29T21:33:09.012"),
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, new DateTime("1969-12-31"),
            new DateTime(0xFFFFFFFF, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("2038-01-19T03:14:07.999"),
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, new DateTime("1970-01-01"),
            new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("1970-01-01T00:00:00.0"), new Period("PT0S"), bytesZeros)
        .build()
        .run();
  }

  @Test //DRILL-5971
  public void testComplexLogicalIntTypes3() throws Exception {
    byte[] bytes12 = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b' };
    byte[] bytesOnes = new byte[12];
    byte[] bytesZeros = new byte[12];
    Arrays.fill(bytesOnes, (byte) 1);
    String query = String.format(
        " select " +
            " t.rowKey as rowKey, " +
            " t.StringTypes._UTF8 as _UTF8, " +
            " t.StringTypes._Enum as _Enum, " +
            " t.NumericTypes.Int32._INT32_RAW as _INT32_RAW, " +
            " t.NumericTypes.Int32._INT_8 as _INT_8, " +
            " t.NumericTypes.Int32._INT_16 as _INT_16, " +
            " t.NumericTypes.Int32._INT_32 as _INT_32, " +
            " t.NumericTypes.Int32._UINT_8 as _UINT_8, " +
            " t.NumericTypes.Int32._UINT_16 as _UINT_16, " +
            " t.NumericTypes.Int32._UINT_32 as _UINT_32, " +
            " t.NumericTypes.Int64._INT64_RAW as _INT64_RAW, " +
            " t.NumericTypes.Int64._INT_64 as _INT_64, " +
            " t.NumericTypes.Int64._UINT_64 as _UINT_64, " +
            " t.NumericTypes.DateTimeTypes._DATE_int32 as _DATE_int32, " +
            " t.NumericTypes.DateTimeTypes._TIME_MILLIS_int32 as _TIME_MILLIS_int32, " +
            " t.NumericTypes.DateTimeTypes._TIMESTAMP_MILLIS_int64 as _TIMESTAMP_MILLIS_int64, " +
            " t.NumericTypes.DateTimeTypes._INTERVAL_fixed_len_byte_array_12 as _INTERVAL_fixed_len_byte_array_12, " +
            " t.NumericTypes.Int96._INT96_RAW as _INT96_RAW " +
            " from " +
            " cp.`store/parquet/complex/parquet_logical_types_complex_nullable.parquet` t " +
            " order by t.rowKey "
    );
    String[] columns = {
        "rowKey " ,
        "_UTF8" ,
        "_Enum" ,
        "_INT32_RAW" ,
        "_INT_8" ,
        "_INT_16" ,
        "_INT_32" ,
        "_UINT_8" ,
        "_UINT_16" ,
        "_UINT_32" ,
        "_INT64_RAW" ,
        "_INT_64" ,
        "_UINT_64" ,
        "_DATE_int32" ,
        "_TIME_MILLIS_int32" ,
        "_TIMESTAMP_MILLIS_int64" ,
        "_INTERVAL_fixed_len_byte_array_12" ,
        "_INT96_RAW"

    };
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(columns)
        .baselineValues(1, "UTF8 string1", "RANDOM_VALUE", 1234567, 123, 12345, 1234567, 123, 1234, 1234567,
            1234567890123456L, 1234567890123456L, 1234567890123456L, new DateTime("5350-02-17"),
            new DateTime(1234567, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("1973-11-29T21:33:09.012"),
            new Period().plusMonths(875770417).plusDays(943142453).plusMillis(1650536505),
            bytes12)
        .baselineValues(2, "UTF8 string2", "MAX_VALUE", 2147483647, 127, 32767, 2147483647, 255, 65535, -1,
            9223372036854775807L, 9223372036854775807L, -1L, new DateTime("1969-12-31"),
            new DateTime(0xFFFFFFFF, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("2038-01-19T03:14:07.999"),
            new Period().plusMonths(16843009).plusDays(16843009).plusMillis(16843009),
            bytesOnes)
        .baselineValues(3, "UTF8 string3", "MIN_VALUE", -2147483648, -128, -32768, -2147483648, 0, 0, 0,
            -9223372036854775808L, -9223372036854775808L, 0L, new DateTime("1970-01-01"),
            new DateTime(0, DateTimeZone.UTC).withZoneRetainFields(DateTimeZone.getDefault()),
            new DateTime("1970-01-01T00:00:00.0"), new Period("PT0S"), bytesZeros)
        .baselineValues(4, null, null, null, null, null, null, null, null, null, null, null, null, null,
            null, null, null, null)
        .build().run();
  }

}
