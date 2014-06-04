/**
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
package org.apache.drill.jdbc.test;

import java.lang.Exception;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.store.hive.HiveTestDataGenerator;
import org.apache.drill.jdbc.Driver;
import org.apache.drill.jdbc.JdbcTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;

public class TestFunctionsQuery {

  public static final String WORKING_PATH;
  static{
    Driver.load();
    WORKING_PATH = Paths.get("").toAbsolutePath().toString();

  }
  @Test
  public void testAbsDecimalFunction() throws Exception{
    String query = String.format("SELECT abs(cast('1234.4567' as decimal(9, 5))) as DEC9_ABS_1, " +
        "abs(cast('-1234.4567' as decimal(9, 5))) DEC9_ABS_2, " +
        "abs(cast('99999912399.4567' as decimal(18, 5))) DEC18_ABS_1, " +
        "abs(cast('-99999912399.4567' as decimal(18, 5))) DEC18_ABS_2, " +
        "abs(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_ABS_1, " +
        "abs(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_ABS_2, " +
        "abs(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_ABS_1, " +
        "abs(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_ABS_2 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_ABS_1=1234.45670; " +
                "DEC9_ABS_2=1234.45670; " +
                "DEC18_ABS_1=99999912399.45670; " +
                "DEC18_ABS_2=99999912399.45670; " +
                "DEC28_ABS_1=12345678912345678912.45670; " +
                "DEC28_ABS_2=12345678912345678912.45670; " +
                "DEC38_ABS_1=1234567891234567891234567891234567891.4; " +
                "DEC38_ABS_2=1234567891234567891234567891234567891.4\n"
        );

  }

  @Test
  public void testCeilDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "ceil(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "ceil(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "ceil(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "ceil(cast('-1234.000' as decimal(9, 5))) as DEC9_4, " +
        "ceil(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "ceil(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "ceil(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "ceil(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "ceil(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "ceil(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "ceil(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "ceil(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "ceil(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "ceil(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "ceil(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "ceil(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "ceil(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "ceil(cast('-1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_5 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1235; " +
                "DEC9_2=1234; " +
                "DEC9_3=-1234; " +
                "DEC9_4=-1234; " +
                "DEC18_1=99999912400; " +
                "DEC18_2=99999912399; " +
                "DEC18_3=-99999912399; " +
                "DEC18_4=-99999912399; " +
                "DEC28_1=12345678912345678913; " +
                "DEC28_2=1000000000000000000; " +
                "DEC28_3=12345678912345678912; " +
                "DEC28_4=-12345678912345678912; " +
                "DEC28_5=-12345678912345678912; " +
                "DEC38_1=1234567891234567891234567891234567892; " +
                "DEC38_2=1000000000000000000000000000000000000; " +
                "DEC38_3=1234567891234567891234567891234567891; " +
                "DEC38_4=-1234567891234567891234567891234567891; " +
                "DEC38_5=-1234567891234567891234567891234567891\n"
        );

  }

  @Test
  public void testFloorDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "floor(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "floor(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "floor(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "floor(cast('-1234.000' as decimal(9, 5))) as DEC9_4, " +
        "floor(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "floor(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "floor(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "floor(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "floor(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "floor(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "floor(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "floor(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "floor(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "floor(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "floor(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "floor(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "floor(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "floor(cast('-999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_5 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1234; " +
                "DEC9_2=1234; " +
                "DEC9_3=-1235; " +
                "DEC9_4=-1234; " +
                "DEC18_1=99999912399; " +
                "DEC18_2=99999912399; " +
                "DEC18_3=-99999912400; " +
                "DEC18_4=-99999912399; " +
                "DEC28_1=12345678912345678912; " +
                "DEC28_2=999999999999999999; " +
                "DEC28_3=12345678912345678912; " +
                "DEC28_4=-12345678912345678913; " +
                "DEC28_5=-12345678912345678912; " +
                "DEC38_1=1234567891234567891234567891234567891; " +
                "DEC38_2=999999999999999999999999999999999999; " +
                "DEC38_3=1234567891234567891234567891234567891; " +
                "DEC38_4=-1234567891234567891234567891234567892; " +
                "DEC38_5=-1000000000000000000000000000000000000\n"
        );
  }

  @Test
  public void testTruncateDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "trunc(cast('1234.4567' as decimal(9, 5))) as DEC9_1, " +
        "trunc(cast('1234.0000' as decimal(9, 5))) as DEC9_2, " +
        "trunc(cast('-1234.4567' as decimal(9, 5))) as DEC9_3, " +
        "trunc(cast('0.111' as decimal(9, 5))) as DEC9_4, " +
        "trunc(cast('99999912399.4567' as decimal(18, 5))) DEC18_1, " +
        "trunc(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "trunc(cast('-99999912399.4567' as decimal(18, 5))) DEC18_3, " +
        "trunc(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "trunc(cast('12345678912345678912.4567' as decimal(28, 5))) DEC28_1, " +
        "trunc(cast('999999999999999999.4567' as decimal(28, 5))) DEC28_2, " +
        "trunc(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "trunc(cast('-12345678912345678912.4567' as decimal(28, 5))) DEC28_4, " +
        "trunc(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "trunc(cast('1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_1, " +
        "trunc(cast('999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_2, " +
        "trunc(cast('1234567891234567891234567891234567891.0' as decimal(38, 1))) DEC38_3, " +
        "trunc(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_4, " +
        "trunc(cast('-999999999999999999999999999999999999.4' as decimal(38, 1))) DEC38_5 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1234; " +
                "DEC9_2=1234; " +
                "DEC9_3=-1234; " +
                "DEC9_4=0; " +
                "DEC18_1=99999912399; " +
                "DEC18_2=99999912399; " +
                "DEC18_3=-99999912399; " +
                "DEC18_4=-99999912399; " +
                "DEC28_1=12345678912345678912; " +
                "DEC28_2=999999999999999999; " +
                "DEC28_3=12345678912345678912; " +
                "DEC28_4=-12345678912345678912; " +
                "DEC28_5=-12345678912345678912; " +
                "DEC38_1=1234567891234567891234567891234567891; " +
                "DEC38_2=999999999999999999999999999999999999; " +
                "DEC38_3=1234567891234567891234567891234567891; " +
                "DEC38_4=-1234567891234567891234567891234567891; " +
                "DEC38_5=-999999999999999999999999999999999999\n"
        );
  }

  @Test
  public void testTruncateWithParamDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "trunc(cast('1234.4567' as decimal(9, 5)), 2) as DEC9_1, " +
        "trunc(cast('1234.45' as decimal(9, 2)), 4) as DEC9_2, " +
        "trunc(cast('-1234.4567' as decimal(9, 5)), 0) as DEC9_3, " +
        "trunc(cast('0.111' as decimal(9, 5)), 2) as DEC9_4, " +
        "trunc(cast('99999912399.4567' as decimal(18, 5)), 2) DEC18_1, " +
        "trunc(cast('99999912399.0000' as decimal(18, 5)), 2) DEC18_2, " +
        "trunc(cast('-99999912399.45' as decimal(18, 2)), 6) DEC18_3, " +
        "trunc(cast('-99999912399.0000' as decimal(18, 5)), 4) DEC18_4, " +
        "trunc(cast('12345678912345678912.4567' as decimal(28, 5)), 1) DEC28_1, " +
        "trunc(cast('999999999999999999.456' as decimal(28, 3)), 6) DEC28_2, " +
        "trunc(cast('12345678912345678912.0000' as decimal(28, 5)), 2) DEC28_3, " +
        "trunc(cast('-12345678912345678912.45' as decimal(28, 2)), 0) DEC28_4, " +
        "trunc(cast('-12345678912345678912.0000' as decimal(28, 5)), 1) DEC28_5, " +
        "trunc(cast('999999999.123456789' as decimal(38, 9)), 7) DEC38_1, " +
        "trunc(cast('999999999.4' as decimal(38, 1)), 8) DEC38_2, " +
        "trunc(cast('999999999.1234' as decimal(38, 4)), 12) DEC38_3, " +
        "trunc(cast('-123456789123456789.4' as decimal(38, 1)), 10) DEC38_4, " +
        "trunc(cast('-999999999999999999999999999999999999.4' as decimal(38, 1)), 1) DEC38_5 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1234.45; " +
                "DEC9_2=1234.4500; " +
                "DEC9_3=-1234; " +
                "DEC9_4=0.11; " +
                "DEC18_1=99999912399.45; " +
                "DEC18_2=99999912399.00; " +
                "DEC18_3=-99999912399.450000; " +
                "DEC18_4=-99999912399.0000; " +
                "DEC28_1=12345678912345678912.4; " +
                "DEC28_2=999999999999999999.456000; " +
                "DEC28_3=12345678912345678912.00; " +
                "DEC28_4=-12345678912345678912; " +
                "DEC28_5=-12345678912345678912.0; " +
                "DEC38_1=999999999.1234567; " +
                "DEC38_2=999999999.40000000; " +
                "DEC38_3=999999999.123400000000; " +
                "DEC38_4=-123456789123456789.4000000000; " +
                "DEC38_5=-999999999999999999999999999999999999.4\n"
        );
  }

  @Test
  public void testRoundDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "round(cast('1234.5567' as decimal(9, 5))) as DEC9_1, " +
        "round(cast('1234.1000' as decimal(9, 5))) as DEC9_2, " +
        "round(cast('-1234.5567' as decimal(9, 5))) as DEC9_3, " +
        "round(cast('-1234.1234' as decimal(9, 5))) as DEC9_4, " +
        "round(cast('99999912399.9567' as decimal(18, 5))) DEC18_1, " +
        "round(cast('99999912399.0000' as decimal(18, 5))) DEC18_2, " +
        "round(cast('-99999912399.5567' as decimal(18, 5))) DEC18_3, " +
        "round(cast('-99999912399.0000' as decimal(18, 5))) DEC18_4, " +
        "round(cast('12345678912345678912.5567' as decimal(28, 5))) DEC28_1, " +
        "round(cast('999999999999999999.5567' as decimal(28, 5))) DEC28_2, " +
        "round(cast('12345678912345678912.0000' as decimal(28, 5))) DEC28_3, " +
        "round(cast('-12345678912345678912.5567' as decimal(28, 5))) DEC28_4, " +
        "round(cast('-12345678912345678912.0000' as decimal(28, 5))) DEC28_5, " +
        "round(cast('999999999999999999999999999.5' as decimal(38, 1))) DEC38_1, " +
        "round(cast('99999999.512345678123456789' as decimal(38, 18))) DEC38_2, " +
        "round(cast('999999999999999999999999999999999999.5' as decimal(38, 1))) DEC38_3, " +
        "round(cast('1234567891234567891234567891234567891.2' as decimal(38, 1))) DEC38_4, " +
        "round(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1))) DEC38_5, " +
        "round(cast('-999999999999999999999999999999999999.9' as decimal(38, 1))) DEC38_6 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1235; " +
                "DEC9_2=1234; " +
                "DEC9_3=-1235; " +
                "DEC9_4=-1234; " +
                "DEC18_1=99999912400; " +
                "DEC18_2=99999912399; " +
                "DEC18_3=-99999912400; " +
                "DEC18_4=-99999912399; " +
                "DEC28_1=12345678912345678913; " +
                "DEC28_2=1000000000000000000; " +
                "DEC28_3=12345678912345678912; " +
                "DEC28_4=-12345678912345678913; " +
                "DEC28_5=-12345678912345678912; " +
                "DEC38_1=1000000000000000000000000000; " +
                "DEC38_2=100000000; " +
                "DEC38_3=1000000000000000000000000000000000000; " +
                "DEC38_4=1234567891234567891234567891234567891; " +
                "DEC38_5=-1234567891234567891234567891234567891; " +
                "DEC38_6=-1000000000000000000000000000000000000\n"
        );
  }

  @Test
  public void testRoundWithScaleDecimalFunction() throws Exception {
    String query = String.format("SELECT " +
        "round(cast('1234.5567' as decimal(9, 5)), 3) as DEC9_1, " +
        "round(cast('1234.1000' as decimal(9, 5)), 2) as DEC9_2, " +
        "round(cast('-1234.5567' as decimal(9, 5)), 4) as DEC9_3, " +
        "round(cast('-1234.1234' as decimal(9, 5)), 3) as DEC9_4, " +
        "round(cast('-1234.1234' as decimal(9, 2)), 4) as DEC9_5, " +
        "round(cast('99999912399.9567' as decimal(18, 5)), 3) DEC18_1, " +
        "round(cast('99999912399.0000' as decimal(18, 5)), 2) DEC18_2, " +
        "round(cast('-99999912399.5567' as decimal(18, 5)), 2) DEC18_3, " +
        "round(cast('-99999912399.0000' as decimal(18, 5)), 0) DEC18_4, " +
        "round(cast('12345678912345678912.5567' as decimal(28, 5)), 2) DEC28_1, " +
        "round(cast('999999999999999999.5567' as decimal(28, 5)), 1) DEC28_2, " +
        "round(cast('12345678912345678912.0000' as decimal(28, 5)), 8) DEC28_3, " +
        "round(cast('-12345678912345678912.5567' as decimal(28, 5)), 3) DEC28_4, " +
        "round(cast('-12345678912345678912.0000' as decimal(28, 5)), 0) DEC28_5, " +
        "round(cast('999999999999999999999999999.5' as decimal(38, 1)), 1) DEC38_1, " +
        "round(cast('99999999.512345678923456789' as decimal(38, 18)), 9) DEC38_2, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 9) DEC38_3, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 11) DEC38_4, " +
        "round(cast('999999999.9999999995678' as decimal(38, 18)), 21) DEC38_5, " +
        "round(cast('-1234567891234567891234567891234567891.4' as decimal(38, 1)), 1) DEC38_6, " +
        "round(cast('-999999999999999999999999999999999999.9' as decimal(38, 1)), 0) DEC38_7 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_1=1234.557; " +
                "DEC9_2=1234.10; " +
                "DEC9_3=-1234.5567; " +
                "DEC9_4=-1234.123; " +
                "DEC9_5=-1234.1200; " +
                "DEC18_1=99999912399.957; " +
                "DEC18_2=99999912399.00; " +
                "DEC18_3=-99999912399.56; " +
                "DEC18_4=-99999912399; " +
                "DEC28_1=12345678912345678912.56; " +
                "DEC28_2=999999999999999999.6; " +
                "DEC28_3=12345678912345678912.00000000; " +
                "DEC28_4=-12345678912345678912.557; " +
                "DEC28_5=-12345678912345678912; " +
                "DEC38_1=999999999999999999999999999.5; " +
                "DEC38_2=99999999.512345679; " +
                "DEC38_3=1000000000.000000000; " +
                "DEC38_4=999999999.99999999957; " +
                "DEC38_5=999999999.999999999567800000000; " +
                "DEC38_6=-1234567891234567891234567891234567891.4; " +
                "DEC38_7=-1000000000000000000000000000000000000\n"
        );
  }

  @Test
  public void testToCharFunction() throws Exception {
    String query = String.format("SELECT " +
        "to_char(1234.5567, '#,###.##') as FLOAT8_1, " +
        "to_char(1234.5, '$#,###.00') as FLOAT8_2, " +
        "to_char(cast('1234.5567' as decimal(9, 5)), '#,###.##') as DEC9_1, " +
        "to_char(cast('99999912399.9567' as decimal(18, 5)), '#.#####') DEC18_1, " +
        "to_char(cast('12345678912345678912.5567' as decimal(28, 5)), '#,###.#####') DEC28_1, " +
        "to_char(cast('999999999999999999999999999.5' as decimal(38, 1)), '#.#') DEC38_1 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "FLOAT8_1=1,234.56; " +
                "FLOAT8_2=$1,234.50; " +
                "DEC9_1=1,234.56; " +
                "DEC18_1=99999912399.9567; " +
                "DEC28_1=12,345,678,912,345,678,912.5567; " +
                "DEC38_1=999999999999999999999999999.5\n"
        );
  }


  @Test
  public void testConcatFunction() throws Exception {
    String query = String.format("SELECT " +
        "concat('1234', ' COL_VALUE ', R_REGIONKEY, ' - STRING') as STR_1 " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "STR_1=1234 COL_VALUE 0 - STRING\n"
        );
  }

  @Test
  @Ignore
  public void testTimeStampConstant() throws Exception {
    String query = String.format("SELECT " +
        "timestamp '2008-2-23 12:23:23' as TS " +
        "FROM dfs.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "TS=2008-02-23 12:23:23.0\n"
        );
  }

  @Test
  public void testIntMinToDecimal() throws Exception {
    String query = "select cast((employee_id - employee_id + -2147483648) as decimal(28, 2)) as DEC_28," +
                   "cast((employee_id - employee_id + -2147483648) as decimal(18, 2)) as DEC_18 from " +
                   "cp.`employee.json` limit 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC_28=-2147483648.00; " +
            "DEC_18=-2147483648.00\n"
        );
  }

  @Test
  public void testHashFunctions() throws Exception {
    String query = "select hash(cast(hire_date as date)), hash(cast(employee_id as decimal(9, 2))), hash(cast(employee_id as decimal(38, 11)))" +
                   "from cp.`employee.json` limit 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query);
  }

  @Test
  public void testDecimalAddConstant() throws Exception {
    String query = "select (cast('-1' as decimal(38, 3)) + cast (employee_id as decimal(38, 3))) as CNT " +
        "from cp.`employee.json` where employee_id <= 4";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "CNT=0.000\n" +
            "CNT=1.000\n" +
            "CNT=3.000\n");
  }

  @Test
  public void testDecimalAddIntConstant() throws Exception {
    String query = "select 1 + cast(employee_id as decimal(9, 3)) as DEC_9 , 1 + cast(employee_id as decimal(38, 5)) as DEC_38 " +
        "from cp.`employee.json` where employee_id <= 2";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC_9=2.000; " +
            "DEC_38=2.00000\n" +
            "DEC_9=3.000; " +
            "DEC_38=3.00000\n");
  }

  @Test
  public void testSignFunction() throws Exception {
    String query = "select sign(cast('1.23' as float)) as SIGN_FLOAT, sign(-1234.4567) as SIGN_DOUBLE, sign(23) as SIGN_INT " +
        "from cp.`employee.json` where employee_id < 2";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "SIGN_FLOAT=1; " +
            "SIGN_DOUBLE=-1; " +
            "SIGN_INT=1\n");
  }

  @Test
  public void testToTimeStamp() throws Exception {
    String query = "select to_timestamp(cast('800120400.12312' as decimal(38, 5))) as DEC38_TS, to_timestamp(200120400) as INT_TS " +
        "from cp.`employee.json` where employee_id < 2";

    DateTime result1 = new DateTime(800120400123l);
    DateTime result2 = new DateTime(200120400000l);
    DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC38_TS=" + f.print(result1)+ "; " +
            "INT_TS=" + f.print(result2) + "\n");
  }
}
