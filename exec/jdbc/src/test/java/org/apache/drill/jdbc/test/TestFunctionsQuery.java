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

import java.nio.file.Paths;

import org.apache.drill.jdbc.Driver;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Ignore;
import org.junit.Test;

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
  public void testTruncateWithParamFunction() throws Exception {
    String query = String.format("SELECT " +
      "trunc(1234.4567, 2) as T_1, " +
      "trunc(-1234.4567, 2) as T_2, " +
      "trunc(1234.4567, -2) as T_3, " +
      "trunc(-1234.4567, -2) as T_4, " +
      "trunc(1234, 4) as T_5, " +
      "trunc(-1234, 4) as T_6, " +
      "trunc(1234, -4) as T_7, " +
      "trunc(-1234, -4) as T_8, " +
      "trunc(8124674407369523212, 0) as T_9, " +
      "trunc(81246744073695.395, 1) as T_10 " +
      "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
      .sql(query)
      .returns(
          "T_1=1234.45; " +
              "T_2=-1234.45; " +
              "T_3=1200.0; " +
              "T_4=-1200.0; " +
              "T_5=1234.0; " +
              "T_6=-1234.0; " +
              "T_7=0.0; " +
              "T_8=0.0; " +
              "T_9=8.1246744073695232E18; " +
              "T_10=8.12467440736953E13\n"
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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
  public void testRoundWithParamFunction() throws Exception {
    String query = String.format("SELECT " +
      "round(1234.4567, 2) as T_1, " +
      "round(-1234.4567, 2) as T_2, " +
      "round(1234.4567, -2) as T_3, " +
      "round(-1234.4567, -2) as T_4, " +
      "round(1234, 4) as T_5, " +
      "round(-1234, 4) as T_6, " +
      "round(1234, -4) as T_7, " +
      "round(-1234, -4) as T_8, " +
      "round(8124674407369523212, -4) as T_9, " +
      "round(81246744073695.395, 1) as T_10 " +
      "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
      .sql(query)
      .returns(
          "T_1=1234.46; " +
              "T_2=-1234.46; " +
              "T_3=1200.0; " +
              "T_4=-1200.0; " +
              "T_5=1234.0; " +
              "T_6=-1234.0; " +
              "T_7=0.0; " +
              "T_8=0.0; " +
              "T_9=8.1246744073695201E18; " +
              "T_10=8.12467440736954E13\n"
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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

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
        "FROM dfs_test.`%s/../../sample-data/region.parquet` limit 1", WORKING_PATH);

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "TS=2008-02-23 12:23:23.0\n"
        );
  }

  @Test
  public void testNullConstantsTimeTimeStampAndDate() throws Exception {
    JdbcAssert.withNoDefaultSchema()
        .sql("SELECT CAST(NULL AS TIME) AS t, " +
            "CAST(NULL AS TIMESTAMP) AS ts, " +
            "CAST(NULL AS DATE) AS d " +
            "FROM cp.`region.json` LIMIT 1")
        .returns("t=null; ts=null; d=null");
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
    String query = "select (cast('-1' as decimal(37, 3)) + cast (employee_id as decimal(37, 3))) as CNT " +
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
    String query = "select 1 + cast(employee_id as decimal(9, 3)) as DEC_9 , 1 + cast(employee_id as decimal(37, 5)) as DEC_38 " +
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

  @Ignore
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
    JdbcAssert.withNoDefaultSchema()
    .sql(query)
    .returns(
        "TIME1=02:30:00; " +
        "TIME2=02:30:21; " +
        "TS1=1991-05-05 10:00:00.0; " +
        "TS2=1991-05-05 10:11:12.0; " +
        "DATE1=2011-02-01; " +
        "DATE2=2011-01-01\n");
  }

  @Test
  @Ignore
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

  @Test
  public void testPadFunctions() throws Exception {
    String query = "select rpad(first_name, 10) as RPAD_DEF, rpad(first_name, 10, '*') as RPAD_STAR, lpad(first_name, 10) as LPAD_DEF, lpad(first_name, 10, '*') as LPAD_STAR, " +
        "lpad(first_name, 2) as LPAD_TRUNC, rpad(first_name, 2) as RPAD_TRUNC " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "RPAD_DEF=Sheri     ; " +
            "RPAD_STAR=Sheri*****; " +
            "LPAD_DEF=     Sheri; " +
            "LPAD_STAR=*****Sheri; " +
            "LPAD_TRUNC=Sh; " +
            "RPAD_TRUNC=Sh\n");
  }

  @Test
  public void testExtractSecond() throws Exception {
    String query = "select extract(second from date '2008-2-23') as DATE_EXT, extract(second from timestamp '2008-2-23 10:00:20.123') as TS_EXT, " +
        "extract(second from time '10:20:30.303') as TM_EXT " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DATE_EXT=0.0; " +
            "TS_EXT=20.123; " +
            "TM_EXT=30.303\n");
  }

  @Test
  public void testCastDecimalDouble() throws Exception {
    String query = "select cast((cast('1.0001' as decimal(18, 9))) as double) DECIMAL_DOUBLE_CAST " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DECIMAL_DOUBLE_CAST=1.0001\n");
  }

  @Ignore("we don't have decimal division")
  @Test
  public void testCastDecimalDivide() throws Exception {
    String query = "select  (cast('9' as decimal(9, 1)) / cast('2' as decimal(4, 1))) as DEC9_DIV, " +
        "cast('999999999' as decimal(9,0)) / cast('0.000000000000000000000000001' as decimal(28,28)) as DEC38_DIV, " +
        "cast('123456789.123456789' as decimal(18, 9)) * cast('123456789.123456789' as decimal(18, 9)) as DEC18_MUL " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC9_DIV=4.500000000; " +
            "DEC38_DIV=999999999000000000000000000000000000.0; " +
            "DEC18_MUL=15241578780673678.515622620750190521\n");
  }

  @Test
  public void testExtractSecondFromInterval() throws Exception {
    String query = "select extract (second from interval '1 2:30:45.100' day to second) as EXT_INTDAY " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "EXT_INTDAY=45.1\n");
  }

  @Test
  public void testFunctionCaseInsensitiveNames() throws Exception {
    String query = "SELECT to_date('2003/07/09', 'yyyy/MM/dd') as col1, " +
        "TO_DATE('2003/07/09', 'yyyy/MM/dd') as col2, " +
        "To_DaTe('2003/07/09', 'yyyy/MM/dd') as col3 " +
        "from cp.`employee.json` LIMIT 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns("col1=2003-07-09; col2=2003-07-09; col3=2003-07-09");
  }

  @Test
  public void testDecimal18Decimal38Comparison() throws Exception {
    String query = "select cast('-999999999.999999999' as decimal(18, 9)) = cast('-999999999.999999999' as decimal(38, 18)) as CMP " +
        "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "CMP=true\n");
  }

  @Test
  public void testDecimalMultiplicationOverflowHandling() throws Exception {
    String query = "select cast('1' as decimal(9, 5)) * cast ('999999999999999999999999999.999999999' as decimal(38, 9)) as DEC38_1, " +
                   "cast('1000000000000000001.000000000000000000' as decimal(38, 18)) * cast('0.999999999999999999' as decimal(38, 18)) as DEC38_2, " +
                   "cast('3' as decimal(9, 8)) * cast ('333333333.3333333333333333333' as decimal(38, 19)) as DEC38_3 " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC38_1=1000000000000000000000000000.00000; " +
            "DEC38_2=1000000000000000000; " +
            "DEC38_3=1000000000.000000000000000000\n");
  }

  @Test
  public void testOptiqDecimalCapping() throws Exception {
    String query = "select  cast('12345.678900000' as decimal(18, 9))=cast('12345.678900000' as decimal(38, 9)) as CMP " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "CMP=true\n");
  }

  @Test
  public void testDecimalRoundUp() throws Exception {
    String query = "select cast('999999999999999999.9999999999999999995' as decimal(38, 18)) as DEC38_1, " +
                   "cast('999999999999999999.9999999999999999994' as decimal(38, 18)) as DEC38_2, " +
                   "cast('999999999999999999.1234567895' as decimal(38, 9)) as DEC38_3, " +
                   "cast('99999.12345' as decimal(18, 4)) as DEC18_1, " +
                   "cast('99999.99995' as decimal(18, 4)) as DEC18_2 " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "DEC38_1=1000000000000000000.000000000000000000; " +
            "DEC38_2=999999999999999999.999999999999999999; " +
            "DEC38_3=999999999999999999.123456790; " +
            "DEC18_1=99999.1235; " +
            "DEC18_2=100000.0000\n");
  }

  @Test
  public void testNegative() throws Exception {
    String query = "select  negative(2) as NEG " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns(
            "NEG=-2\n");
  }

  @Test
  public void testOptiqValidationFunctions() throws Exception {
    String query = "select trim(first_name) as TRIM_STR, substring(first_name, 2) as SUB_STR " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns("TRIM_STR=Sheri; "+
                 "SUB_STR=heri\n");
  }
  @Test
  public void testDecimalDownwardCast() throws Exception {
    String query = "select cast((cast('12345.6789' as decimal(18, 4))) as decimal(9, 4)) as DEC18_DEC9_1, " +
                   "cast((cast('12345.6789' as decimal(18, 4))) as decimal(9, 2)) as DEC18_DEC9_2, " +
                   "cast((cast('-12345.6789' as decimal(18, 4))) as decimal(9, 0)) as DEC18_DEC9_3, " +
                   "cast((cast('999999999.6789' as decimal(38, 4))) as decimal(9, 0)) as DEC38_DEC19_1, " +
                   "cast((cast('-999999999999999.6789' as decimal(38, 4))) as decimal(18, 2)) as DEC38_DEC18_1, " +
                   "cast((cast('-999999999999999.6789' as decimal(38, 4))) as decimal(18, 0)) as DEC38_DEC18_2, " +
                   "cast((cast('100000000999999999.6789' as decimal(38, 4))) as decimal(28, 0)) as DEC38_DEC28_1 " +
                   "from cp.`employee.json` where employee_id = 1";

    JdbcAssert.withNoDefaultSchema()
        .sql(query)
        .returns("DEC18_DEC9_1=12345.6789; "+
                 "DEC18_DEC9_2=12345.68; " +
                 "DEC18_DEC9_3=-12346; " +
                 "DEC38_DEC19_1=1000000000; " +
                 "DEC38_DEC18_1=-999999999999999.68; " +
                 "DEC38_DEC18_2=-1000000000000000; " +
                 "DEC38_DEC28_1=100000001000000000\n");
  }
}
