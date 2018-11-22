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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.vector.IntervalYearVector;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;

@Category({UnlikelyTest.class, SqlFunctionTest.class})
public class TestCastFunctions extends BaseTestQuery {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testVarbinaryToDate() throws Exception {
    testBuilder()
      .sqlQuery("select count(*) as cnt from cp.`employee.json` where (cast(convert_to(birth_date, 'utf8') as date)) = date '1961-08-26'")
      .unOrdered()
      .baselineColumns("cnt")
      .baselineValues(1L)
      .go();
  }

  @Test // DRILL-2827
  public void testImplicitCastStringToBoolean() throws Exception {
    testBuilder()
      .sqlQuery("(select * from cp.`store/json/booleanData.json` where key = 'true' or key = 'false')")
      .unOrdered()
      .baselineColumns("key")
      .baselineValues(true)
      .baselineValues(false)
      .build().run();
  }

  @Test // DRILL-2808
  public void testCastByConstantFolding() throws Exception {
    final String query = "SELECT count(DISTINCT employee_id) as col1, " +
        "count((to_number(date_diff(now(), cast(birth_date AS date)),'####'))) as col2 \n" +
        "FROM cp.`employee.json`";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col1", "col2")
        .baselineValues(1155L, 1155L)
        .go();
  }

  @Test // DRILL-3769
  public void testToDateForTimeStamp() throws Exception {
    mockUtcDateTimeZone();

    final String query = "select to_date(to_timestamp(-1)) as col \n"
      + "from (values(1))";

    testBuilder()
      .sqlQuery(query)
      .ordered()
      .baselineColumns("col")
      .baselineValues(LocalDate.of(1969, 12, 31))
      .build()
      .run();
  }

  @Test
  public void testCastFloatToInt() throws Exception {
    Map<Float, Integer> values = Maps.newHashMap();

    values.put(0F, 0);
    values.put(0.4F, 0);
    values.put(-0.4F, 0);
    values.put(0.5F, 1);
    values.put(-0.5F, -1);
    values.put(16777215F, 16777215);
    values.put(1677721F + 0.4F, 1677721);
    values.put(1677721F + 0.5F, 1677722);
    values.put(-16777216F, -16777216);
    values.put(-1677721 - 0.4F, -1677721);
    values.put(-1677721 - 0.5F, -1677722);
    values.put(Float.MAX_VALUE, Integer.MAX_VALUE);
    values.put(-Float.MAX_VALUE, Integer.MIN_VALUE);
    values.put(Float.MIN_VALUE, 0);

    for (float value : values.keySet()) {
      try {
        test("create table dfs.tmp.table_with_float as\n" +
              "(select cast(%1$s as float) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as int) col1 from dfs.tmp.table_with_float")
          .unOrdered()
          .baselineColumns("col1")
          .baselineValues(values.get(value))
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_float");
      }
    }
  }

  @Test
  public void testCastIntToFloatAndDouble() throws Exception {
    List<Integer> values = Lists.newArrayList();

    values.add(0);
    values.add(1);
    values.add(-1);
    values.add(16777215);
    values.add(-16777216);
    values.add(Integer.MAX_VALUE);
    values.add(Integer.MIN_VALUE);

    for (int value : values) {
      try {
        test("create table dfs.tmp.table_with_int as\n" +
              "(select cast(%1$s as int) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as float) col1,\n" +
                            "cast(c1 as double) col2\n" +
                    "from dfs.tmp.table_with_int")
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues((float) value, (double) value)
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_int");
      }
    }
  }

  @Test
  public void testCastFloatToBigInt() throws Exception {
    Map<Float, Long> values = Maps.newHashMap();

    values.put(0F, 0L);
    values.put(0.4F, 0L);
    values.put(-0.4F, 0L);
    values.put(0.5F, 1L);
    values.put(-0.5F, -1L);
    values.put(16777215F, 16777215L);
    values.put(1677721F + 0.4F, 1677721L);
    values.put(1677721F + 0.5F, 1677722L);
    values.put(-16777216F, -16777216L);
    values.put(-1677721 - 0.4F, -1677721L);
    values.put(-1677721 - 0.5F, -1677722L);
    values.put(Float.MAX_VALUE, Long.MAX_VALUE);
    values.put(Long.MIN_VALUE * 2F, Long.MIN_VALUE);
    values.put(Float.MIN_VALUE, 0L);

    for (float value : values.keySet()) {
      try {
        test("create table dfs.tmp.table_with_float as\n" +
              "(select cast(%1$s as float) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as bigInt) col1 from dfs.tmp.table_with_float")
          .unOrdered()
          .baselineColumns("col1")
          .baselineValues(values.get(value))
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_float");
      }
    }
  }

  @Test
  public void testCastBigIntToFloatAndDouble() throws Exception {
    List<Long> values = Lists.newArrayList();

    values.add(0L);
    values.add(1L);
    values.add(-1L);
    values.add(16777215L);
    values.add(-16777216L);
    values.add(9007199254740991L);
    values.add(-9007199254740992L);
    values.add(Long.MAX_VALUE);
    values.add(Long.MIN_VALUE);

    for (long value : values) {
      try {
        test("create table dfs.tmp.table_with_bigint as\n" +
              "(select cast(%1$s as bigInt) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as float) col1,\n" +
                            "cast(c1 as double) col2\n" +
                    "from dfs.tmp.table_with_bigint")
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues((float) value, (double) value)
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_bigint");
      }
    }
  }

  @Test
  public void testCastDoubleToInt() throws Exception {
    Map<Double, Integer> values = Maps.newHashMap();

    values.put(0D, 0);
    values.put(0.4, 0);
    values.put(-0.4, 0);
    values.put(0.5, 1);
    values.put(-0.5, -1);
    values.put((double) Integer.MAX_VALUE, Integer.MAX_VALUE);
    values.put(Integer.MAX_VALUE + 0.4, Integer.MAX_VALUE);
    values.put(Integer.MAX_VALUE + 0.5, Integer.MAX_VALUE);
    values.put((double) Integer.MIN_VALUE, Integer.MIN_VALUE);
    values.put(Integer.MIN_VALUE - 0.4, Integer.MIN_VALUE);
    values.put(Integer.MIN_VALUE - 0.5, Integer.MIN_VALUE);
    values.put(Double.MAX_VALUE, Integer.MAX_VALUE);
    values.put(-Double.MAX_VALUE, Integer.MIN_VALUE);
    values.put(Double.MIN_VALUE, 0);

    for (double value : values.keySet()) {
      try {
        test("create table dfs.tmp.table_with_double as\n" +
              "(select cast(%1$s as double) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as int) col1 from dfs.tmp.table_with_double")
          .unOrdered()
          .baselineColumns("col1")
          .baselineValues(values.get(value))
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_double");
      }
    }
  }

  @Test
  public void testCastDoubleToBigInt() throws Exception {
    Map<Double, Long> values = Maps.newHashMap();

    values.put(0D, 0L);
    values.put(0.4, 0L);
    values.put(-0.4, 0L);
    values.put(0.5, 1L);
    values.put(-0.5, -1L);
    values.put((double) Integer.MAX_VALUE, (long) Integer.MAX_VALUE);
    values.put((double) 9007199254740991L, 9007199254740991L);
    values.put(900719925474098L + 0.4, 900719925474098L);
    values.put(900719925474098L + 0.5, 900719925474099L);
    values.put((double) -9007199254740991L, -9007199254740991L);
    values.put(-900719925474098L - 0.4, -900719925474098L);
    values.put(-900719925474098L - 0.5, -900719925474099L);
    values.put(Double.MAX_VALUE, Long.MAX_VALUE);
    values.put(-Double.MAX_VALUE, Long.MIN_VALUE);
    values.put(Double.MIN_VALUE, 0L);
    for (double value : values.keySet()) {
      try {
        test("create table dfs.tmp.table_with_double as\n" +
              "(select cast(%1$s as double) c1 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as bigInt) col1 from dfs.tmp.table_with_double")
          .unOrdered()
          .baselineColumns("col1")
          .baselineValues(values.get(value))
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_double");
      }
    }
  }

  @Test
  public void testCastIntAndBigInt() throws Exception {
    List<Integer> values = Lists.newArrayList();

    values.add(0);
    values.add(1);
    values.add(-1);
    values.add(Integer.MAX_VALUE);
    values.add(Integer.MIN_VALUE);
    values.add(16777215);

    for (int value : values) {
      try {
        test("create table dfs.tmp.table_with_int as\n" +
              "(select cast(%1$s as int) c1, cast(%1$s as bigInt) c2 from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as bigint) col1,\n" +
                            "cast(c1 as int) col2\n" +
                    "from dfs.tmp.table_with_int")
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues((long) value, value)
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_int");
      }
    }
  }

  @Test
  public void testCastFloatAndDouble() throws Exception {
    List<Double> values = Lists.newArrayList();

    values.add(0d);
    values.add(0.4);
    values.add(-0.4);
    values.add(0.5);
    values.add(-0.5);
    values.add(16777215d);
    values.add(-16777216d);
    values.add((double) Float.MAX_VALUE);
    values.add(Double.MAX_VALUE);
    values.add((double) Float.MIN_VALUE);
    values.add(Double.MIN_VALUE);

    for (double value : values) {
      try {
        test("create table dfs.tmp.table_with_float as\n" +
              "(select cast(%1$s as float) c1,\n" +
                      "cast(%1$s as double) c2\n" +
              "from (values(1)))", value);

        testBuilder()
          .sqlQuery("select cast(c1 as double) col1,\n" +
                            "cast(c2 as float) col2\n" +
                    "from dfs.tmp.table_with_float")
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues((double) ((float) (value)), (float) value)
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_float");
      }
    }
  }

  @Test
  public void testCastIntAndBigIntToDecimal() throws Exception {
      try {
        test("alter session set planner.enable_decimal_data_type = true");

        testBuilder()
          .physicalPlanFromFile("decimal/cast_int_decimal.json")
          .unOrdered()
          .baselineColumns("DEC9_INT", "DEC38_INT", "DEC9_BIGINT", "DEC38_BIGINT")
          .baselineValues(new BigDecimal(0), new BigDecimal(0), new BigDecimal(0), new BigDecimal(0))
          .baselineValues(new BigDecimal(1), new BigDecimal(1), new BigDecimal(1), new BigDecimal(1))
          .baselineValues(new BigDecimal(-1), new BigDecimal(-1), new BigDecimal(-1), new BigDecimal(-1))

          .baselineValues(new BigDecimal(Integer.MAX_VALUE),
                          new BigDecimal(Integer.MAX_VALUE),
                          new BigDecimal(Long.MAX_VALUE),
                          new BigDecimal(Long.MAX_VALUE))

          .baselineValues(new BigDecimal(Integer.MIN_VALUE),
                          new BigDecimal(Integer.MIN_VALUE),
                          new BigDecimal(Long.MIN_VALUE),
                          new BigDecimal(Long.MIN_VALUE))

          .baselineValues(new BigDecimal(123456789),
                          new BigDecimal(123456789),
                          new BigDecimal(123456789),
                          new BigDecimal(123456789))
          .go();
      } finally {
        test("drop table if exists dfs.tmp.table_with_int");
        test("alter session reset planner.enable_decimal_data_type");
      }
  }

  @Test
  public void testCastDecimalToIntAndBigInt() throws Exception {
    try {
      test("alter session set planner.enable_decimal_data_type = true");

      testBuilder()
        .physicalPlanFromFile("decimal/cast_decimal_int.json")
        .unOrdered()
        .baselineColumns("DEC9_INT", "DEC38_INT", "DEC9_BIGINT", "DEC38_BIGINT")
        .baselineValues(0, 0, 0L, 0L)
        .baselineValues(1, 1, 1L, 1L)
        .baselineValues(-1, -1, -1L, -1L)
        .baselineValues(Integer.MAX_VALUE,
                        (int) Long.MAX_VALUE,
                        (long) Integer.MAX_VALUE,
                        Long.MAX_VALUE)
        .baselineValues(Integer.MIN_VALUE,
                        (int) Long.MIN_VALUE,
                        (long) Integer.MIN_VALUE,
                        Long.MIN_VALUE)
        .baselineValues(123456789, 123456789, 123456789L, 123456789L)
        .go();
    } finally {
      test("drop table if exists dfs.tmp.table_with_int");
      test("alter session reset planner.enable_decimal_data_type");
    }
  }

  @Test
  public void testCastDecimalToFloatAndDouble() throws Exception {
    try {
      test("alter session set planner.enable_decimal_data_type = true");

      testBuilder()
        .physicalPlanFromFile("decimal/cast_decimal_float.json")
        .ordered()
        .baselineColumns("DEC9_FLOAT", "DEC38_FLOAT", "DEC9_DOUBLE", "DEC38_DOUBLE")
        .baselineValues(99f, 123456789f, 99d, 123456789d)
        .baselineValues(11.1235f, 11.1235f, 11.1235, 11.1235)
        .baselineValues(0.1000f, 0.1000f, 0.1000, 0.1000)
        .baselineValues(-0.12f, -0.1004f, -0.12, -0.1004)
        .baselineValues(-123.1234f, -987654321.1234567891f, -123.1234, -987654321.1235)
        .baselineValues(-1.0001f, -2.0301f, -1.0001, -2.0301)
        .go();
    } finally {
      test("drop table if exists dfs.tmp.table_with_int");
      test("alter session reset planner.enable_decimal_data_type");
    }
  }

  @Test
  public void testCastDecimalToVarDecimal() throws Exception {
    try {
      setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true);

      testBuilder()
        .physicalPlanFromFile("decimal/cast_decimal_vardecimal.json")
        .unOrdered()
        .baselineColumns("DEC28_COL", "DEC38_COL", "DEC9_COL", "DEC18_COL")
        .baselineValues(new BigDecimal("-100000000001.0000000000000000"), new BigDecimal("1123.3000000000000000"),
            new BigDecimal("1123"), new BigDecimal("-100000000001"))
        .baselineValues(new BigDecimal("11.1234567890123456"), new BigDecimal("0.3000000000000000"),
            new BigDecimal("0"), new BigDecimal("11"))
        .baselineValues(new BigDecimal("0.1000000000010000"), new BigDecimal("123456789.0000000000000000"),
            new BigDecimal("123456789"), new BigDecimal("0"))
        .baselineValues(new BigDecimal("-0.1200000000000000"), new BigDecimal("0.0000020000000000"),
            new BigDecimal("0"), new BigDecimal("0"))
        .baselineValues(new BigDecimal("100000000001.1234567890010000"), new BigDecimal("111.3000000000000000"),
            new BigDecimal("111"), new BigDecimal("100000000001"))
        .baselineValues(new BigDecimal("-100000000001.0000000000000000"), new BigDecimal("121.0930000000000000"),
            new BigDecimal("121"), new BigDecimal("-100000000001"))
        .baselineValues(new BigDecimal("123456789123456789.0000000000000000"), new BigDecimal("12.3000000000000000"),
            new BigDecimal("12"), new BigDecimal("123456789123456789"))
        .go();
    } finally {
      test("drop table if exists dfs.tmp.table_with_int");
      resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
    }
  }

  @Test
  public void testCastVarDecimalToDecimal() throws Exception {
    try {
      setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, true);

      testBuilder()
        .physicalPlanFromFile("decimal/cast_vardecimal_decimal.json")
        .unOrdered()
        .baselineColumns("DEC28_COL", "DEC38_COL", "DEC9_COL", "DEC18_COL")
        .baselineValues(new BigDecimal("-100000000001.0000000000000000"), new BigDecimal("1123.3000000000000000"),
          new BigDecimal("1123"), new BigDecimal("-100000000001"))
        .baselineValues(new BigDecimal("11.1234567890123456"), new BigDecimal("0.3000000000000000"),
          new BigDecimal("0"), new BigDecimal("11"))
        .baselineValues(new BigDecimal("0.1000000000010000"), new BigDecimal("123456789.0000000000000000"),
          new BigDecimal("123456789"), new BigDecimal("0"))
        .baselineValues(new BigDecimal("-0.1200000000000000"), new BigDecimal("0.0000020000000000"),
          new BigDecimal("0"), new BigDecimal("0"))
        .baselineValues(new BigDecimal("100000000001.1234567890010000"), new BigDecimal("111.3000000000000000"),
          new BigDecimal("111"), new BigDecimal("100000000001"))
        .baselineValues(new BigDecimal("-100000000001.0000000000000000"), new BigDecimal("121.0930000000000000"),
          new BigDecimal("121"), new BigDecimal("-100000000001"))
        .baselineValues(new BigDecimal("123456789123456789.0000000000000000"), new BigDecimal("12.3000000000000000"),
          new BigDecimal("12"), new BigDecimal("123456789123456789"))
        .go();
    } finally {
      test("drop table if exists dfs.tmp.table_with_int");
      resetSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY);
    }
  }

  @Test // DRILL-4970
  public void testCastNegativeFloatToInt() throws Exception {
    try {
      test("create table dfs.tmp.table_with_float as\n" +
              "(select cast(-255.0 as double) as double_col,\n" +
                      "cast(-255.0 as float) as float_col\n" +
              "from (values(1)))");

      final List<String> columnNames = Lists.newArrayList();
      columnNames.add("float_col");
      columnNames.add("double_col");

      final List<String> castTypes = Lists.newArrayList();
      castTypes.add("int");
      castTypes.add("bigInt");

      final String query = "select count(*) as c from dfs.tmp.table_with_float\n" +
                            "where (cast(%1$s as %2$s) >= -255 and (%1$s <= -5)) or (%1$s <= -256)";

      for (String columnName : columnNames) {
        for (String castType : castTypes) {
          testBuilder()
            .sqlQuery(query, columnName, castType)
            .unOrdered()
            .baselineColumns("c")
            .baselineValues(1L)
            .go();
        }
      }
    } finally {
      test("drop table if exists dfs.tmp.table_with_float");
    }
  }

  @Test // DRILL-4970
  public void testCastNegativeDecimalToVarChar() throws Exception {
    try {
      test("alter session set planner.enable_decimal_data_type = true");

      test("create table dfs.tmp.table_with_decimal as" +
              "(select cast(cast(manager_id as double) * (-1) as decimal(9, 0)) as decimal9_col,\n" +
                      "cast(cast(manager_id as double) * (-1) as decimal(18, 0)) as decimal18_col\n" +
              "from cp.`parquet/fixedlenDecimal.parquet` limit 1)");

      final List<String> columnNames = Lists.newArrayList();
      columnNames.add("decimal9_col");
      columnNames.add("decimal18_col");

      final String query = "select count(*) as c from dfs.tmp.table_with_decimal\n" +
                            "where (cast(%1$s as varchar) = '-124' and (%1$s <= -5)) or (%1$s <= -256)";

      for (String colName : columnNames) {
        testBuilder()
          .sqlQuery(query, colName)
          .unOrdered()
          .baselineColumns("c")
          .baselineValues(1L)
          .go();
      }
    } finally {
      test("drop table if exists dfs.tmp.table_with_decimal");
      test("alter session reset planner.enable_decimal_data_type");
    }
  }

  @Test
  public void testCastDecimalLiteral() throws Exception {
    String query =
        "select case when true then cast(100.0 as decimal(38,2)) else cast('123.0' as decimal(38,2)) end as c1";

    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("c1")
        .baselineValues(new BigDecimal("100.00"))
        .go();
  }

  @Test
  public void testCastDecimalZeroPrecision() throws Exception {
    String query = "select cast('123.0' as decimal(0, 5))";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Expected precision greater than 0, but was 0"));

    test(query);
  }

  @Test
  public void testCastDecimalGreaterScaleThanPrecision() throws Exception {
    String query = "select cast('123.0' as decimal(3, 5))";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Expected scale less than or equal to precision, but was scale 5 and precision 3"));

    test(query);
  }

  @Test
  public void testCastIntDecimalOverflow() throws Exception {
    String query = "select cast(i1 as DECIMAL(4, 0)) as s1 from (select cast(123456 as int) as i1)";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Value 123456 overflows specified precision 4 with scale 0"));

    test(query);
  }

  @Test
  public void testCastBigIntDecimalOverflow() throws Exception {
    String query = "select cast(i1 as DECIMAL(4, 0)) as s1 from (select cast(123456 as bigint) as i1)";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Value 123456 overflows specified precision 4 with scale 0"));

    test(query);
  }

  @Test
  public void testCastFloatDecimalOverflow() throws Exception {
    String query = "select cast(i1 as DECIMAL(4, 0)) as s1 from (select cast(123456.123 as float) as i1)";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Value 123456.123 overflows specified precision 4 with scale 0"));

    test(query);
  }

  @Test
  public void testCastDoubleDecimalOverflow() throws Exception {
    String query = "select cast(i1 as DECIMAL(4, 0)) as s1 from (select cast(123456.123 as double) as i1)";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Value 123456.123 overflows specified precision 4 with scale 0"));

    test(query);
  }

  @Test
  public void testCastVarCharDecimalOverflow() throws Exception {
    String query = "select cast(i1 as DECIMAL(4, 0)) as s1 from (select cast(123456.123 as varchar) as i1)";

    thrown.expect(UserRemoteException.class);
    thrown.expectMessage(containsString("VALIDATION ERROR: Value 123456.123 overflows specified precision 4 with scale 0"));

    test(query);
  }

  @Test // DRILL-6783
  public void testCastVarCharIntervalYear() throws Exception {
    String query = "select cast('P31M' as interval month) as i from cp.`employee.json` limit 10";
    List<QueryDataBatch> result = testSqlWithResults(query);
    RecordBatchLoader loader = new RecordBatchLoader(getDrillbitContext().getAllocator());

    QueryDataBatch b = result.get(0);
    loader.load(b.getHeader().getDef(), b.getData());

    IntervalYearVector vector = (IntervalYearVector) loader.getValueAccessorById(
          IntervalYearVector.class,
          loader.getValueVectorId(SchemaPath.getCompoundPath("i")).getFieldIds())
        .getValueVector();

    Set<String> resultSet = new HashSet<>();
    for (int i = 0; i < loader.getRecordCount(); i++) {
      String displayValue = vector.getAccessor().getAsStringBuilder(i).toString();
      resultSet.add(displayValue);
    }

    Assert.assertEquals(
        "Casting literal string as INTERVAL should yield the same result for each row", 1, resultSet.size());
    Assert.assertThat(resultSet, hasItem("2 years 7 months"));

    b.release();
    loader.clear();
  }
}
