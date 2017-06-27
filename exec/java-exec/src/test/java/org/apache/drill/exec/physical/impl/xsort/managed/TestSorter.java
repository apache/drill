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
package org.apache.drill.exec.physical.impl.xsort.managed;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests the generated per-batch sort code via its wrapper layer.
 */

public class TestSorter extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    fixture.close();
  }

  public static Sort makeSortConfig(String key, String sortOrder, String nullOrder) {
    FieldReference expr = FieldReference.getWithQuotedRef(key);
    Ordering ordering = new Ordering(sortOrder, expr, nullOrder);
    return new Sort(null, Lists.newArrayList(ordering), false);
  }

  public void runSorterTest(SingleRowSet rowSet, SingleRowSet expected) throws Exception {
    runSorterTest(makeSortConfig("key", Ordering.ORDER_ASC, Ordering.NULLS_LAST), rowSet, expected);
  }

  public void runSorterTest(Sort popConfig, SingleRowSet rowSet, SingleRowSet expected) throws Exception {
    OperExecContext opContext = fixture.newOperExecContext(popConfig);
    SorterWrapper sorter = new SorterWrapper(opContext);

    sorter.sortBatch(rowSet.container(), rowSet.getSv2());

    new RowSetComparison(expected)
        .verifyAndClearAll(rowSet);
    sorter.close();
  }

  // Test degenerate case: no rows

  @Test
  public void testEmptyRowSet() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .withSv2()
        .build();
    SingleRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .build();
    runSorterTest(rowSet, expected);
  }

  // Sanity test: single row

  @Test
  public void testSingleRow() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
          .add(0, "0")
          .withSv2()
          .build();

    SingleRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .add(0, "0")
        .build();
    runSorterTest(rowSet, expected);
  }

  // Paranoia: sort with two rows.

  @Test
  public void testTwoRows() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    SingleRowSet rowSet = new RowSetBuilder(fixture.allocator(), schema)
        .add(1, "1")
        .add(0, "0")
        .withSv2()
        .build();

    SingleRowSet expected = new RowSetBuilder(fixture.allocator(), schema)
        .add(0, "0")
        .add(1, "1")
        .build();
    runSorterTest(rowSet, expected);
  }

  private abstract static class BaseSortTester {
    protected final OperatorFixture fixture;
    protected final SorterWrapper sorter;
    protected final boolean nullable;

    public BaseSortTester(OperatorFixture fixture, String sortOrder, String nullOrder, boolean nullable) {
      this.fixture = fixture;
      Sort popConfig = makeSortConfig("key", sortOrder, nullOrder);
      this.nullable = nullable;

      OperExecContext opContext = fixture.newOperExecContext(popConfig);
      sorter = new SorterWrapper(opContext);
    }
  }

  private abstract static class SortTester extends BaseSortTester {

    protected DataItem data[];

    public SortTester(OperatorFixture fixture, String sortOrder, String nullOrder, boolean nullable) {
      super(fixture, sortOrder, nullOrder, nullable);
    }

    public void test(MinorType type) throws SchemaChangeException {
      data = makeDataArray(20);
      BatchSchema schema = SortTestUtilities.makeSchema(type, nullable);
      SingleRowSet input = makeDataSet(fixture.allocator(), schema, data);
      input = input.toIndirect();
      sorter.sortBatch(input.container(), input.getSv2());
      sorter.close();
      verify(input);
    }

    public static class DataItem {
      public final int key;
      public final int value;
      public final boolean isNull;

      public DataItem(int key, int value, boolean isNull) {
        this.key = key;
        this.value = value;
        this.isNull = isNull;
      }

      @Override
      public String toString() {
        return "(" + key + ", \"" + value + "\", " +
               (isNull ? "null" : "set") + ")";
      }
    }

    public DataItem[] makeDataArray(int size) {
      DataItem values[] = new DataItem[size];
      int key = 11;
      int delta = 3;
      for (int i = 0; i < size; i++) {
        values[i] = new DataItem(key, i, key % 5 == 0);
        key = (key + delta) % size;
      }
      return values;
    }

    public SingleRowSet makeDataSet(BufferAllocator allocator, BatchSchema schema, DataItem[] items) {
      ExtendableRowSet rowSet = fixture.rowSet(schema);
      RowSetWriter writer = rowSet.writer(items.length);
      for (int i = 0; i < items.length; i++) {
        DataItem item = items[i];
        if (nullable && item.isNull) {
          writer.column(0).setNull();
        } else {
          RowSetUtilities.setFromInt(writer, 0, item.key);
        }
        writer.column(1).setString(Integer.toString(item.value));
        writer.save();
      }
      writer.done();
      return rowSet;
    }

    private void verify(RowSet actual) {
      DataItem expected[] = Arrays.copyOf(data, data.length);
      doSort(expected);
      RowSet expectedRows = makeDataSet(actual.allocator(), actual.schema().batch(), expected);
      doVerify(expected, expectedRows, actual);
    }

    protected void doVerify(DataItem[] expected, RowSet expectedRows, RowSet actual) {
      new RowSetComparison(expectedRows)
            .verifyAndClearAll(actual);
    }

    protected abstract void doSort(DataItem[] expected);
  }

  private static class TestSorterNumeric extends SortTester {

    private final int sign;

    public TestSorterNumeric(OperatorFixture fixture, boolean asc) {
      super(fixture,
            asc ? Ordering.ORDER_ASC : Ordering.ORDER_DESC,
            Ordering.NULLS_UNSPECIFIED, false);
      sign = asc ? 1 : -1;
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return sign * Integer.compare(o1.key, o2.key);
        }
      });
    }
  }

  private static class TestSorterNullableNumeric extends SortTester {

    private final int sign;
    private final int nullSign;

    public TestSorterNullableNumeric(OperatorFixture fixture, boolean asc, boolean nullsLast) {
      super(fixture,
          asc ? Ordering.ORDER_ASC : Ordering.ORDER_DESC,
          nullsLast ? Ordering.NULLS_LAST : Ordering.NULLS_FIRST,
          true);
      sign = asc ? 1 : -1;
      nullSign = nullsLast ? 1 : -1;
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          if (o1.isNull  &&  o2.isNull) { return 0; }
          if (o1.isNull) { return nullSign; }
          if (o2.isNull) { return -nullSign; }
          return sign * Integer.compare(o1.key, o2.key);
        }
      });
    }

    @Override
    protected void doVerify(DataItem[] expected, RowSet expectedRows, RowSet actual) {
      int nullCount = 0;
      for (DataItem item : expected) {
        if (item.isNull) { nullCount++; }
      }
      int length = expected.length - nullCount;
      int offset = (nullSign == 1) ? 0 : nullCount;
      new RowSetComparison(expectedRows)
            .offset(offset)
            .span(length)
            .verify(actual);
      offset = length - offset;
      new RowSetComparison(expectedRows)
            .offset(offset)
            .span(nullCount)
            .withMask(true, false)
            .verifyAndClearAll(actual);
    }
  }

  private static class TestSorterStringAsc extends SortTester {

    public TestSorterStringAsc(OperatorFixture fixture) {
      super(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, false);
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return Integer.toString(o1.key).compareTo(Integer.toString(o2.key));
        }
      });
    }
  }

  private static class TestSorterBinaryAsc extends SortTester {

    public TestSorterBinaryAsc(OperatorFixture fixture) {
      super(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, false);
    }

    @Override
    protected void doSort(DataItem[] expected) {
      Arrays.sort(expected, new Comparator<DataItem>(){
        @Override
        public int compare(DataItem o1, DataItem o2) {
          return Integer.toHexString(o1.key).compareTo(Integer.toHexString(o2.key));
        }
      });
    }
  }

  private abstract static class BaseTestSorterIntervalAsc extends BaseSortTester {

    public BaseTestSorterIntervalAsc(OperatorFixture fixture) {
      super(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, false);
    }

    public void test(MinorType type) throws SchemaChangeException {
      BatchSchema schema = new SchemaBuilder()
          .add("key", type)
          .build();
      SingleRowSet input = makeInputData(fixture.allocator(), schema);
      input = input.toIndirect();
      sorter.sortBatch(input.container(), input.getSv2());
      sorter.close();
      verify(input);
      input.clear();
    }

    protected SingleRowSet makeInputData(BufferAllocator allocator,
        BatchSchema schema) {
      RowSetBuilder builder = fixture.rowSetBuilder(schema);
      int rowCount = 100;
      Random rand = new Random();
      for (int i = 0; i < rowCount; i++) {
        int ms = rand.nextInt(1000);
        int sec = rand.nextInt(60);
        int min = rand.nextInt(60);
        int hr = rand.nextInt(24);
        int day = rand.nextInt(28);
        int mo = rand.nextInt(12);
        int yr = rand.nextInt(10);
        Period period = makePeriod(yr, mo, day, hr, min, sec, ms);
        builder.add(period);
      }
      return builder.build();
    }

    protected abstract Period makePeriod(int yr, int mo, int day, int hr, int min, int sec,
        int ms);

    private void verify(SingleRowSet output) {
      RowSetReader reader = output.reader();
      int prevYears = 0;
      int prevMonths = 0;
      long prevMs = 0;
      while (reader.next()) {
        Period period = reader.column(0).getPeriod().normalizedStandard();
        int years = period.getYears();
        assertTrue(prevYears <= years);
        if (prevYears != years) {
          prevMonths = 0;
          prevMs = 0;
        }
        prevYears = years;

        int months = period.getMonths();
        assertTrue(prevMonths <= months);
        if (prevMonths != months) {
           prevMs = 0;
        }
        prevMonths = months;

        Period remainder = period
            .withYears(0)
            .withMonths(0);

        long ms = remainder.toStandardDuration().getMillis();
        assertTrue(prevMs <= ms);
        prevMs = ms;
      }
    }
  }

  private static class TestSorterIntervalAsc extends BaseTestSorterIntervalAsc {

    public TestSorterIntervalAsc(OperatorFixture fixture) {
      super(fixture);
    }

    public void test() throws SchemaChangeException {
      test(MinorType.INTERVAL);
    }

    @Override
    protected Period makePeriod(int yr, int mo, int day, int hr, int min,
        int sec, int ms) {
      return Period.years(yr)
          .withMonths(mo)
          .withDays(day)
          .withHours(hr)
          .withMinutes(min)
          .withSeconds(sec)
          .withMillis(ms);
    }
  }

  private static class TestSorterIntervalYearAsc extends BaseTestSorterIntervalAsc {

    public TestSorterIntervalYearAsc(OperatorFixture fixture) {
      super(fixture);
    }

    public void test() throws SchemaChangeException {
      test(MinorType.INTERVALYEAR);
    }

    @Override
    protected Period makePeriod(int yr, int mo, int day, int hr, int min,
        int sec, int ms) {
      return Period.years(yr)
          .withMonths(mo);
    }
  }

  private static class TestSorterIntervalDayAsc extends BaseTestSorterIntervalAsc {

    public TestSorterIntervalDayAsc(OperatorFixture fixture) {
      super(fixture);
    }

    public void test() throws SchemaChangeException {
      test(MinorType.INTERVALDAY);
    }

    @Override
    protected Period makePeriod(int yr, int mo, int day, int hr, int min,
        int sec, int ms) {
      return Period.days(day)
          .withHours(hr)
          .withMinutes(min)
          .withSeconds(sec)
          .withMillis(ms);
    }
  }

  @Test
  public void testNumericTypes() throws Exception {
    TestSorterNumeric tester1 = new TestSorterNumeric(fixture, true);
//      tester1.test(MinorType.TINYINT); // DRILL-5329
//      tester1.test(MinorType.UINT1); DRILL-5329
//      tester1.test(MinorType.SMALLINT); DRILL-5329
//      tester1.test(MinorType.UINT2); DRILL-5329
    tester1.test(MinorType.INT);
//      tester1.test(MinorType.UINT4); DRILL-5329
    tester1.test(MinorType.BIGINT);
//      tester1.test(MinorType.UINT8); DRILL-5329
    tester1.test(MinorType.FLOAT4);
    tester1.test(MinorType.FLOAT8);
    tester1.test(MinorType.DECIMAL9);
    tester1.test(MinorType.DECIMAL18);
//      tester1.test(MinorType.DECIMAL28SPARSE); DRILL-5329
//      tester1.test(MinorType.DECIMAL38SPARSE); DRILL-5329
//    tester1.test(MinorType.DECIMAL28DENSE); No writer
//    tester1.test(MinorType.DECIMAL38DENSE); No writer
    tester1.test(MinorType.DATE);
    tester1.test(MinorType.TIME);
    tester1.test(MinorType.TIMESTAMP);
  }

  @Test
  public void testVarCharTypes() throws Exception {
    TestSorterStringAsc tester = new TestSorterStringAsc(fixture);
    tester.test(MinorType.VARCHAR);
//      tester.test(MinorType.VAR16CHAR); DRILL-5329
  }

  /**
   * Test the VARBINARY data type as a sort key.
   *
   * @throws Exception for internal errors
   */

  @Test
  public void testVarBinary() throws Exception {
    TestSorterBinaryAsc tester = new TestSorterBinaryAsc(fixture);
    tester.test(MinorType.VARBINARY);
  }

  /**
   * Test the INTERVAL data type as a sort key.
   *
   * @throws Exception for internal errors
   */

  @Test
  public void testInterval() throws Exception {
    TestSorterIntervalAsc tester = new TestSorterIntervalAsc(fixture);
    tester.test();
  }

  /**
   * Test the INTERVALYEAR data type as a sort key.
   *
   * @throws Exception for internal errors
   */

  @Test
  public void testIntervalYear() throws Exception {
    TestSorterIntervalYearAsc tester = new TestSorterIntervalYearAsc(fixture);
    tester.test();
  }

  /**
   * Test the INTERVALDAY data type as a sort key.
   *
   * @throws Exception for internal errors
   */

  @Test
  public void testIntervalDay() throws Exception {
    TestSorterIntervalDayAsc tester = new TestSorterIntervalDayAsc(fixture);
    tester.test();
  }

  @Test
  public void testDesc() throws Exception {
    TestSorterNumeric tester = new TestSorterNumeric(fixture, false);
    tester.test(MinorType.INT);
  }

  /**
   * Verify that nulls sort in the requested position: high or low.
   * Earlier tests verify that "unspecified" maps to high or low
   * depending on sort order.
   */

  @Test
  public void testNullable() throws Exception {
    TestSorterNullableNumeric tester = new TestSorterNullableNumeric(fixture, true, true);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, true, false);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, false, true);
    tester.test(MinorType.INT);
    tester = new TestSorterNullableNumeric(fixture, false, false);
    tester.test(MinorType.INT);
  }

  @Test
  @Ignore("DRILL-5384")
  public void testMapKey() throws Exception {
    BatchSchema schema = new SchemaBuilder()
        .addMap("map")
          .add("key", MinorType.INT)
          .add("value", MinorType.VARCHAR)
          .buildMap()
        .build();

    SingleRowSet input = fixture.rowSetBuilder(schema)
        .add(3, "third")
        .add(1, "first")
        .add(2, "second")
        .withSv2()
        .build();

    SingleRowSet output = fixture.rowSetBuilder(schema)
        .add(1, "first")
        .add(2, "second")
        .add(3, "third")
        .build();
    Sort popConfig = makeSortConfig("map.key", Ordering.ORDER_ASC, Ordering.NULLS_LAST);
    runSorterTest(popConfig, input, output);
  }
}
