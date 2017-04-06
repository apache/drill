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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.xsort.managed.PriorityQueueCopierWrapper.BatchMerger;
import org.apache.drill.exec.physical.impl.xsort.managed.SortTestUtilities.CopierTester;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Light-weight sanity test of the copier class. The implementation has
 * been used in production, so the tests here just check for the obvious
 * cases.
 * <p>
 * Note, however, that if significant changes are made to the copier,
 * then additional tests should be added to re-validate the code.
 */

public class TestCopier extends DrillTest {

  public static OperatorFixture fixture;

  @BeforeClass
  public static void setup() {
    fixture = OperatorFixture.builder().build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    fixture.close();
  }

  @Test
  public void testEmptyInput() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    List<BatchGroup> batches = new ArrayList<>();
    PriorityQueueCopierWrapper copier = SortTestUtilities.makeCopier(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED);
    VectorContainer dest = new VectorContainer();
    try {
      @SuppressWarnings({ "resource", "unused" })
      BatchMerger merger = copier.startMerge(schema, batches, dest, 10);
      fail();
    } catch (AssertionError e) {
      // Expected
    }
  }

  @Test
  public void testEmptyBatch() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .withSv2()
          .build());

    tester.run();
  }

  @Test
  public void testSingleRow() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .build());
    tester.run();
  }

  @Test
  public void testTwoBatchesSingleRow() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();
    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .withSv2()
          .build());
    tester.addInput(fixture.rowSetBuilder(schema)
          .add(20, "20")
          .withSv2()
          .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
          .add(10, "10")
          .add(20, "20")
          .build());
    tester.run();
  }

  public static SingleRowSet makeDataSet(BatchSchema schema, int first, int step, int count) {
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(count);
    int value = first;
    for (int i = 0; i < count; i++, value += step) {
      RowSetUtilities.setFromInt(writer, 0, value);
      writer.column(1).setString(Integer.toString(value));
      writer.save();
    }
    writer.done();
    return rowSet;
  }

  @Test
  public void testMultipleOutput() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.addInput(makeDataSet(schema, 0, 2, 10).toIndirect());
    tester.addInput(makeDataSet(schema, 1, 2, 10).toIndirect());

    tester.addOutput(makeDataSet(schema, 0, 1, 10));
    tester.addOutput(makeDataSet(schema, 10, 1, 10));
    tester.run();
  }

  // Also verifies that SV2s work

  @Test
  public void testMultipleOutputDesc() throws Exception {
    BatchSchema schema = SortTestUtilities.nonNullSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_UNSPECIFIED;
    SingleRowSet input = makeDataSet(schema, 0, 2, 10).toIndirect();
    RowSetUtilities.reverse(input.getSv2());
    tester.addInput(input);

    input = makeDataSet(schema, 1, 2, 10).toIndirect();
    RowSetUtilities.reverse(input.getSv2());
    tester.addInput(input);

    tester.addOutput(makeDataSet(schema, 19, -1, 10));
    tester.addOutput(makeDataSet(schema, 9, -1, 10));

    tester.run();
  }

  @Test
  public void testAscNullsLast() throws Exception {
    BatchSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_ASC;
    tester.nullOrder = Ordering.NULLS_LAST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(4, "4")
        .add(null, "null")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(2, "2")
        .add(3, "3")
        .add(null, "null")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .add(null, "null")
        .add(null, "null")
        .build());

    tester.run();
  }

  @Test
  public void testAscNullsFirst() throws Exception {
    BatchSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_ASC;
    tester.nullOrder = Ordering.NULLS_FIRST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(1, "1")
        .add(4, "4")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(2, "2")
        .add(3, "3")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(1, "1")
        .add(2, "2")
        .add(3, "3")
        .add(4, "4")
        .build());

    tester.run();
  }

  @Test
  public void testDescNullsLast() throws Exception {
    BatchSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_LAST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(1, "1")
        .add(null, "null")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(3, "3")
        .add(2, "2")
        .add(null, "null")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .add(null, "null")
        .add(null, "null")
        .build());

    tester.run();
  }

  @Test
  public void testDescNullsFirst() throws Exception {
    BatchSchema schema = SortTestUtilities.nullableSchema();

    CopierTester tester = new CopierTester(fixture);
    tester.sortOrder = Ordering.ORDER_DESC;
    tester.nullOrder = Ordering.NULLS_FIRST;
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(4, "4")
        .add(1, "1")
        .withSv2()
        .build());
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(3, "3")
        .add(2, "2")
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(null, "null")
        .add(null, "null")
        .add(4, "4")
        .add(3, "3")
        .add(2, "2")
        .add(1, "1")
        .build());

    tester.run();
  }

  public static void runTypeTest(OperatorFixture fixture, MinorType type) throws Exception {
    BatchSchema schema = SortTestUtilities.makeSchema(type, false);

    CopierTester tester = new CopierTester(fixture);
    tester.addInput(makeDataSet(schema, 0, 2, 5).toIndirect());
    tester.addInput(makeDataSet(schema, 1, 2, 5).toIndirect());

    tester.addOutput(makeDataSet(schema, 0, 1, 10));

    tester.run();
  }

  @Test
  public void testTypes() throws Exception {
    testAllTypes(fixture);
  }

  public static void testAllTypes(OperatorFixture fixture) throws Exception {
    runTypeTest(fixture, MinorType.INT);
    runTypeTest(fixture, MinorType.BIGINT);
    runTypeTest(fixture, MinorType.FLOAT4);
    runTypeTest(fixture, MinorType.FLOAT8);
    runTypeTest(fixture, MinorType.DECIMAL9);
    runTypeTest(fixture, MinorType.DECIMAL18);
    runTypeTest(fixture, MinorType.VARCHAR);
    runTypeTest(fixture, MinorType.VARBINARY);
    runTypeTest(fixture, MinorType.DATE);
    runTypeTest(fixture, MinorType.TIME);
    runTypeTest(fixture, MinorType.TIMESTAMP);
    runTypeTest(fixture, MinorType.INTERVAL);
    runTypeTest(fixture, MinorType.INTERVALDAY);
    runTypeTest(fixture, MinorType.INTERVALYEAR);

    // Others not tested. See DRILL-5329
  }

  @Test
  public void testMapType() throws Exception {
    testMapType(fixture);
  }

  public void testMapType(OperatorFixture fixture) throws Exception {
    BatchSchema schema = new SchemaBuilder()
        .add("key", MinorType.INT)
        .addMap("m1")
          .add("b", MinorType.INT)
          .addMap("m2")
            .add("c", MinorType.INT)
            .buildMap()
          .buildMap()
        .build();

    CopierTester tester = new CopierTester(fixture);
    tester.addInput(fixture.rowSetBuilder(schema)
        .add(1, 10, 100)
        .add(5, 50, 500)
        .withSv2()
        .build());

    tester.addInput(fixture.rowSetBuilder(schema)
        .add(2, 20, 200)
        .add(6, 60, 600)
        .withSv2()
        .build());

    tester.addOutput(fixture.rowSetBuilder(schema)
        .add(1, 10, 100)
        .add(2, 20, 200)
        .add(5, 50, 500)
        .add(6, 60, 600)
        .build());

    tester.run();
  }
}
