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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.categories.OperatorTest;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.DrillTest;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.OperatorFixture.OperatorFixtureBuilder;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.HyperRowSetImpl;
import org.apache.drill.test.rowSet.IndirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;
import org.apache.drill.test.rowSet.RowSet.RowSetWriter;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.junit.experimental.categories.Category;

/**
 * Tests the external sort implementation: the "guts" of the sort stripped of the
 * Volcano-protocol layer. Assumes the individual components are already tested.
 */

@Category(OperatorTest.class)
public class TestSortImpl extends DrillTest {

  /**
   * Create the sort implementation to be used by test.
   *
   * @param fixture operator fixture
   * @param sortOrder sort order as specified by {@link Ordering}
   * @param nullOrder null order as specified by {@link Ordering}
   * @param outputBatch where the sort should write its output
   * @return the sort initialized sort implementation, ready to
   * do work
   */

  public static SortImpl makeSortImpl(OperatorFixture fixture,
                               String sortOrder, String nullOrder,
                               VectorContainer outputBatch) {
    FieldReference expr = FieldReference.getWithQuotedRef("key");
    Ordering ordering = new Ordering(sortOrder, expr, nullOrder);
    Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);
    OperatorContext opContext = fixture.operatorContext(popConfig);
    QueryId queryId = QueryId.newBuilder()
        .setPart1(1234)
        .setPart2(5678)
        .build();
    FragmentHandle handle = FragmentHandle.newBuilder()
          .setMajorFragmentId(2)
          .setMinorFragmentId(3)
          .setQueryId(queryId)
          .build();
    SortConfig sortConfig = new SortConfig(opContext.getFragmentContext().getConfig());

    SpillSet spillSet = new SpillSet(opContext.getFragmentContext().getConfig(), handle, popConfig);
    PriorityQueueCopierWrapper copierHolder = new PriorityQueueCopierWrapper(opContext);
    SpilledRuns spilledRuns = new SpilledRuns(opContext, spillSet, copierHolder);
    return new SortImpl(opContext, sortConfig, spilledRuns, outputBatch);
  }

  /**
   * Handy fixture to hold a sort, a set of input row sets (batches) and the
   * output set of row sets (batches.) Pumps the input into the sort and
   * harvests the output. Subclasses define the specifics of the sort,
   * define the input data, and validate the output data.
   */

  public static class SortTestFixture {
    private final OperatorFixture fixture;
    private final List<RowSet> inputSets = new ArrayList<>();
    private final List<RowSet> expected = new ArrayList<>();
    String sortOrder = Ordering.ORDER_ASC;
    String nullOrder = Ordering.NULLS_UNSPECIFIED;

    public SortTestFixture(OperatorFixture fixture) {
      this.fixture = fixture;
    }

    public SortTestFixture(OperatorFixture fixture, String sortOrder, String nullOrder) {
      this.fixture = fixture;
      this.sortOrder = sortOrder;
      this.nullOrder = nullOrder;
    }

    public void addInput(RowSet input) {
      inputSets.add(input);
    }

    public void addOutput(RowSet output) {
      expected.add(output);
    }

    public void run() {
      VectorContainer dest = new VectorContainer();
      SortImpl sort = makeSortImpl(fixture, sortOrder, nullOrder, dest);

      // Simulates a NEW_SCHEMA event

      if (! inputSets.isEmpty()) {
        sort.setSchema(inputSets.get(0).container().getSchema());
      }

      // Simulates an OK event

      for (RowSet input : inputSets) {
        sort.addBatch(input.vectorAccessible());
      }

      // Simulate returning results

      SortResults results = sort.startMerge();
      if (results.getContainer() != dest) {
        dest.clear();
        dest = results.getContainer();
      }
      for (RowSet expectedSet : expected) {
        assertTrue(results.next());
        RowSet rowSet = toRowSet(fixture, results, dest);
        // Uncomment these for debugging. Leave them commented otherwise
        // to avoid polluting the Maven build output unnecessarily.
//        System.out.println("Expected:");
//        expectedSet.print();
//        System.out.println("Actual:");
//        rowSet.print();
        new RowSetComparison(expectedSet)
              .verify(rowSet);
        expectedSet.clear();
      }
      assertFalse(results.next());
      validateSort(sort);
      results.close();
      dest.clear();
      sort.close();
      validateFinalStats(sort);
    }

    protected void validateSort(SortImpl sort) { }
    protected void validateFinalStats(SortImpl sort) { }
  }

  /**
   * Sort produces a variety of output types. Convert each type to the corresponding
   * row set format. For historical reasons, the sort dumps its output into a vector
   * container (normally attached to the external sort batch, here used stand-alone.)
   *
   * @param fixture operator test fixture
   * @param results sort results iterator
   * @param dest container that holds the sort results
   * @return
   */

  private static RowSet toRowSet(OperatorFixture fixture, SortResults results, VectorContainer dest) {
    if (results.getSv4() != null) {
      return new HyperRowSetImpl(fixture.allocator(), dest, results.getSv4());
    } else if (results.getSv2() != null) {
      return new IndirectRowSet(fixture.allocator(), dest, results.getSv2());
    } else {
      return new DirectRowSet(fixture.allocator(), dest);
    }
  }

  /**
   * Test for null input (no input batches). Note that, in this case,
   * we never see a schema.
   * @throws Exception
   */

  @Test
  public void testNullInput() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      SortTestFixture sortTest = new SortTestFixture(fixture);
      sortTest.run();
    }
  }

  /**
   * Test for an input with a schema, but only an empty input batch.
   * @throws Exception
   */

  @Test
  public void testEmptyInput() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      BatchSchema schema = SortTestUtilities.nonNullSchema();
      SortTestFixture sortTest = new SortTestFixture(fixture);
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .build());
      sortTest.run();
    }
  }

  /**
   * Degenerate case: single row in single batch.
   * @throws Exception
   */

  @Test
  public void testSingleRow() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      BatchSchema schema = SortTestUtilities.nonNullSchema();
      SortTestFixture sortTest = new SortTestFixture(fixture);
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .build());
      sortTest.addOutput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .build());
      sortTest.run();
    }
  }

  /**
   * Degenerate case: two (unsorted) rows in single batch
   * @throws Exception
   */

  @Test
  public void testSingleBatch() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      BatchSchema schema = SortTestUtilities.nonNullSchema();
      SortTestFixture sortTest = new SortTestFixture(fixture);
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(2, "second")
          .add(1, "first")
          .build());
      sortTest.addOutput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .add(2, "second")
          .build());
      sortTest.run();
    }
  }

  /**
   * Degenerate case, one row in each of two
   * (unsorted) batches.
   * @throws Exception
   */

  @Test
  public void testTwoBatches() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      BatchSchema schema = SortTestUtilities.nonNullSchema();
      SortTestFixture sortTest = new SortTestFixture(fixture);
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(2, "second")
          .build());
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .build());
      sortTest.addOutput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .add(2, "second")
          .build());
      sortTest.run();
    }
  }

  /**
   * Crude-but-effective data generator that produces pseudo-random data
   * that can be easily verified. The pseudo-random data is generate by the
   * simple means of incrementing a counter using a random value, and wrapping.
   * This ensures we visit each value twice, and that the sorted output will
   * be a continuous run of numbers in proper order.
   */

  public static class DataGenerator {
    private final OperatorFixture fixture;
    private final BatchSchema schema;
    private final int targetCount;
    private final int batchSize;
    private final int step;
    private int rowCount;
    private int currentValue;

    public DataGenerator(OperatorFixture fixture, int targetCount, int batchSize) {
      this(fixture, targetCount, batchSize, 0, guessStep(targetCount));
    }

    public DataGenerator(OperatorFixture fixture, int targetCount, int batchSize, int seed, int step) {
      this.fixture = fixture;
      this.targetCount = targetCount;
      this.batchSize = Math.min(batchSize, Character.MAX_VALUE);
      this.step = step;
      schema = SortTestUtilities.nonNullSchema();
      currentValue = seed;
    }

    /**
     * Pick a reasonable prime step based on data size.
     *
     * @param target number of rows to generate
     * @return the prime step size
     */

    private static int guessStep(int target) {
      if (target < 10) {
        return 7;
      } else if (target < 200) {
        return 71;
      } else if (target < 2000) {
        return 701;
      } else if (target < 20000) {
        return 7001;
      } else {
        return 17011;
      }
    }

    public RowSet nextRowSet() {
      if (rowCount == targetCount) {
        return null;
      }
      RowSetBuilder builder = fixture.rowSetBuilder(schema);
      int end = Math.min(batchSize, targetCount - rowCount);
      for (int i = 0; i < end; i++) {
        builder.add(currentValue, i + ", " + currentValue);
        currentValue = (currentValue + step) % targetCount;
        rowCount++;
      }
      return builder.build();
    }
  }

  /**
   * Validate a sort output batch based on the expectation that the key
   * is an ordered sequence of integers, split across multiple batches.
   */

  public static class DataValidator {
    private final int targetCount;
    private final int batchSize;
    private int batchCount;
    private int rowCount;

    public DataValidator(int targetCount, int batchSize) {
      this.targetCount = targetCount;
      this.batchSize = Math.min(batchSize, Character.MAX_VALUE);
    }

    public void validate(RowSet output) {
      batchCount++;
      int expectedSize = Math.min(batchSize, targetCount - rowCount);
      assertEquals("Size of batch " + batchCount, expectedSize, output.rowCount());
      RowSetReader reader = output.reader();
      while (reader.next()) {
        assertEquals("Value of " + batchCount + ":" + rowCount,
            rowCount, reader.column(0).getInt());
        rowCount++;
      }
    }

    public void validateDone() {
      assertEquals("Wrong row count", targetCount, rowCount);
    }
  }

  Stopwatch timer = Stopwatch.createUnstarted();

  /**
   * Run a full-blown sort test with multiple input batches. Because we want to
   * generate multiple inputs, we don't create them statically. Instead, we generate
   * them on the fly using a data generator. A matching data validator verifies the
   * output. Here, we are focusing on overall test flow. Separate, detailed, unit
   * tests have already probed the details of each sort component and data type,
   * so we don't need to repeat that whole exercise here; using integer keys is
   * sufficient.
   *
   * @param fixture the operator test fixture
   * @param dataGen input batch generator
   * @param validator validates output batches
   */

  public void runLargeSortTest(OperatorFixture fixture, DataGenerator dataGen,
                               DataValidator validator) {
    VectorContainer dest = new VectorContainer();
    SortImpl sort = makeSortImpl(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, dest);

    int batchCount = 0;
    RowSet input;
    while ((input = dataGen.nextRowSet()) != null) {
      batchCount++;
      if (batchCount == 1) {
        // Simulates a NEW_SCHEMA event

        timer.start();
        sort.setSchema(input.container().getSchema());
        timer.stop();
      }

      // Simulates an OK event

      timer.start();
      sort.addBatch(input.vectorAccessible());
      timer.stop();
    }

    // Simulate returning results

    timer.start();
    SortResults results = sort.startMerge();
    if (results.getContainer() != dest) {
      dest.clear();
      dest = results.getContainer();
    }
    while (results.next()) {
      timer.stop();
      RowSet output = toRowSet(fixture, results, dest);
      validator.validate(output);
      timer.start();
    }
    timer.stop();
    validator.validateDone();
    results.close();
    dest.clear();
    sort.close();
  }

  /**
   * Set up and run a test for "jumbo" batches, and time the run.
   * @param fixture operator test fixture
   * @param rowCount number of rows to test
   */

  public void runJumboBatchTest(OperatorFixture fixture, int rowCount) {
    timer.reset();
    DataGenerator dataGen = new DataGenerator(fixture, rowCount, Character.MAX_VALUE);
    DataValidator validator = new DataValidator(rowCount, Character.MAX_VALUE);
    runLargeSortTest(fixture, dataGen, validator);
    System.out.println(timer.elapsed(TimeUnit.MILLISECONDS));
  }

  /**
   * Most tests have used small row counts because we want to probe specific bits
   * of interest. Try 1000 rows just to ensure things work
   *
   * @throws Exception
   */
  @Test
  public void testModerateBatch() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      runJumboBatchTest(fixture, 1000);
    }
  }

  /**
   * Hit the sort with the largest possible batch size to ensure nothing is lost
   * at the edges.
   *
   * @throws Exception
   */

  @Test
  public void testLargeBatch() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      runJumboBatchTest(fixture, Character.MAX_VALUE);
    }
  }

  /**
   * Run a test using wide rows. This stresses the "copier" portion of the sort
   * and allows us to test the original generated copier and the revised "generic"
   * copier.
   *
   * @param fixture operator test fixture
   * @param colCount number of data (non-key) columns
   * @param rowCount number of rows to generate
   */

  public void runWideRowsTest(OperatorFixture fixture, int colCount, int rowCount) {
    SchemaBuilder builder = new SchemaBuilder()
        .add("key", MinorType.INT);
    for (int i = 0; i < colCount; i++) {
      builder.add("col" + (i+1), MinorType.INT);
    }
    BatchSchema schema = builder.build();
    ExtendableRowSet rowSet = fixture.rowSet(schema);
    RowSetWriter writer = rowSet.writer(rowCount);
    for (int i = 0; i < rowCount; i++) {
      writer.set(0, i);
      for (int j = 0; j < colCount; j++) {
        writer.set(j + 1, i * 100_000 + j);
      }
      writer.save();
    }
    writer.done();

    VectorContainer dest = new VectorContainer();
    SortImpl sort = makeSortImpl(fixture, Ordering.ORDER_ASC, Ordering.NULLS_UNSPECIFIED, dest);
    timer.reset();
    timer.start();
    sort.setSchema(rowSet.container().getSchema());
    sort.addBatch(rowSet.vectorAccessible());
    SortResults results = sort.startMerge();
    if (results.getContainer() != dest) {
      dest.clear();
      dest = results.getContainer();
    }
    assertTrue(results.next());
    timer.stop();
    assertFalse(results.next());
    results.close();
    dest.clear();
    sort.close();
    System.out.println(timer.elapsed(TimeUnit.MILLISECONDS));
  }

  /**
   * Test wide rows with the stock copier.
   *
   * @throws Exception
   */

  @Test
  public void testWideRows() throws Exception {
    try (OperatorFixture fixture = OperatorFixture.standardFixture()) {
      runWideRowsTest(fixture, 1000, Character.MAX_VALUE);
    }
  }

  /**
   * Force the sorter to spill, and verify that the resulting data
   * is correct. Uses a specific property of the sort to set the
   * in-memory batch limit so that we don't have to fiddle with filling
   * up memory. The point here is not to test the code that decides when
   * to spill (that was already tested.) Nor to test the spilling
   * mechanism itself (that has also already been tested.) Rather it is
   * to ensure that, when those components are integrated into the
   * sort implementation, that the whole assembly does the right thing.
   *
   * @throws Exception
   */

  @Test
  public void testSpill() throws Exception {
    OperatorFixtureBuilder builder = OperatorFixture.builder();
    builder.configBuilder()
      .put(ExecConstants.EXTERNAL_SORT_BATCH_LIMIT, 2);
    try (OperatorFixture fixture = builder.build()) {
      BatchSchema schema = SortTestUtilities.nonNullSchema();
      SortTestFixture sortTest = new SortTestFixture(fixture) {
        @Override
        protected void validateSort(SortImpl sort) {
          assertEquals(1, sort.getMetrics().getSpillCount());
          assertEquals(0, sort.getMetrics().getMergeCount());
          assertEquals(2, sort.getMetrics().getPeakBatchCount());
        }
        @Override
        protected void validateFinalStats(SortImpl sort) {
          assertTrue(sort.getMetrics().getWriteBytes() > 0);
        }
      };
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(2, "second")
          .build());
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(3, "third")
          .build());
      sortTest.addInput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .build());
      sortTest.addOutput(fixture.rowSetBuilder(schema)
          .add(1, "first")
          .add(2, "second")
          .add(3, "third")
          .build());
      sortTest.run();
    }
  }
}
