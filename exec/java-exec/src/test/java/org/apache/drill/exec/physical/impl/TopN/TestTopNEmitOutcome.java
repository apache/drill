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
package org.apache.drill.exec.physical.impl.TopN;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.drill.categories.OperatorTest;
import org.apache.drill.exec.physical.config.TopN;
import org.apache.drill.exec.physical.impl.BaseTestOpBatchEmitOutcome;
import org.apache.drill.exec.physical.impl.MockRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.test.rowSet.RowSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class TestTopNEmitOutcome extends BaseTestOpBatchEmitOutcome {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTopNEmitOutcome.class);

  /**
   * Verifies count of column in the received batch is same as expected count of columns.
   * @param batch - Incoming record batch
   * @param expectedColCount - Expected count of columns in the record batch
   */
  private void verifyColumnCount(VectorAccessible batch, int expectedColCount) {
    List<String> columns = Lists.newArrayList();
    SelectionVector4 sv4 = batch.getSelectionVector4();
    for (VectorWrapper<?> vw : batch) {
      if (sv4 != null) {
        columns.add(vw.getValueVectors()[0].getField().getName());
      } else {
        columns.add(vw.getValueVector().getField().getName());
      }
    }
    assertEquals(String.format("Actual number of columns: %d is different than expected count: %d",
      columns.size(), expectedColCount), columns.size(), expectedColCount);
  }

  /**
   * Verifies the data received in incoming record batch with the expected data stored inside the expected batch.
   * Assumes input record batch has associated sv4 with it.
   * @param batch - incoming record batch
   * @param sv4 - associated sv4 with incoming record batch
   * @param expectedBatch - expected record batch with expected data
   */
  private void verifyBaseline(VectorAccessible batch, SelectionVector4 sv4, VectorContainer expectedBatch) {
    assertTrue(sv4 != null);
    List<String> columns = Lists.newArrayList();
    for (VectorWrapper<?> vw : batch) {
      columns.add(vw.getValueVectors()[0].getField().getName());
    }

    for (int j = 0; j < sv4.getCount(); j++) {
      List<String> eValue = new ArrayList<>(columns.size());
      List<String> value = new ArrayList<>(columns.size());

      for (VectorWrapper<?> vw : batch) {
        Object o = vw.getValueVectors()[sv4.get(j) >>> 16].getAccessor().getObject(sv4.get(j) & 65535);
        decodeAndAddValue(o, value);
      }

      for (VectorWrapper<?> vw : expectedBatch) {
        Object e = vw.getValueVector().getAccessor().getObject(j);
        decodeAndAddValue(e, eValue);
      }
      assertTrue("Some of expected value didn't matches with actual value",eValue.equals(value));
    }
  }

  private void decodeAndAddValue(Object currentValue, List<String> listToAdd) {
    if (currentValue == null) {
      listToAdd.add("null");
    } else if (currentValue instanceof byte[]) {
      listToAdd.add(new String((byte[]) currentValue));
    } else {
      listToAdd.add(currentValue.toString());
    }
  }

  /**
   * Verifies that if TopNBatch receives empty batches with OK_NEW_SCHEMA and EMIT outcome then it correctly produces
   * empty batches as output. First empty batch will be with OK_NEW_SCHEMA and second will be with EMIT outcome.
   * @throws Exception
   */
  @Test
  public void testTopNEmptyBatchEmitOutcome() {
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());

    inputOutcomes.add(OK_NEW_SCHEMA);
    inputOutcomes.add(EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.ASCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == OK_NEW_SCHEMA);
    outputRecordCount += topNBatch.getRecordCount();
    assertTrue(topNBatch.next() == OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == EMIT);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(0, outputRecordCount);
    assertTrue(topNBatch.next() == NONE);
  }

  /**
   * Verifies that if TopNBatch receives a RecordBatch with EMIT outcome post build schema phase then it produces
   * output for those input batch correctly. The first output batch will always be returned with OK_NEW_SCHEMA
   * outcome followed by EMIT with empty batch. The test verifies the output order with the expected baseline.
   * @throws Exception
   */
  @Test
  public void testTopNNonEmptyBatchEmitOutcome() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .build();

    final RowSet.SingleRowSet expectedRowSet = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .addRow(2, 20, "item2")
      .build();

    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(0, outputRecordCount);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // verify results
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // Release memory for row sets
    nonEmptyInputRowSet2.clear();
    expectedRowSet.clear();
  }

  @Test
  public void testTopNEmptyBatchFollowedByNonEmptyBatchEmitOutcome() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .build();

    final RowSet.SingleRowSet expectedRowSet = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .addRow(2, 20, "item2")
      .build();

    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(0, outputRecordCount);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // verify results
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // Release memory for row sets
    nonEmptyInputRowSet2.clear();
    expectedRowSet.clear();
  }

  @Test
  public void testTopNMultipleEmptyBatchFollowedByNonEmptyBatchEmitOutcome() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .build();

    final RowSet.SingleRowSet expectedRowSet = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(13, 130, "item13")
      .addRow(4, 40, "item4")
      .addRow(2, 20, "item2")
      .build();

    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(0, outputRecordCount);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // verify results
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    outputRecordCount += topNBatch.getRecordCount();
    assertEquals(3, outputRecordCount);

    // Release memory for row sets
    nonEmptyInputRowSet2.clear();
    expectedRowSet.clear();
  }

  /**
   * Verifies that if TopNBatch receives multiple non-empty record batch with EMIT outcome in between then it produces
   * output for those input batch correctly. In this case it receives first non-empty batch with OK_NEW_SCHEMA in
   * buildSchema phase followed by an empty batch with EMIT outcome. For this combination it produces output for the
   * record received so far along with EMIT outcome. Then it receives second non-empty batch with OK outcome and
   * produces output for it differently. The test validates that for each output received the order of the records are
   * correct.
   * @throws Exception
   */
  @Test
  public void testTopNResetsAfterFirstEmitOutcome() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(3, 30, "item3")
      .build();

    final RowSet.SingleRowSet expectedRowSet1 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(1, 10, "item1")
      .build();

    final RowSet.SingleRowSet expectedRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(3, 30, "item3")
      .addRow(2, 20, "item2")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);


    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(1, topNBatch.getRecordCount());

    // verify results with baseline
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet1.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    // State refresh happens and limit again works on new data batches
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    assertEquals(2, topNBatch.getRecordCount());

    // verify results with baseline
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet2.container());

    // Release memory for row sets
    nonEmptyInputRowSet2.clear();
    expectedRowSet2.clear();
    expectedRowSet1.clear();
  }

  /**
   * Verifies TopNBatch correctness for the case where it receives non-empty batch in build schema phase followed by
   * empty batchs with OK and EMIT outcomes.
   * @throws Exception
   */
  @Test
  public void testTopN_NonEmptyFirst_EmptyOKEmitOutcome() {
    final RowSet.SingleRowSet expectedRowSet1 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(1, 10, "item1")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.NONE);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);


    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(0, topNBatch.getRecordCount());
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(1, topNBatch.getRecordCount());

    // verify results with baseline
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet1.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    // Release memory for row set
    expectedRowSet1.clear();
  }

  /**
   * Verifies that if TopNBatch receives multiple non-empty record batch with EMIT outcome in between then it produces
   * output for those input batch correctly. In this case it receives first non-empty batch with OK_NEW_SCHEMA in
   * buildSchema phase followed by an empty batch with EMIT outcome. For this combination it produces output for the
   * record received so far along with EMIT outcome. Then it receives second non-empty batch with OK outcome and
   * produces output for it differently. The test validates that for each output received the order of the records are
   * correct.
   * @throws Exception
   */
  @Test
  public void testTopNMultipleOutputBatchWithLowerLimits() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(4, 40, "item4")
      .addRow(2, 20, "item2")
      .addRow(5, 50, "item5")
      .addRow(3, 30, "item3")
      .build();

    final RowSet.SingleRowSet expectedRowSet1 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(1, 10, "item1")
      .build();

    final RowSet.SingleRowSet expectedRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(5, 50, "item5")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 1);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);


    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(1, topNBatch.getRecordCount());

    // verify results with baseline
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet1.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    // State refresh happens and limit again works on new data batches
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    assertEquals(1, topNBatch.getRecordCount());

    // verify results with baseline
    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet2.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.NONE);

    // Release memory for row sets
    nonEmptyInputRowSet2.clear();
    expectedRowSet2.clear();
    expectedRowSet1.clear();
  }

  @Test
  public void testTopNMultipleEMITOutcome() {
    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(3, 30, "item3")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());
    inputContainer.add(emptyInputRowSet.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    // first limit evaluation
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(1, topNBatch.getRecordCount());
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    // After seeing EMIT limit will refresh it's state and again evaluate limit on next set of input batches
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    assertEquals(2, topNBatch.getRecordCount());
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    nonEmptyInputRowSet2.clear();
  }

  @Test
  public void testTopNMultipleInputToSingleOutputBatch() throws Exception {

    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .build();

    final RowSet.SingleRowSet expectedRowSet = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(2, 20, "item2")
      .addRow(1, 10, "item1")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());
    inputContainer.add(emptyInputRowSet.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 10);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(2, topNBatch.getRecordCount());

    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    nonEmptyInputRowSet2.clear();
  }

  @Test
  public void testTopNMultipleInputToMultipleOutputBatch_LowerLimits() {

    final RowSet.SingleRowSet nonEmptyInputRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(7, 70, "item7")
      .addRow(3, 30, "item3")
      .addRow(13, 130, "item13")
      .build();

    final RowSet.SingleRowSet nonEmptyInputRowSet3 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(17, 170, "item17")
      .addRow(3, 30, "item3")
      .addRow(13, 130, "item13")
      .build();

    final RowSet.SingleRowSet expectedRowSet1 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(13, 130, "item13")
      .addRow(7, 70, "item7")
      .build();

    final RowSet.SingleRowSet expectedRowSet2 = operatorFixture.rowSetBuilder(inputSchema)
      .addRow(17, 170, "item17")
      .addRow(13, 130, "item13")
      .build();

    inputContainer.add(nonEmptyInputRowSet.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet2.container());
    inputContainer.add(emptyInputRowSet.container());
    inputContainer.add(nonEmptyInputRowSet3.container());
    inputContainer.add(emptyInputRowSet.container());

    inputOutcomes.add(RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.OK);
    inputOutcomes.add(RecordBatch.IterOutcome.EMIT);

    final MockRecordBatch mockInputBatch = new MockRecordBatch(operatorFixture.getFragmentContext(), opContext,
      inputContainer, inputOutcomes, emptyInputRowSet.container().getSchema());

    final TopN topNConfig = new TopN(null,
      Lists.newArrayList(ordering("id_left", RelFieldCollation.Direction.DESCENDING,
        RelFieldCollation.NullDirection.FIRST)), false, 2);
    final TopNBatch topNBatch = new TopNBatch(topNConfig, operatorFixture.getFragmentContext(), mockInputBatch);

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK_NEW_SCHEMA);
    assertEquals(2, topNBatch.getRecordCount());

    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet1.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());
    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.OK);
    assertEquals(2, topNBatch.getRecordCount());

    verifyColumnCount(topNBatch, inputSchema.size());
    verifyBaseline(topNBatch, topNBatch.getSelectionVector4(), expectedRowSet2.container());

    assertTrue(topNBatch.next() == RecordBatch.IterOutcome.EMIT);
    assertEquals(0, topNBatch.getRecordCount());

    nonEmptyInputRowSet2.clear();
    nonEmptyInputRowSet3.clear();
    expectedRowSet1.clear();
    expectedRowSet2.clear();
  }
}
