/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.drill.exec.physical.impl.unnest;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.base.LateralContract;
import org.apache.drill.exec.physical.impl.MockRecordBatch;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mock lateral join implementation for testing unnest. This ignores all the other input and
 * simply puts the unnest output into a results hypervector.
 * Since Unnest returns an empty batch when it encounters a schema change, this implementation
 * will also output an empty batch when it sees a schema change from unnest
 */
public class MockLateralJoinBatch implements LateralContract, CloseableRecordBatch {

  private RecordBatch incoming;

  private int recordIndex = 0;
  private RecordBatch unnest;

  private boolean isDone;

  private final FragmentContext context;
  private  final OperatorContext oContext;

  private List<ValueVector> resultList = new ArrayList<>();

  public MockLateralJoinBatch(FragmentContext context, OperatorContext oContext, RecordBatch incoming) {
    this.context = context;
    this.oContext = oContext;
    this.incoming = incoming;
    this.isDone = false;
  }

  @Override public RecordBatch getIncoming() {
    return incoming; // don't need this
  }

  @Override public int getRecordIndex() {
    return recordIndex;
  }

  /**
   * TODO: Update based on the requirement.
   * @return
   */
  @Override
  public IterOutcome getLeftOutcome() {
    return IterOutcome.OK;
  }

  public void moveToNextRecord() {
    recordIndex++;
  }

  public void reset() {
    recordIndex = 0;
  }

  public void setUnnest(RecordBatch unnest){
    this.unnest = unnest;
  }

  public RecordBatch getUnnest() {
    return unnest;
  }

  public IterOutcome next() {

    IterOutcome currentOutcome = incoming.next();
    recordIndex = 0;

    switch (currentOutcome) {
      case OK_NEW_SCHEMA:
        // Nothing to do for this.
      case OK:
        IterOutcome outcome;
        // consume all the outout from unnest until EMIT or end of
        // incoming data
        while (recordIndex < incoming.getRecordCount()) {
          outcome = unnest.next();
          if (outcome == IterOutcome.OK_NEW_SCHEMA) {
            // setup schema does nothing (this is just a place holder)
            setupSchema();
            // however unnest is also expected to return an empty batch
            // which we will add to our output
          }
          // We put each batch output from unnest into a hypervector
          // the calling test can match this against the baseline
          addBatchToHyperContainer(unnest);

          if (outcome == IterOutcome.EMIT) {
            moveToNextRecord();
          }
        }
        return currentOutcome;
      case NONE:
      case STOP:
      case OUT_OF_MEMORY:
        isDone = true;
        return currentOutcome;
      case NOT_YET:
        return currentOutcome;
      default:
        throw new UnsupportedOperationException("This state is not supported");
    }
  }

  @Override public WritableBatch getWritableBatch() {
    return null;
  }

  public List<ValueVector> getResultList() {
    return resultList;
  }

  @Override
  public void close() throws Exception {

  }

  @Override public int getRecordCount() {
    return 0;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return null;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override public BatchSchema getSchema() {
    return null;
  }

  @Override public void kill(boolean sendUpstream) {

  }

  @Override public VectorContainer getOutgoingContainer() {
    return null;
  }

  @Override public TypedFieldId getValueVectorId(SchemaPath path) {
    return null;
  }

  @Override public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return null;
  }

  private void setupSchema(){
    // Nothing to do in this test
    return;
  }

  public boolean isCompleted() {
    return isDone;
  }

  private void addBatchToHyperContainer(RecordBatch inputBatch) {
    final RecordBatchData batchCopy = new RecordBatchData(inputBatch, oContext.getAllocator());
    boolean success = false;
    try {
      for (VectorWrapper<?> w : batchCopy.getContainer()) {
        resultList.add(w.getValueVector());
      }
      success = true;
    } finally {
      if (!success) {
        batchCopy.clear();
      }
    }
  }


  @Override public Iterator<VectorWrapper<?>> iterator() {
    return null;
  }
}
