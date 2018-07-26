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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import java.util.Iterator;
import java.util.List;

public class MockRecordBatch implements CloseableRecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MockRecordBatch.class);

  // These resources are owned by this RecordBatch
  protected VectorContainer container;
  protected SelectionVector2 sv2;
  private int currentContainerIndex;
  private int currentOutcomeIndex;
  private boolean isDone;
  private boolean limitWithUnnest;

  // All the below resources are owned by caller
  private final List<VectorContainer> allTestContainers;
  private List<SelectionVector2> allTestContainersSv2;
  private final List<IterOutcome> allOutcomes;
  private final FragmentContext context;
  protected final OperatorContext oContext;
  protected final BufferAllocator allocator;

  public MockRecordBatch(FragmentContext context, OperatorContext oContext,
                         List<VectorContainer> testContainers, List<IterOutcome> iterOutcomes,
                         BatchSchema schema) {
    this.context = context;
    this.oContext = oContext;
    this.allocator = oContext.getAllocator();
    this.allTestContainers = testContainers;
    this.container = new VectorContainer(allocator, schema);
    this.allOutcomes = iterOutcomes;
    this.currentContainerIndex = 0;
    this.currentOutcomeIndex = 0;
    this.isDone = false;
    this.allTestContainersSv2 = null;
    this.sv2 = null;
  }

  public MockRecordBatch(FragmentContext context, OperatorContext oContext,
                         List<VectorContainer> testContainers, List<IterOutcome> iterOutcomes,
                         List<SelectionVector2> testContainersSv2, BatchSchema schema) {
    this(context, oContext, testContainers, iterOutcomes, schema);
    allTestContainersSv2 = testContainersSv2;
    sv2 = (allTestContainersSv2 != null && allTestContainersSv2.size() > 0) ? new SelectionVector2(allocator) : null;
  }

  @Override
  public void close() {
    container.clear();
    container.setRecordCount(0);
    currentContainerIndex = 0;
    currentOutcomeIndex = 0;
    if (sv2 != null) {
      sv2.clear();
    }
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return sv2;
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return null;
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return container.getSchema();
  }

  @Override
  public int getRecordCount() {
    return (sv2 == null) ? container.getRecordCount() : sv2.getCount();
  }

  @Override
  public void kill(boolean sendUpstream) {
    if (!limitWithUnnest) {
      isDone = true;
      container.clear();
      container.setRecordCount(0);
      if (sv2 != null) {
        sv2.clear();
      }
    }
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return null;
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }

  @Override
  public IterOutcome next() {

    if(isDone) {
      return IterOutcome.NONE;
    }

    IterOutcome currentOutcome = IterOutcome.OK;

    if (currentContainerIndex < allTestContainers.size()) {
      final VectorContainer input = allTestContainers.get(currentContainerIndex);
      final int recordCount = input.getRecordCount();
      // We need to do this since the downstream operator expects vector reference to be same
      // after first next call in cases when schema is not changed
      final BatchSchema inputSchema = input.getSchema();
      if (!container.getSchema().isEquivalent(inputSchema)) {
        container.clear();
        container = new VectorContainer(allocator, inputSchema);
      }
      container.transferIn(input);
      container.setRecordCount(recordCount);

      // Transfer the sv2 as well
      final SelectionVector2 inputSv2 =
        (allTestContainersSv2 != null && allTestContainersSv2.size() > 0)
          ? allTestContainersSv2.get(currentContainerIndex) : null;
      if (inputSv2 != null) {
        sv2.allocateNewSafe(inputSv2.getCount());
        for (int i=0; i<inputSv2.getCount(); ++i) {
          sv2.setIndex(i, inputSv2.getIndex(i));
        }
        sv2.setRecordCount(inputSv2.getCount());
      }
    }

    if (currentOutcomeIndex < allOutcomes.size()) {
      currentOutcome = allOutcomes.get(currentOutcomeIndex);
      ++currentOutcomeIndex;
    } else {
      currentOutcome = IterOutcome.NONE;
    }

    switch (currentOutcome) {
      case OK:
      case OK_NEW_SCHEMA:
      case EMIT:
        ++currentContainerIndex;
        return currentOutcome;
      case NONE:
      case STOP:
      case OUT_OF_MEMORY:
      //case OK_NEW_SCHEMA:
        isDone = true;
        container.setRecordCount(0);
        return currentOutcome;
      case NOT_YET:
        container.setRecordCount(0);
        return currentOutcome;
      default:
        throw new UnsupportedOperationException("This state is not supported");
    }
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("MockRecordBatch doesn't support gettingWritableBatch yet");
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return container.iterator();
  }

  @Override
  public VectorContainer getContainer() { return container; }

  public boolean isCompleted() {
    return isDone;
  }

  public void useUnnestKillHandlingForLimit(boolean limitWithUnnest) {
    this.limitWithUnnest = limitWithUnnest;
  }
}
