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
package org.apache.drill.exec.physical.impl.TopN;

import io.netty.buffer.DrillBuf;

import java.util.concurrent.TimeUnit;

import javax.inject.Named;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.base.Stopwatch;

public abstract class PriorityQueueTemplate implements PriorityQueue {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PriorityQueueTemplate.class);

  private SelectionVector4 heapSv4; //This holds the heap
  private SelectionVector4 finalSv4; //This is for final sorted output
  private ExpandableHyperContainer hyperBatch;
  private FragmentContext context;
  private BufferAllocator allocator;
  private int limit;
  private int queueSize = 0;
  private int batchCount = 0;
  private boolean hasSv2;

  @Override
  public void init(int limit, FragmentContext context, BufferAllocator allocator,  boolean hasSv2) throws SchemaChangeException {
    this.limit = limit;
    this.context = context;
    this.allocator = allocator;
    final DrillBuf drillBuf = allocator.buffer(4 * (limit + 1));
    heapSv4 = new SelectionVector4(drillBuf, limit, Character.MAX_VALUE);
    this.hasSv2 = hasSv2;
  }

  @Override
  public void resetQueue(VectorContainer container, SelectionVector4 v4) throws SchemaChangeException {
    assert container.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.FOUR_BYTE;
    BatchSchema schema = container.getSchema();
    VectorContainer newContainer = new VectorContainer();
    for (MaterializedField field : schema) {
      int[] ids = container.getValueVectorId(SchemaPath.getSimplePath(field.getPath())).getFieldIds();
      newContainer.add(container.getValueAccessorById(field.getValueClass(), ids).getValueVectors());
    }
    newContainer.buildSchema(BatchSchema.SelectionVectorMode.FOUR_BYTE);
    // Cleanup before recreating hyperbatch and sv4.
    cleanup();
    hyperBatch = new ExpandableHyperContainer(newContainer);
    batchCount = hyperBatch.iterator().next().getValueVectors().length;
    final DrillBuf drillBuf = allocator.buffer(4 * (limit + 1));
    heapSv4 = new SelectionVector4(drillBuf, limit, Character.MAX_VALUE);
    // Reset queue size (most likely to be set to limit).
    queueSize = 0;
    for (int i = 0; i < v4.getTotalCount(); i++) {
      heapSv4.set(i, v4.get(i));
      ++queueSize;
    }
    v4.clear();
    doSetup(context, hyperBatch, null);
  }

  @Override
  public void add(FragmentContext context, RecordBatchData batch) throws SchemaChangeException{
    Stopwatch watch = Stopwatch.createStarted();
    if (hyperBatch == null) {
      hyperBatch = new ExpandableHyperContainer(batch.getContainer());
    } else {
      hyperBatch.addBatch(batch.getContainer());
    }

    doSetup(context, hyperBatch, null); // may not need to do this every time

    int count = 0;
    SelectionVector2 sv2 = null;
    if (hasSv2) {
      sv2 = batch.getSv2();
    }
    for (; queueSize < limit && count < batch.getRecordCount();  count++) {
      heapSv4.set(queueSize, batchCount, hasSv2 ? sv2.getIndex(count) : count);
      queueSize++;
      siftUp();
    }
    for (; count < batch.getRecordCount(); count++) {
      heapSv4.set(limit, batchCount, hasSv2 ? sv2.getIndex(count) : count);
      if (compare(limit, 0) < 0) {
        swap(limit, 0);
        siftDown();
      }
    }
    batchCount++;
    if (hasSv2) {
      sv2.clear();
    }
    logger.debug("Took {} us to add {} records", watch.elapsed(TimeUnit.MICROSECONDS), count);
  }

  @Override
  public void generate() throws SchemaChangeException {
    Stopwatch watch = Stopwatch.createStarted();
    final DrillBuf drillBuf = allocator.buffer(4 * queueSize);
    finalSv4 = new SelectionVector4(drillBuf, queueSize, 4000);
    for (int i = queueSize - 1; i >= 0; i--) {
      finalSv4.set(i, pop());
    }
    logger.debug("Took {} us to generate output of {}", watch.elapsed(TimeUnit.MICROSECONDS), finalSv4.getTotalCount());
  }

  @Override
  public VectorContainer getHyperBatch() {
    return hyperBatch;
  }

  @Override
  public SelectionVector4 getHeapSv4() {
    return heapSv4;
  }

  @Override
  public SelectionVector4 getFinalSv4() {
    return finalSv4;
  }

  @Override
  public void cleanup() {
    if (heapSv4 != null) {
      heapSv4.clear();
    }
    if (hyperBatch != null) {
      hyperBatch.clear();
    }
    if (finalSv4 != null) {
      finalSv4.clear();
    }
  }

  private void siftUp() {
    int p = queueSize - 1;
    while (p > 0) {
      if (compare(p, (p - 1) / 2) > 0) {
        swap(p, (p - 1) / 2);
        p = (p - 1) / 2;
      } else {
        break;
      }
    }
  }

  private void siftDown() {
    int p = 0;
    int next;
    while (p * 2 + 1 < queueSize) {
      if (p * 2 + 2 >= queueSize) {
        next = p * 2 + 1;
      } else {
        if (compare(p * 2 + 1, p * 2 + 2) >= 0) {
          next = p * 2 + 1;
        } else {
          next = p * 2 + 2;
        }
      }
      if (compare(p, next) < 0) {
        swap(p, next);
        p = next;
      } else {
        break;
      }
    }
  }

  public int pop() {
    int value = heapSv4.get(0);
    swap(0, queueSize - 1);
    queueSize--;
    siftDown();
    return value;
  }

  public void swap(int sv0, int sv1) {
    int tmp = heapSv4.get(sv0);
    heapSv4.set(sv0, heapSv4.get(sv1));
    heapSv4.set(sv1, tmp);
  }

  public int compare(int leftIndex, int rightIndex) {
    int sv1 = heapSv4.get(leftIndex);
    int sv2 = heapSv4.get(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorContainer incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);

}
