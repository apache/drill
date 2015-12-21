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
package org.apache.drill.exec.physical.impl.xsort;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.List;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;

public abstract class PriorityQueueCopierTemplate implements PriorityQueueCopier {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PriorityQueueCopierTemplate.class);

  private SelectionVector4 vector4;
  private List<BatchGroup> batchGroups;
  private VectorAccessible hyperBatch;
  private VectorAccessible outgoing;
  private int size;
  private int queueSize = 0;

  @Override
  public void setup(FragmentContext context, BufferAllocator allocator, VectorAccessible hyperBatch, List<BatchGroup> batchGroups,
                    VectorAccessible outgoing) throws SchemaChangeException {
    this.hyperBatch = hyperBatch;
    this.batchGroups = batchGroups;
    this.outgoing = outgoing;
    this.size = batchGroups.size();

    final DrillBuf drillBuf = allocator.buffer(4 * size);
    vector4 = new SelectionVector4(drillBuf, size, Character.MAX_VALUE);
    doSetup(context, hyperBatch, outgoing);

    queueSize = 0;
    for (int i = 0; i < size; i++) {
      vector4.set(i, i, batchGroups.get(i).getNextIndex());
      siftUp();
      queueSize++;
    }
  }

  @Override
  public int next(int targetRecordCount) {
    allocateVectors(targetRecordCount);
    for (int outgoingIndex = 0; outgoingIndex < targetRecordCount; outgoingIndex++) {
      if (queueSize == 0) {
        return 0;
      }
      int compoundIndex = vector4.get(0);
      int batch = compoundIndex >>> 16;
      assert batch < batchGroups.size() : String.format("batch: %d batchGroups: %d", batch, batchGroups.size());
      doCopy(compoundIndex, outgoingIndex);
      int nextIndex = batchGroups.get(batch).getNextIndex();
      if (nextIndex < 0) {
        vector4.set(0, vector4.get(--queueSize));
      } else {
        vector4.set(0, batch, nextIndex);
      }
      if (queueSize == 0) {
        setValueCount(++outgoingIndex);
        return outgoingIndex;
      }
      siftDown();
    }
    setValueCount(targetRecordCount);
    return targetRecordCount;
  }

  private void setValueCount(int count) {
    for (VectorWrapper w: outgoing) {
      w.getValueVector().getMutator().setValueCount(count);
    }
  }

  @Override
  public void close() throws IOException {
    vector4.clear();
    for (final VectorWrapper<?> w: outgoing) {
      w.getValueVector().clear();
    }
    for (final VectorWrapper<?> w : hyperBatch) {
      w.clear();
    }

    for (BatchGroup batchGroup : batchGroups) {
      batchGroup.close();
    }
  }

  private void siftUp() {
    int p = queueSize;
    while (p > 0) {
      if (compare(p, (p - 1) / 2) < 0) {
        swap(p, (p - 1) / 2);
        p = (p - 1) / 2;
      } else {
        break;
      }
    }
  }

  private void allocateVectors(int targetRecordCount) {
    for (VectorWrapper w: outgoing) {
      AllocationHelper.allocateNew(w.getValueVector(), targetRecordCount);
    }
  }

  private void siftDown() {
    int p = 0;
    int next;
    while (p * 2 + 1 < queueSize) { // While the current node has at least one child
      if (p * 2 + 2 >= queueSize) { // if current node has only one child, then we only look at it
        next = p * 2 + 1;
      } else {
        if (compare(p * 2 + 1, p * 2 + 2) <= 0) {//if current node has two children, we must first determine which one has higher priority
          next = p * 2 + 1;
        } else {
          next = p * 2 + 2;
        }
      }
      if (compare(p, next) > 0) { // compare current node to highest priority child and swap if necessary
        swap(p, next);
        p = next;
      } else {
        break;
      }
    }
  }

  public void swap(int sv0, int sv1) {
    int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }

  public int compare(int leftIndex, int rightIndex) {
    int sv1 = vector4.get(leftIndex);
    int sv2 = vector4.get(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);
  public abstract void doCopy(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);

}
