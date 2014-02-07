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

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import javax.inject.Named;
import java.util.List;

public abstract class PriorityQueueSelectorTemplate implements PriorityQueueSelector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PriorityQueueSelectorTemplate.class);

  private SelectionVector4 sv4;
  private SelectionVector4 vector4;
  private List<BatchGroup> batchGroups;
  private FragmentContext context;
  private int size;
  private int queueSize = 0;
  private int targetRecordCount = ExternalSortBatch.TARGET_RECORD_COUNT;
  private VectorAccessible hyperBatch;

  @Override
  public void setup(FragmentContext context, VectorAccessible hyperBatch, SelectionVector4 sv4, List<BatchGroup> batchGroups) throws SchemaChangeException {
    this.context = context;
    this.sv4 = sv4;
    this.batchGroups = batchGroups;
    this.size = batchGroups.size();
    this.hyperBatch = hyperBatch;

    BufferAllocator.PreAllocator preAlloc = context.getAllocator().getNewPreAllocator();
    preAlloc.preAllocate(4 * size);
    vector4 = new SelectionVector4(preAlloc.getAllocation(), size, Character.MAX_VALUE);
    doSetup(context, hyperBatch, null);

    for (int i = 0; i < size; i++, queueSize++) {
      vector4.set(i, i * 2, batchGroups.get(i).getNextIndex());
      siftUp();
    }
  }

  @Override
  public int next() {
    if (queueSize == 0) {
      cleanup();
      return 0;
    }
    rotate();
    for (int outgoingIndex = 0; outgoingIndex < targetRecordCount; outgoingIndex++) {
      int compoundIndex = vector4.get(0);
      int batch = compoundIndex >> 16;
      int batchGroup = batch / 2; //two containers per batch group
      sv4.set(outgoingIndex, compoundIndex);
      int nextIndex = batchGroups.get(batchGroup).getNextIndex();
      batch = batchGroup * 2 + batchGroups.get(batchGroup).getBatchPointer(); // increment batch pointer if batchGroup is currently using second container
      if (nextIndex == -1) {
        vector4.set(0, vector4.get(--queueSize));
      } else if (nextIndex == -2) {
        vector4.set(0, batch - 1, 0);
        sv4.setCount(outgoingIndex);
        return outgoingIndex;
      } else {
        vector4.set(0, batch, nextIndex);
      }
      if (queueSize == 0) {
        sv4.setCount(++outgoingIndex);
        return outgoingIndex;
      }
      siftDown();
    }
    sv4.setCount(targetRecordCount);
    return targetRecordCount;
  }

  private void rotate() {
    for (BatchGroup group : batchGroups) {
      group.rotate();
    }
    for (int i = 0; i < vector4.getTotalCount(); i++) {
      vector4.set(i, vector4.get(i) & 0xFFFEFFFF);
    }
  }

  @Override
  public void cleanup() {
    vector4.clear();
  }

  private final void siftUp() {
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

  private final void siftDown() {
    int p = 0;
    int next;
    while (p * 2 + 1 < queueSize) {
      if (p * 2 + 2 >= queueSize) {
        next = p * 2 + 1;
      } else {
        if (compare(p * 2 + 1, p * 2 + 2) <= 0) {
          next = p * 2 + 1;
        } else {
          next = p * 2 + 2;
        }
      }
      if (compare(p, next) > 0) {
        swap(p, next);
        p = next;
      } else {
        break;
      }
    }
  }


  public final void swap(int sv0, int sv1) {
    int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }
  
  public final int compare(int leftIndex, int rightIndex) {
    int sv1 = vector4.get(leftIndex);
    int sv2 = vector4.get(rightIndex);
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorAccessible incoming, @Named("outgoing") VectorAccessible outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);
}
