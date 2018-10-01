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
package org.apache.drill.exec.physical.impl.xsort;

import com.typesafe.config.ConfigException;
import io.netty.buffer.DrillBuf;

import java.util.Queue;

import javax.inject.Named;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.util.IndexedSortable;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Queues;

public abstract class MSortTemplate implements MSorter, IndexedSortable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MSortTemplate.class);

  private SelectionVector4 vector4;
  private SelectionVector4 aux;
  private Queue<Integer> runStarts = Queues.newLinkedBlockingQueue();
  private FragmentContext context;

  /**
   * This is only useful for debugging and/or unit testing. Controls the maximum size of batches exposed to downstream
   */
  private int desiredRecordBatchCount;

  @Override
  public void setup(final FragmentContext context, final BufferAllocator allocator, final SelectionVector4 vector4, final VectorContainer hyperBatch) throws SchemaChangeException{
    // we pass in the local hyperBatch since that is where we'll be reading data.
    Preconditions.checkNotNull(vector4);
    this.vector4 = vector4.createNewWrapperCurrent();
    this.context = context;
    vector4.clear();
    doSetup(context, hyperBatch, null);
    runStarts.add(0);
    int batch = 0;
    final int totalCount = this.vector4.getTotalCount();
    for (int i = 0; i < totalCount; i++) {
      final int newBatch = this.vector4.get(i) >>> 16;
      if (newBatch == batch) {
        continue;
      } else if (newBatch == batch + 1) {
        runStarts.add(i);
        batch = newBatch;
      } else {
        throw new UnsupportedOperationException(String.format("Missing batch. batch: %d newBatch: %d", batch, newBatch));
      }
    }
    @SuppressWarnings("resource")
    final DrillBuf drillBuf = allocator.buffer(4 * totalCount);

    try {
      desiredRecordBatchCount = context.getConfig().getInt(ExecConstants.EXTERNAL_SORT_MSORT_MAX_BATCHSIZE);
    } catch(ConfigException.Missing e) {
      // value not found, use default value instead
      desiredRecordBatchCount = ValueVector.MAX_ROW_COUNT;
    }
    aux = new SelectionVector4(drillBuf, totalCount, desiredRecordBatchCount);
  }

  /**
   * For given recordCount how much memory does MSorter needs for its own purpose. This is used in
   * ExternalSortBatch to make decisions about whether to spill or not.
   *
   * @param recordCount
   * @return The amount of memory MSorter needs for a given record count.
   */
  public static long memoryNeeded(final int recordCount) {
    // We need 4 bytes (SV4) for each record, power of 2 rounded.

    return BaseAllocator.nextPowerOfTwo(recordCount * 4);
  }

  private int merge(final int leftStart, final int rightStart, final int rightEnd, final int outStart) {
    int l = leftStart;
    int r = rightStart;
    int o = outStart;
    while (l < rightStart && r < rightEnd) {
      if (compare(l, r) <= 0) {
        aux.set(o++, vector4.get(l++));
      } else {
        aux.set(o++, vector4.get(r++));
      }
    }
    while (l < rightStart) {
      aux.set(o++, vector4.get(l++));
    }
    while (r < rightEnd) {
      aux.set(o++, vector4.get(r++));
    }
    assert o == outStart + (rightEnd - leftStart);
    return o;
  }

  @Override
  public SelectionVector4 getSV4() {
    return vector4;
  }

  @Override
  public void sort(final VectorContainer container) {
    while (runStarts.size() > 1) {

      // check if we're cancelled/failed frequently
      if (!context.getExecutorState().shouldContinue()) {
        return;
      }

      int outIndex = 0;
      final Queue<Integer> newRunStarts = Queues.newLinkedBlockingQueue();
      newRunStarts.add(outIndex);
      final int size = runStarts.size();
      for (int i = 0; i < size / 2; i++) {
        final int left = runStarts.poll();
        final int right = runStarts.poll();
        Integer end = runStarts.peek();
        if (end == null) {
          end = vector4.getTotalCount();
        }
        outIndex = merge(left, right, end, outIndex);
        if (outIndex < vector4.getTotalCount()) {
          newRunStarts.add(outIndex);
        }
      }
      if (outIndex < vector4.getTotalCount()) {
        copyRun(outIndex, vector4.getTotalCount());
      }
      @SuppressWarnings("resource")
      final SelectionVector4 tmp = aux.createNewWrapperCurrent(desiredRecordBatchCount);
      aux.clear();
      aux = vector4.createNewWrapperCurrent(desiredRecordBatchCount);
      vector4.clear();
      vector4 = tmp.createNewWrapperCurrent(desiredRecordBatchCount);
      tmp.clear();
      runStarts = newRunStarts;
    }
    aux.clear();
  }

  private void copyRun(final int start, final int end) {
    for (int i = start; i < end; i++) {
      aux.set(i, vector4.get(i));
    }
  }

  @Override
  public void swap(final int sv0, final int sv1) {
    final int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }

  @Override
  public int compare(final int leftIndex, final int rightIndex) {
    final int sv1 = vector4.get(leftIndex);
    final int sv2 = vector4.get(rightIndex);
    try {
      return doEval(sv1, sv2);
    } catch (SchemaChangeException e) {
      throw new UnsupportedOperationException(e);
    }
  }

  @Override
  public void clear() {
    if(vector4 != null) {
      vector4.clear();
    }

    if(aux != null) {
      aux.clear();
    }
  }

  public abstract void doSetup(@Named("context") FragmentContext context,
                               @Named("incoming") VectorContainer incoming,
                               @Named("outgoing") RecordBatch outgoing)
                       throws SchemaChangeException;
  public abstract int doEval(@Named("leftIndex") int leftIndex,
                             @Named("rightIndex") int rightIndex)
                      throws SchemaChangeException;

  @Override
  public String toString() {
    return "MSortTemplate[vector4=" + vector4
        + ", aux=" + aux
        + ", runStarts=" + runStarts
        + ", desiredRecordBatchCount=" + desiredRecordBatchCount
        + "]";
  }
}
