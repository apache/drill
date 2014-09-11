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

import java.util.Queue;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.hadoop.util.IndexedSortable;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;

public abstract class MSortTemplate implements MSorter, IndexedSortable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MSortTemplate.class);

  private BufferAllocator allocator;
  private SelectionVector4 vector4;
  private SelectionVector4 aux;
  private long compares;
  private Queue<Integer> runStarts = Queues.newLinkedBlockingQueue();
  private Queue<Integer> newRunStarts;


  @Override
  public void setup(FragmentContext context, BufferAllocator allocator, SelectionVector4 vector4, VectorContainer hyperBatch) throws SchemaChangeException{
    this.allocator = allocator;
    // we pass in the local hyperBatch since that is where we'll be reading data.
    Preconditions.checkNotNull(vector4);
    this.vector4 = vector4.createNewWrapperCurrent();
    vector4.clear();
    doSetup(context, hyperBatch, null);
    runStarts.add(0);
    int batch = 0;
    for (int i = 0; i < this.vector4.getTotalCount(); i++) {
      int newBatch = this.vector4.get(i) >>> 16;
      if (newBatch == batch) {
        continue;
      } else if(newBatch == batch + 1) {
        runStarts.add(i);
        batch = newBatch;
      } else {
        throw new UnsupportedOperationException("Missing batch");
      }
    }
    BufferAllocator.PreAllocator preAlloc = allocator.getNewPreAllocator();
    preAlloc.preAllocate(4 * this.vector4.getTotalCount());
    aux = new SelectionVector4(preAlloc.getAllocation(), this.vector4.getTotalCount(), Character.MAX_VALUE);
  }

  private int merge(int leftStart, int rightStart, int rightEnd, int outStart) {
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
  public void sort(VectorContainer container) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    while (runStarts.size() > 1) {
      int outIndex = 0;
      newRunStarts = Queues.newLinkedBlockingQueue();
      newRunStarts.add(outIndex);
      int size = runStarts.size();
      for (int i = 0; i < size / 2; i++) {
        int left = runStarts.poll();
        int right = runStarts.poll();
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
      SelectionVector4 tmp = aux.createNewWrapperCurrent();
      aux.clear();
      aux = this.vector4.createNewWrapperCurrent();
      vector4.clear();
      this.vector4 = tmp.createNewWrapperCurrent();
      tmp.clear();
      runStarts = newRunStarts;
    }
    aux.clear();
  }

  private void copyRun(int start, int end) {
    for (int i = start; i < end; i++) {
      aux.set(i, vector4.get(i));
    }
  }

  @Override
  public void swap(int sv0, int sv1) {
    int tmp = vector4.get(sv0);
    vector4.set(sv0, vector4.get(sv1));
    vector4.set(sv1, tmp);
  }

  @Override
  public int compare(int leftIndex, int rightIndex) {
    int sv1 = vector4.get(leftIndex);
    int sv2 = vector4.get(rightIndex);
    compares++;
    return doEval(sv1, sv2);
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") VectorContainer incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract int doEval(@Named("leftIndex") int leftIndex, @Named("rightIndex") int rightIndex);

}
