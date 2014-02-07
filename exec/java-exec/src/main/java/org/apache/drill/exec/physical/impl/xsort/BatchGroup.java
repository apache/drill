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

import com.google.common.base.Stopwatch;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.*;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class BatchGroup implements VectorAccessible {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchGroup.class);

  private final VectorContainer firstContainer;
  private final VectorContainer secondContainer;
  private VectorContainer currentContainer;
  private SelectionVector2 sv2;
  private int pointer = 0;
  private int batchPointer = 0;
  private boolean hasSecond = false;
  private FSDataInputStream inputStream;
  private FSDataOutputStream outputStream;
  private Path path;
  private FileSystem fs;
  private BufferAllocator allocator;
  private int spilledBatches = 0;
  private boolean done = false;

  public BatchGroup(VectorContainer container, SelectionVector2 sv2) {
    this.firstContainer = container;
    this.secondContainer = null;
    this.sv2 = sv2;
    this.currentContainer = firstContainer;
  }

  public BatchGroup(VectorContainer firstContainer, VectorContainer secondContainer, FileSystem fs, String path, BufferAllocator allocator) {
    assert firstContainer.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.NONE;
    assert secondContainer.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.NONE;

    this.firstContainer = firstContainer;
    this.secondContainer = secondContainer;
    currentContainer = firstContainer;
    this.hasSecond = true;
    this.fs = fs;
    this.path = new Path(path);
    this.allocator = allocator;
  }

  public SelectionVector2 getSv2() {
    return sv2;
  }

  public void addBatch(VectorContainer newContainer) throws IOException {
    assert fs != null;
    assert path != null;
    if (outputStream == null) {
      outputStream = fs.create(path);
    }
    int recordCount = newContainer.getRecordCount();
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(recordCount, newContainer, false);
    VectorAccessibleSerializable outputBatch = new VectorAccessibleSerializable(batch, allocator);
    Stopwatch watch = new Stopwatch();
    watch.start();
    outputBatch.writeToStream(outputStream);
    newContainer.zeroVectors();
    logger.debug("Took {} us to spill {} records", watch.elapsed(TimeUnit.MICROSECONDS), recordCount);
    spilledBatches++;
  }

  private VectorContainer getBatch() throws IOException {
    assert fs != null;
    assert path != null;
    if (inputStream == null) {
      inputStream = fs.open(path);
    }
    VectorAccessibleSerializable vas = new VectorAccessibleSerializable(allocator);
    Stopwatch watch = new Stopwatch();
    watch.start();
    vas.readFromStream(inputStream);
    VectorContainer c = (VectorContainer) vas.get();
    logger.debug("Took {} us to read {} records", watch.elapsed(TimeUnit.MICROSECONDS), c.getRecordCount());
    spilledBatches--;
    return c;
  }

  public int getNextIndex() {
    if (pointer == currentContainer.getRecordCount()) {
      if (!hasSecond || (batchPointer == 1 && spilledBatches == 0)) {
        return -1;
      } else if (batchPointer == 1 && spilledBatches > 0) {
        return -2;
      }
      batchPointer++;
      currentContainer = secondContainer;
      pointer = 0;
    }
    if (sv2 == null) {
      int val = pointer;
      pointer++;
      assert val < currentContainer.getRecordCount();
      return val;
    } else {
      int val = pointer;
      pointer++;
      assert val < currentContainer.getRecordCount();
      return sv2.getIndex(val);
    }
  }

  public VectorContainer getFirstContainer() {
    return firstContainer;
  }

  public VectorContainer getSecondContainer() {
    return secondContainer;
  }

  public boolean hasSecond() {
    return hasSecond;
  }

  public void rotate() {
    if (batchPointer == 0) {
      return;
    }
    if (pointer == secondContainer.getRecordCount()) {
      try {
        getTwoBatches();
        return;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    firstContainer.zeroVectors();
    Iterator<VectorWrapper<?>> wrapperIterator = secondContainer.iterator();
    for (VectorWrapper w : firstContainer) {
      TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
      pair.transfer();
    }
    firstContainer.setRecordCount(secondContainer.getRecordCount());

    if (spilledBatches > 0) {
      VectorContainer c = null;
      try {
        c = getBatch();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      secondContainer.zeroVectors();
      wrapperIterator = c.iterator();
      for (VectorWrapper w : secondContainer) {
        TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
        pair.transfer();
      }
      secondContainer.setRecordCount(c.getRecordCount());
      c.zeroVectors();
    } else {
      secondContainer.zeroVectors();
      hasSecond = false;
    }
    batchPointer = 0;
    currentContainer = firstContainer;
  }

  private void getTwoBatches() throws IOException {
    firstContainer.zeroVectors();
    if (spilledBatches > 0) {
      VectorContainer c = getBatch();
      Iterator<VectorWrapper<?>> wrapperIterator = c.iterator();
      for (VectorWrapper w : firstContainer) {
        TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
        pair.transfer();
      }
      firstContainer.setRecordCount(c.getRecordCount());
      c.zeroVectors();
    } else {
      batchPointer = -1;
      pointer = -1;
      firstContainer.zeroVectors();
      secondContainer.zeroVectors();
    }
    if (spilledBatches > 0) {
      VectorContainer c = getBatch();
      Iterator<VectorWrapper<?>> wrapperIterator = c.iterator();
      for (VectorWrapper w : secondContainer) {
        TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
        pair.transfer();
      }
      secondContainer.setRecordCount(c.getRecordCount());
      c.zeroVectors();
    } else {
      secondContainer.zeroVectors();
      hasSecond = false;
    }
    batchPointer = 0;
    currentContainer = firstContainer;
    pointer = 0;
//    BatchPrinter.printBatch(firstContainer);
//    BatchPrinter.printBatch(secondContainer);
    return;
  }

  public int getBatchPointer() {
    assert batchPointer < 2;
    return batchPointer;
  }

  public void cleanup() throws IOException {
    if (sv2 != null) sv2.clear();
    if (outputStream != null) outputStream.close();
    if (inputStream != null) inputStream.close();
    if (fs != null && fs.exists(path)) fs.delete(path, false);
  }

  public void closeOutputStream() throws IOException {
    if (outputStream != null) outputStream.close();
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return currentContainer.getValueAccessorById(fieldId, clazz);
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return currentContainer.getValueVectorId(path);
  }

  @Override
  public BatchSchema getSchema() {
    return currentContainer.getSchema();
  }

  @Override
  public int getRecordCount() {
    return currentContainer.getRecordCount();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return currentContainer.iterator();
  }
}
