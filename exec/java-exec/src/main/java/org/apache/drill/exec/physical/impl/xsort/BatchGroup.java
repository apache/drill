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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Stopwatch;

public class BatchGroup implements VectorAccessible {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchGroup.class);

  private VectorContainer currentContainer;
  private SelectionVector2 sv2;
  private int pointer = 0;
  private FSDataInputStream inputStream;
  private FSDataOutputStream outputStream;
  private Path path;
  private FileSystem fs;
  private BufferAllocator allocator;
  private int spilledBatches = 0;

  public BatchGroup(VectorContainer container, SelectionVector2 sv2) {
    this.sv2 = sv2;
    this.currentContainer = container;
  }

  public BatchGroup(VectorContainer container, FileSystem fs, String path, BufferAllocator allocator) {
    currentContainer = container;
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
    VectorContainer c =  vas.get();
//    logger.debug("Took {} us to read {} records", watch.elapsed(TimeUnit.MICROSECONDS), c.getRecordCount());
    spilledBatches--;
    currentContainer.zeroVectors();
    Iterator<VectorWrapper<?>> wrapperIterator = c.iterator();
    for (VectorWrapper w : currentContainer) {
      TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
      pair.transfer();
    }
    currentContainer.setRecordCount(c.getRecordCount());
    c.zeroVectors();
    return c;
  }

  public int getNextIndex() {
    int val;
    if (pointer == getRecordCount()) {
      if (spilledBatches == 0) {
        return -1;
      }
      try {
        currentContainer.zeroVectors();
        getBatch();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      pointer = 1;
      return 0;
    }
    if (sv2 == null) {
      val = pointer;
      pointer++;
      assert val < currentContainer.getRecordCount();
    } else {
      val = pointer;
      pointer++;
      assert val < currentContainer.getRecordCount();
      val = sv2.getIndex(val);
    }

    return val;
  }

  public VectorContainer getContainer() {
    return currentContainer;
  }

  public void cleanup() throws IOException {
    if (sv2 != null) {
      sv2.clear();
    }
    if (outputStream != null) {
      outputStream.close();
    }
    if (inputStream != null) {
      inputStream.close();
    }
    if (fs != null && fs.exists(path)) {
      fs.delete(path, false);
    }
  }

  public void closeOutputStream() throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return currentContainer.getValueAccessorById(clazz, ids);
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
    if (sv2 != null) {
      return sv2.getCount();
    } else {
      return currentContainer.getRecordCount();
    }
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return currentContainer.iterator();
  }

}
