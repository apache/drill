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
package org.apache.drill.exec.physical.impl.xsort.managed;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.cache.VectorSerializer;
import org.apache.drill.exec.cache.VectorSerializer.Writer;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

/**
 * Represents a group of batches spilled to disk.
 * <p>
 * The batches are defined by a schema which can change over time. When the schema changes,
 * all existing and new batches are coerced into the new schema. Provides a
 * uniform way to iterate over records for one or more batches whether
 * the batches are in memory or on disk.
 * <p>
 * The <code>BatchGroup</code> operates in two modes as given by the two
 * subclasses:
 * <ul>
 * <li>Input mode (@link InputBatchGroup): Used to buffer in-memory batches
 * prior to spilling.</li>
 * <li>Spill mode (@link SpilledBatchGroup): Holds a "memento" to a set
 * of batches written to disk. Acts as both a reader and writer for
 * those batches.</li>
 */

public abstract class BatchGroup implements VectorAccessible, AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BatchGroup.class);

  /**
   * The input batch group gathers batches buffered in memory before
   * spilling. The structure of the data is:
   * <ul>
   * <li>Contains a single batch received from the upstream (input)
   * operator.</li>
   * <li>Associated selection vector that provides a sorted
   * indirection to the values in the batch.</li>
   * </ul>
   */

  public static class InputBatch extends BatchGroup {
    private final SelectionVector2 sv2;
    private final long dataSize;

    public InputBatch(VectorContainer container, SelectionVector2 sv2, BufferAllocator allocator, long dataSize) {
      super(container, allocator);
      this.sv2 = sv2;
      this.dataSize = dataSize;
    }

    public SelectionVector2 getSv2() { return sv2; }

    public long getDataSize() { return dataSize; }

    @Override
    public int getRecordCount() {
      if (sv2 != null) {
        return sv2.getCount();
      } else {
        return super.getRecordCount();
      }
    }

    @Override
    public int getNextIndex() {
      int val = super.getNextIndex();
      if (val == -1) {
        return val;
      }
      return sv2.getIndex(val);
    }

    @Override
    public void close() throws IOException {
      if (sv2 != null) {
        sv2.clear();
      }
      super.close();
    }
  }

  /**
   * Holds a set of spilled batches, represented by a file on disk.
   * Handles reads from, and writes to the spill file. The data structure
   * is:
   * <ul>
   * <li>A pointer to a file that contains serialized batches.</li>
   * <li>When writing, each batch is appended to the output file.</li>
   * <li>When reading, iterates over each spilled batch, and for each
   * of those, each spilled record.</li>
   * </ul>
   * <p>
   * Starts out with no current batch. Defines the current batch to be the
   * (shell: schema without data) of the last batch spilled to disk.
   * <p>
   * When reading, has destructive read-once behavior: closing the
   * batch (after reading) deletes the underlying spill file.
   * <p>
   * This single class does three tasks: load data, hold data and
   * read data. This should be split into three separate classes. But,
   * the original (combined) structure is retained for expedience at
   * present.
   */

  public static class SpilledRun extends BatchGroup {
    private InputStream inputStream;
    private String path;
    private SpillSet spillSet;
    private BufferAllocator allocator;
    private int spilledBatches;
    private long batchSize;
    private Writer writer;
    private VectorSerializer.Reader reader;

    public SpilledRun(SpillSet spillSet, String path, BufferAllocator allocator) throws IOException {
      super(null, allocator);
      this.spillSet = spillSet;
      this.path = path;
      this.allocator = allocator;
      writer = spillSet.writer(path);
    }

    public void addBatch(VectorContainer newContainer) throws IOException {
      writer.write(newContainer);
      newContainer.zeroVectors();
      logger.trace("Wrote {} records in {} us", newContainer.getRecordCount(), writer.time(TimeUnit.MICROSECONDS));
      spilledBatches++;

      // Hold onto the husk of the last added container so that we have a
      // current container when starting to read rows back later.

      currentContainer = newContainer;
      currentContainer.setRecordCount(0);
    }

    public void setBatchSize(long batchSize) {
      this.batchSize = batchSize;
    }

    public long getBatchSize() { return batchSize; }
    public String getPath() { return path; }

    @Override
    public int getNextIndex() {
      if (pointer == getRecordCount()) {
        if (spilledBatches == 0) {
          return -1;
        }
        try {
          currentContainer.zeroVectors();
          getBatch();
        } catch (IOException e) {
          // Release any partially-loaded data.
          currentContainer.clear();
          throw UserException.dataReadError(e)
              .message("Failure while reading spilled data")
              .build(logger);
        }

        // The pointer indicates the NEXT index, not the one we
        // return here. At this point, we just started reading a
        // new batch and have returned index 0. So, the next index
        // is 1.

        pointer = 1;
        return 0;
      }
      return super.getNextIndex();
    }

    private VectorContainer getBatch() throws IOException {
      if (inputStream == null) {
        inputStream = spillSet.openForInput(path);
        reader = VectorSerializer.reader(allocator, inputStream);
      }
      Stopwatch watch = Stopwatch.createStarted();
      long start = allocator.getAllocatedMemory();
      VectorContainer c =  reader.read();
      long end = allocator.getAllocatedMemory();
      logger.trace("Read {} records in {} us; size = {}, memory = {}",
                   c.getRecordCount(),
                   watch.elapsed(TimeUnit.MICROSECONDS),
                   (end - start), end);
      if (schema != null) {
        c = SchemaUtil.coerceContainer(c, schema, allocator);
      }
      spilledBatches--;
      currentContainer.zeroVectors();
      Iterator<VectorWrapper<?>> wrapperIterator = c.iterator();
      for (VectorWrapper<?> w : currentContainer) {
        TransferPair pair = wrapperIterator.next().getValueVector().makeTransferPair(w.getValueVector());
        pair.transfer();
      }
      currentContainer.setRecordCount(c.getRecordCount());
      c.zeroVectors();
      return c;
    }

    /**
     * Close resources owned by this batch group. Each can fail; report
     * only the first error. This is cluttered because this class tries
     * to do multiple tasks. TODO: Split into multiple classes.
     */

    @Override
    public void close() throws IOException {
      IOException ex = null;
      try {
        super.close();
      } catch (IOException e) {
        ex = e;
      }
      try {
        closeWriter();
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      try {
        closeInputStream();
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      try {
        spillSet.delete(path);
      } catch (IOException e) {
        ex = ex == null ? e : ex;
      }
      if (ex != null) {
        throw ex;
      }
    }

    private void closeInputStream() throws IOException {
      if (inputStream == null) {
        return;
      }
      long readLength = spillSet.getPosition(inputStream);
      spillSet.tallyReadBytes(readLength);
      inputStream.close();
      inputStream = null;
      reader = null;
      logger.trace("Summary: Read {} bytes from {}", readLength, path);
    }

    public void closeWriter() throws IOException {
      if (writer != null) {
        spillSet.close(writer);
        logger.trace("Summary: Wrote {} bytes in {} us to {}", writer.getBytesWritten(), writer.time(TimeUnit.MICROSECONDS), path);
        writer = null;
      }
    }
  }

  protected VectorContainer currentContainer;
  protected int pointer = 0;
  protected final BufferAllocator allocator;
  protected BatchSchema schema;

  public BatchGroup(VectorContainer container, BufferAllocator allocator) {
    this.currentContainer = container;
    this.allocator = allocator;
  }

  /**
   * Updates the schema for this batch group. The current as well as any
   * deserialized batches will be coerced to this schema.
   * @param schema
   */
  public void setSchema(BatchSchema schema) {
    currentContainer = SchemaUtil.coerceContainer(currentContainer, schema, allocator);
    this.schema = schema;
  }

  public int getNextIndex() {
    if (pointer == getRecordCount()) {
      return -1;
    }
    int val = pointer++;
    assert val < currentContainer.getRecordCount();
    return val;
  }

  public VectorContainer getContainer() {
    return currentContainer;
  }

  @Override
  public void close() throws IOException {
    currentContainer.zeroVectors();
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
    return currentContainer.getRecordCount();
  }

  public int getUnfilteredRecordCount() {
    return currentContainer.getRecordCount();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return currentContainer.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  public static void closeAll(Collection<? extends BatchGroup> groups) {
    Exception ex = null;
    for (BatchGroup group: groups) {
      try {
        group.close();
      } catch (Exception e) {
        ex = (ex == null) ? e : ex;
      }
    }
    if (ex != null) {
      throw UserException.dataWriteError(ex)
          .message("Failure while flushing spilled data")
          .build(logger);
    }
  }
}
