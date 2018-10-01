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
package org.apache.drill.exec.record.selection;

import io.netty.buffer.ByteBuf;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.DeadBuf;

public class SelectionVector4 implements AutoCloseable {

  private ByteBuf data;
  private int recordCount;
  private int start;
  private int length;

  public SelectionVector4(ByteBuf vector, int recordCount, int batchRecordCount) throws SchemaChangeException {
    if (recordCount > Integer.MAX_VALUE / 4) {
      throw new SchemaChangeException(String.format("Currently, Drill can only support allocations up to 2gb in size. " +
          "You requested an allocation of %d bytes.", recordCount * 4L));
    }
    this.recordCount = recordCount;
    this.start = 0;
    this.length = Math.min(batchRecordCount, recordCount);
    this.data = vector;
  }

  public SelectionVector4(BufferAllocator allocator, int recordCount) {
    if (recordCount > Integer.MAX_VALUE / 4) {
      throw new IllegalStateException(String.format("Currently, Drill can only support allocations up to 2gb in size. " +
          "You requested an allocation of %d bytes.", recordCount * 4L));
    }
    this.recordCount = recordCount;
    this.start = 0;
    this.length = recordCount;
    this.data = allocator.buffer(recordCount * 4);
  }

  public int getTotalCount() {
    return recordCount;
  }

  public int getCount() {
    return length;
  }

  private ByteBuf getData() {
    return data;
  }

  public void setCount(int length) {
    this.length = length;
    this.recordCount = length;
  }

  public void set(int index, int compound) {
    data.setInt(index * 4, compound);
  }

  public void set(int index, int recordBatch, int recordIndex) {
    data.setInt(index * 4, (recordBatch << 16) | (recordIndex & 65535));
  }

  public int get(int index) {
    return data.getInt((start+index) * 4);
  }

  /**
   * Caution: This method shares the underlying buffer between this vector and the newly created one.
   * @param batchRecordCount this will be used when creating the new vector
   * @return Newly created single batch SelectionVector4.
   */
  public SelectionVector4 createNewWrapperCurrent(int batchRecordCount) {
    try {
      data.retain();
      final SelectionVector4 sv4 = new SelectionVector4(data, recordCount, batchRecordCount);
      sv4.start = this.start;
      return sv4;
    } catch (SchemaChangeException e) {
      throw new IllegalStateException("This shouldn't happen.");
    }
  }

  /**
   * Caution: This method shares the underlying buffer between this vector and the newly created one.
   * @return Newly created single batch SelectionVector4.
   */
  public SelectionVector4 createNewWrapperCurrent() {
    return createNewWrapperCurrent(length);
  }

  public boolean next() {
//    logger.debug("Next called. Start: {}, Length: {}, recordCount: " + recordCount, start, length);

    if (!hasNext()) {
      start = recordCount;
      length = 0;
//      logger.debug("Setting count to zero.");
      return false;
    }

    start = start + length;
    int newEnd = Math.min(start + length, recordCount);
    length = newEnd - start;
//    logger.debug("New start {}, new length {}", start, length);
    return true;
  }

  public boolean hasNext() {
    final int endIndex = start + length;
    return endIndex < recordCount;
  }

  public void clear() {
    start = 0;
    length = 0;
    recordCount = 0;
    if (data != DeadBuf.DEAD_BUFFER) {
      data.release();
      data = DeadBuf.DEAD_BUFFER;
    }
  }

  public void copy(SelectionVector4 fromSV4) {
    clear();
    this.recordCount = fromSV4.getTotalCount();
    this.length = fromSV4.getCount();
    this.data = fromSV4.getData();
    // Need to retain the data buffer since if fromSV4 clears out the buffer it's not actually released unless the
    // copied SV4 has also released it
    if (data != DeadBuf.DEAD_BUFFER) {
      this.data.retain();
    }
  }

  public static int getBatchIndex(int sv4Index) {
    return (sv4Index >> 16) & 0xFFFF;
  }

  public static int getRecordIndex(int sv4Index) {
    return (sv4Index) & 0xFFFF;
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public String toString() {
    return "SelectionVector4[data=" + data
        + ", recordCount=" + recordCount
        + ", start=" + start
        + ", length=" + length
        + "]";
  }
}
