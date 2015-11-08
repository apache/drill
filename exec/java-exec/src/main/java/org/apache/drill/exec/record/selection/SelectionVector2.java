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
package org.apache.drill.exec.record.selection;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.record.DeadBuf;

/**
 * A selection vector that fronts, at most, a
 */
public class SelectionVector2 implements AutoCloseable {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVector2.class);

  private final BufferAllocator allocator;
  private int recordCount;
  private DrillBuf buffer = DeadBuf.DEAD_BUFFER;

  public static final int RECORD_SIZE = 2;

  public SelectionVector2(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public int getCount() {
    return recordCount;
  }

  public DrillBuf getBuffer() {
    return getBuffer(true);
  }

  public DrillBuf getBuffer(boolean clear) {
    DrillBuf bufferHandle = this.buffer;

    if (clear) {
      /* Increment the ref count for this buffer */
      bufferHandle.retain(1);

      /* We are passing ownership of the buffer to the
       * caller. clear the buffer from within our selection vector
       */
      clear();
    }

    return bufferHandle;
  }

  public void setBuffer(DrillBuf bufferHandle) {
      /* clear the existing buffer */
      clear();

      this.buffer = bufferHandle;
      buffer.retain(1);
  }

  public char getIndex(int index) {
    return buffer.getChar(index * RECORD_SIZE);
  }

  public void setIndex(int index, char value) {
    buffer.setChar(index * RECORD_SIZE, value);
  }

  public long getDataAddr() {
    return buffer.memoryAddress();
  }

  public void setIndex(int index, int value) {
    buffer.setChar(index, value);
  }

  public boolean allocateNewSafe(int size) {
    try {
      allocateNew(size);
    } catch (OutOfMemoryException e) {
      return false;
    }
    return true;
  }

  public void allocateNew(int size) {
    clear();
    buffer = allocator.buffer(size * RECORD_SIZE);
  }

  @Override
  public SelectionVector2 clone() {
    SelectionVector2 newSV = new SelectionVector2(allocator);
    newSV.recordCount = recordCount;
    newSV.buffer = buffer;

    /* Since buffer and newSV.buffer essentially point to the
     * same buffer, if we don't do a retain() on the newSV's
     * buffer, it might get freed.
     */
    newSV.buffer.retain(1);
    clear();
    return newSV;
  }

  public void clear() {
    if (buffer != null && buffer != DeadBuf.DEAD_BUFFER) {
      buffer.release();
      buffer = DeadBuf.DEAD_BUFFER;
      recordCount = 0;
    }
  }

  public void setRecordCount(int recordCount){
//    logger.debug("Seting record count to {}", recordCount);
    this.recordCount = recordCount;
  }

  @Override
  public void close() {
    clear();
  }
}
